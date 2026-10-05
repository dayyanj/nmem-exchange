"""The observer web app: serves the dashboard + a JSON/SSE API over the store and the live tap.

Thin + read-only. Every data route reads the :class:`ObserverStore` in a thread executor (SQLite is
sync); ``/api/stream`` is a Server-Sent-Events tail fed by a :class:`LiveHub` the tap publishes into.
There are no write routes — the observer cannot post to the fleet.

Requires the ``observer`` extra (``aiohttp``); importing this module without it raises a clear error.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

try:
    from aiohttp import web
except ModuleNotFoundError as e:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "the exchange observer web server needs aiohttp — install the extra: "
        "pip install 'nmem-exchange[observer]'") from e

from nmem_exchange.observer.store import ObserverStore

log = logging.getLogger(__name__)

_DASHBOARD = Path(__file__).with_name("dashboard.html")

_WINDOWS = {"1h": 3600, "6h": 6 * 3600, "24h": 24 * 3600, "7d": 7 * 86400, "30d": 30 * 86400,
            "all": None}


class LiveHub:
    """Fan-out of newly-recorded messages to connected SSE clients. Each subscriber gets a bounded
    queue; a slow client is dropped (its queue fills) rather than back-pressuring ingest."""

    def __init__(self, maxq: int = 256) -> None:
        self._subs: set[asyncio.Queue] = set()
        self._maxq = maxq

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._maxq)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    async def publish(self, rec: dict) -> None:
        for q in list(self._subs):
            try:
                q.put_nowait(rec)
            except asyncio.QueueFull:
                self._subs.discard(q)                          # slow client → drop it


def _since(window: str | None) -> float | None:
    span = _WINDOWS.get((window or "24h").lower(), 24 * 3600)
    return None if span is None else time.time() - span


async def _run(store: ObserverStore, fn, *args):
    return await asyncio.get_running_loop().run_in_executor(None, lambda: fn(*args))


def build_app(store: ObserverStore, *, hub: LiveHub | None = None) -> web.Application:
    """Build the aiohttp app. ``hub`` (optional) powers the live SSE tail; without it ``/api/stream``
    simply holds open with heartbeats."""
    app = web.Application()
    app["store"] = store
    app["hub"] = hub

    async def index(_req):
        if not _DASHBOARD.exists():
            return web.Response(text="dashboard.html missing", status=500)
        return web.FileResponse(_DASHBOARD)

    async def api_rooms(_req):
        return web.json_response(await _run(store, store.rooms))

    async def api_messages(req):
        room = req.match_info["room"]
        limit = _clamp(req.query.get("limit"), 100, 1, 1000)
        before = req.query.get("before")
        before_f = float(before) if before else None
        rows = await _run(store, lambda: store.messages(room, limit=limit, before=before_f))
        return web.json_response(rows)

    async def api_search(req):
        q = req.query.get("q", "")
        room = req.query.get("room") or None
        sender = req.query.get("sender") or None
        limit = _clamp(req.query.get("limit"), 100, 1, 1000)
        rows = await _run(store, lambda: store.search(q, room=room, sender=sender, limit=limit))
        return web.json_response({"query": q, "results": rows})

    async def api_stats(req):
        window = req.query.get("window", "24h")
        since = _since(window)
        stats = await _run(store, lambda: store.stats(since=since))
        stats["window"] = window
        return web.json_response(stats)

    async def api_stream(req):
        resp = web.StreamResponse(headers={
            "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
            "Connection": "keep-alive", "X-Accel-Buffering": "no"})
        await resp.prepare(req)
        hub_ = app["hub"]
        q = hub_.subscribe() if hub_ is not None else None
        try:
            await resp.write(b": connected\n\n")
            while True:
                try:
                    if q is not None:
                        rec = await asyncio.wait_for(q.get(), timeout=20)
                        await resp.write(f"event: message\ndata: {json.dumps(rec)}\n\n".encode())
                    else:
                        await asyncio.sleep(20)
                        await resp.write(b": heartbeat\n\n")
                except asyncio.TimeoutError:
                    await resp.write(b": heartbeat\n\n")       # keep the connection warm
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            if q is not None and hub_ is not None:
                hub_.unsubscribe(q)
        return resp

    app.router.add_get("/", index)
    app.router.add_get("/api/rooms", api_rooms)
    app.router.add_get("/api/rooms/{room}/messages", api_messages)
    app.router.add_get("/api/search", api_search)
    app.router.add_get("/api/stats", api_stats)
    app.router.add_get("/api/stream", api_stream)
    return app


def _clamp(v: Any, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default
