"""Pluggable transport for nmem-exchange. The broker is UNTRUSTED — it only ever
carries opaque envelope blobs (ciphertext + wrapped keys for encrypted channels).

Transport interface:
  - publish(channel, blob)         — fan a blob out to every subscriber of channel
  - subscribe(channel, group, cb)  — deliver every blob on channel to THIS member
  - unsubscribe(channel, group)    — stop THIS member's consumer for channel (idempotent)
  - close()

RedisTransport: one stream per channel (`nmex:ch:{channel}`); each member reads it
via its OWN consumer group (group = the member's agent_id), so all members receive
every message (native fan-out). InMemoryTransport: same semantics, in-process, for
tests.
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)

STREAM_PREFIX = "nmex:ch:"


class RedisTransport:
    def __init__(self, url: str, *, maxlen: int = 10000):
        self._url = url
        self._maxlen = maxlen
        self._redis = None
        # Keyed by (channel, group) so a single channel's consumer can be cancelled on unsubscribe
        # (dynamic-room leave) without touching the others. close() still cancels every task.
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}

    async def _client(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(self._url)
        return self._redis

    async def publish(self, channel: str, blob: bytes):
        r = await self._client()
        await r.xadd(STREAM_PREFIX + channel, {"env": blob}, maxlen=self._maxlen, approximate=True)

    async def subscribe(self, channel: str, group: str, cb):
        """Start a background consumer: group=agent_id so each member gets a copy. Idempotent per
        (channel, group) — a second subscribe for a channel already consumed is a no-op (so a
        dynamic add_channel can't spawn a duplicate reader)."""
        if (channel, group) in self._tasks:
            return
        r = await self._client()
        stream = STREAM_PREFIX + channel
        try:
            await r.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as e:  # BUSYGROUP if it already exists — fine
            if "BUSYGROUP" not in str(e):
                log.warning("[nmex] xgroup_create %s/%s: %s", stream, group, e)

        async def _loop():
            consumer = f"{group}-c"
            while True:
                try:
                    resp = await r.xreadgroup(group, consumer, {stream: ">"}, count=16, block=5000)
                    for _stream, entries in resp or []:
                        for msg_id, fields in entries:
                            blob = fields.get(b"env") or fields.get("env")
                            try:
                                await cb(blob)
                            except Exception:
                                log.warning("[nmex] handler error", exc_info=True)
                            finally:
                                await r.xack(stream, group, msg_id)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    # A blocking XREADGROUP that returns no messages within the
                    # window surfaces as a TimeoutError — that's normal, not an error.
                    if e.__class__.__name__ == "TimeoutError":
                        continue
                    log.warning("[nmex] read loop error", exc_info=True)
                    await asyncio.sleep(1.0)

        self._tasks[(channel, group)] = asyncio.create_task(_loop())

    async def unsubscribe(self, channel: str, group: str):
        """Cancel THIS member's consumer for ``channel`` (idempotent). The Redis consumer group is left
        in place — it's harmless and keeps re-subscribe cheap — we just stop reading."""
        task = self._tasks.pop((channel, group), None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def close(self):
        for t in list(self._tasks.values()):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        if self._redis is not None:
            await self._redis.aclose()


class InMemoryTransport:
    """In-process broker for tests — a shared instance stands in for Redis. Every
    subscriber on a channel gets every published blob (consumer-group fan-out)."""
    def __init__(self):
        self._subs: dict[str, list[asyncio.Queue]] = {}
        # (channel, group) -> (queue, task), so unsubscribe can drop THIS member's queue + reader
        # for one channel without disturbing other subscribers (dynamic-room leave).
        self._tasks: dict[tuple[str, str], tuple[asyncio.Queue, asyncio.Task]] = {}

    async def publish(self, channel: str, blob: bytes):
        for q in self._subs.get(channel, []):
            q.put_nowait(blob)

    async def subscribe(self, channel: str, group: str, cb):
        if (channel, group) in self._tasks:   # idempotent per (channel, group)
            return
        q: asyncio.Queue = asyncio.Queue()
        self._subs.setdefault(channel, []).append(q)

        async def _loop():
            while True:
                blob = await q.get()
                try:
                    await cb(blob)
                except Exception:
                    log.warning("[nmex] handler error", exc_info=True)

        self._tasks[(channel, group)] = (q, asyncio.create_task(_loop()))

    async def unsubscribe(self, channel: str, group: str):
        """Drop THIS member's queue + reader for ``channel`` (idempotent)."""
        entry = self._tasks.pop((channel, group), None)
        if entry is None:
            return
        q, task = entry
        try:
            self._subs.get(channel, []).remove(q)
        except ValueError:
            pass
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def close(self):
        for _q, t in list(self._tasks.values()):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()


def build_transport(cfg: dict):
    kind = (cfg or {}).get("kind", "redis")
    if kind == "memory":
        return InMemoryTransport()
    if kind == "redis":
        return RedisTransport(cfg["url"], maxlen=int(cfg.get("maxlen", 10000)))
    raise ValueError(f"unknown transport kind: {kind}")
