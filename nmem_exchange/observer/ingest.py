"""The bus tap: discover + tail the broker's OPEN room streams, verify, persist, fan out live.

A passive observer needs no identity and joins no roster. Open channels are signed-but-plaintext, so
for each envelope we only (1) check the signer is in the fleet keyring and the signature is valid
(``crypto.verify_sig``), then (2) read the plaintext body — exactly the guarantee the exchange makes
for ``open`` channels ("authorship verified, anyone on the bus can read"). Encrypted DM streams are
never touched: we subscribe only to ``room:*``.

Discovery is by Redis key scan (``nmex:ch:room:*``) on an interval, so rooms created after the
observer starts are picked up automatically — no gossip membership required (gossip rides encrypted
DMs we can't read anyway). Each stream is tailed from the beginning once (backfill) then followed
live. New messages are written to the :class:`ObserverStore` and pushed to any live subscribers
(the web dashboard's SSE stream).

Redis-only: the tap needs a real broker to observe. (Unit tests exercise the pure envelope decode via
:func:`decode_room_envelope` without Redis.)
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

from nmem_exchange import crypto
from nmem_exchange.observer.store import ObserverStore
from nmem_exchange.transport import STREAM_PREFIX

log = logging.getLogger(__name__)

ROOM_CHANNEL_PREFIX = "room:"
GROUP_MSG_KIND = "group.msg"          # mirrors agent_core.group.KIND_GROUP_MSG
ROOM_META_KIND = "room.meta"          # mirrors room_registry.KIND_ROOM_META — a room's human-name beacon


def _is_timeout(e: Exception) -> bool:
    """A blocking-read timeout on a quiet stream (normal), vs a real transport error. Matched by type
    name so we needn't import redis's exception just to classify it."""
    return isinstance(e, TimeoutError) or type(e).__name__ == "TimeoutError" \
        or "timeout" in str(e).lower()


class EnvelopeRejected(Exception):
    """An envelope that failed verification or isn't an observable room message."""


def decode_room_envelope(env: dict, keyring: dict, *, expected_channel: str | None = None) -> dict:
    """Verify + unpack one OPEN room envelope into ``{msg_id, room, sender, ts, text, in_reply_to}``.

    Raises :class:`EnvelopeRejected` for anything we must not record: an unknown/forged signer, a
    non-open (encrypted) envelope, a non-group-message kind, or a channel that doesn't match the
    stream it arrived on (an untrusted broker must not be able to cross-post into a room). This is the
    whole trust boundary, so it is intentionally strict + pure (no I/O)."""
    if not isinstance(env, dict):
        raise EnvelopeRejected("not an envelope")
    sender = env.get("from")
    peer = keyring.get(sender)
    if peer is None:
        raise EnvelopeRejected(f"sender {sender!r} not in keyring")
    if not crypto.verify_sig(env, peer["sign_pub"]):
        raise EnvelopeRejected("bad signature")
    if env.get("enc") != "none":
        raise EnvelopeRejected("not an open (plaintext) envelope")
    if env.get("kind") != GROUP_MSG_KIND:
        raise EnvelopeRejected(f"not a room message (kind={env.get('kind')!r})")
    channel = env.get("channel") or ""
    if expected_channel is not None and channel != expected_channel:
        raise EnvelopeRejected(f"channel mismatch: signed {channel!r} on {expected_channel!r}")
    if not channel.startswith(ROOM_CHANNEL_PREFIX):
        raise EnvelopeRejected(f"not a room channel ({channel!r})")
    try:
        text = crypto._b64d(env["pt"]).decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        raise EnvelopeRejected(f"undecodable body: {e}") from e
    return {
        "msg_id": env.get("msg_id") or "",
        "room": channel[len(ROOM_CHANNEL_PREFIX):],
        "sender": sender,
        "ts": float(env.get("ts") or 0.0),
        "text": text,
        "in_reply_to": env.get("in_reply_to"),
    }


def decode_room_meta(env: dict, keyring: dict, *, expected_channel: str | None = None) -> dict:
    """Verify + unpack one OPEN ``room.meta`` envelope into ``{room, name, policy, created_by, ts}``.

    Same trust boundary as :func:`decode_room_envelope` (known signer, valid signature, open, a room
    channel matching the stream) but for the human-name beacon rather than a chat message. Raises
    :class:`EnvelopeRejected` for anything we must not trust."""
    if not isinstance(env, dict):
        raise EnvelopeRejected("not an envelope")
    sender = env.get("from")
    peer = keyring.get(sender)
    if peer is None:
        raise EnvelopeRejected(f"sender {sender!r} not in keyring")
    if not crypto.verify_sig(env, peer["sign_pub"]):
        raise EnvelopeRejected("bad signature")
    if env.get("enc") != "none":
        raise EnvelopeRejected("not an open (plaintext) envelope")
    if env.get("kind") != ROOM_META_KIND:
        raise EnvelopeRejected(f"not a room-meta beacon (kind={env.get('kind')!r})")
    channel = env.get("channel") or ""
    if expected_channel is not None and channel != expected_channel:
        raise EnvelopeRejected(f"channel mismatch: signed {channel!r} on {expected_channel!r}")
    if not channel.startswith(ROOM_CHANNEL_PREFIX):
        raise EnvelopeRejected(f"not a room channel ({channel!r})")
    try:
        payload = json.loads(crypto._b64d(env["pt"]).decode("utf-8", errors="replace"))
    except Exception as e:  # noqa: BLE001
        raise EnvelopeRejected(f"undecodable meta body: {e}") from e
    if not isinstance(payload, dict):
        raise EnvelopeRejected("meta body is not an object")
    return {
        "room": channel[len(ROOM_CHANNEL_PREFIX):],
        "name": str(payload.get("name") or ""),
        "policy": str(payload.get("policy") or ""),
        "created_by": str(payload.get("created_by") or ""),
        "ts": float(env.get("ts") or 0.0),
    }


class RoomTap:
    """Tails every ``room:*`` stream on a Redis broker into an :class:`ObserverStore`, following new
    rooms as they appear, and notifies live subscribers of each newly-recorded message."""

    def __init__(self, url: str, keyring: dict, store: ObserverStore, *,
                 rescan_interval: float = 15.0, block_ms: int = 5000,
                 on_message: Callable[[dict], Awaitable[None] | None] | None = None) -> None:
        self._url = url
        self._keyring = dict(keyring or {})
        self._store = store
        self._known = set(keyring or {})                      # agent ids, for @mention resolution
        self._rescan = max(1.0, float(rescan_interval))
        self._block_ms = int(block_ms)
        self._on_message = on_message
        self._redis = None
        self._followers: dict[str, asyncio.Task] = {}         # stream key -> follower task
        self._stop = asyncio.Event()

    async def _client(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(self._url)
        return self._redis

    async def run(self) -> None:
        """Discover-then-follow loop: scan for room streams, start a follower per new one, repeat until
        :meth:`stop`. Fail-open — a scan/Redis hiccup logs and retries next interval."""
        r = await self._client()
        while not self._stop.is_set():
            try:
                await self._discover(r)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.warning("[observer] room scan failed (non-fatal)", exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), self._rescan)
            except asyncio.TimeoutError:
                pass

    async def _discover(self, r) -> None:
        pattern = f"{STREAM_PREFIX}{ROOM_CHANNEL_PREFIX}*"
        async for key in r.scan_iter(match=pattern, count=200):
            stream = key.decode() if isinstance(key, (bytes, bytearray)) else str(key)
            if stream not in self._followers:
                self._followers[stream] = asyncio.create_task(self._follow(r, stream))

    async def _follow(self, r, stream: str) -> None:
        """Backfill a stream from the start, then follow it live. One task per stream."""
        channel = stream[len(STREAM_PREFIX):]
        last_id = "0"                                          # from the beginning (backfill history)
        while not self._stop.is_set():
            try:
                resp = await r.xread({stream: last_id}, count=200, block=self._block_ms)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                # A read timeout on a quiet stream is normal (the block elapsed with nothing new) —
                # don't treat it as an error; just poll again. Anything else is worth a warning.
                if _is_timeout(e):
                    continue
                log.warning("[observer] xread %s failed (non-fatal)", stream, exc_info=True)
                await asyncio.sleep(1.0)
                continue
            for _s, entries in resp or []:
                for msg_id, fields in entries:
                    last_id = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else msg_id
                    blob = fields.get(b"env") or fields.get("env")
                    if blob:
                        await self._ingest(channel, blob)

    async def _ingest(self, channel: str, blob) -> None:
        try:
            env = json.loads(blob)
        except Exception:  # noqa: BLE001
            return
        loop = asyncio.get_running_loop()
        # Room-name beacon: label the room (no message recorded), so the dashboard shows the human name
        # instead of the opaque <creator>-<hex> id.
        if isinstance(env, dict) and env.get("kind") == ROOM_META_KIND:
            try:
                meta = decode_room_meta(env, self._keyring, expected_channel=channel)
            except EnvelopeRejected as e:
                log.debug("[observer] dropped meta on %s: %s", channel, e)
                return
            if meta["name"]:
                await loop.run_in_executor(None, lambda: self._store.set_room_name(
                    room=meta["room"], name=meta["name"], ts=meta["ts"]))
            return
        try:
            rec = decode_room_envelope(env, self._keyring, expected_channel=channel)
        except EnvelopeRejected as e:
            log.debug("[observer] dropped envelope on %s: %s", channel, e)
            return
        # SQLite writes are quick; run in a thread so a slow disk never stalls the bus loop.
        loop = asyncio.get_running_loop()
        is_new = await loop.run_in_executor(None, lambda: self._store.record_message(
            msg_id=rec["msg_id"], room=rec["room"], sender=rec["sender"], ts=rec["ts"],
            text=rec["text"], in_reply_to=rec["in_reply_to"], known_agents=self._known))
        if is_new and self._on_message is not None:
            try:
                res = self._on_message(rec)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:  # noqa: BLE001 — a live-subscriber hiccup must not break ingest
                log.debug("[observer] on_message hook failed (non-fatal)", exc_info=True)

    async def stop(self) -> None:
        self._stop.set()
        for t in list(self._followers.values()):
            t.cancel()
        for t in list(self._followers.values()):
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._followers.clear()
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:  # noqa: BLE001
                pass
