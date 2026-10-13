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
_READ_COUNT = 16          # max entries per stream per XREADGROUP (per-stream; batch ≈ this × #streams)
_CB_TIMEOUT = 60.0        # per-message handler deadline — a hung callback can't wedge its stream's worker
_QUEUE_MAX = 1000         # per-stream worker backlog before the reader drops+acks (keeps the reader free)


class RedisTransport:
    def __init__(self, url: str, *, maxlen: int = 10000, max_connections: int = 512,
                 pool_timeout: float = 20.0):
        self._url = url
        self._maxlen = maxlen
        # MULTIPLEXED CONSUMER: ONE background loop PER GROUP (= agent_id) reads ALL that group's streams
        # in a SINGLE blocking XREADGROUP — the group name is shared across every stream, so one call
        # serves every room + DM the agent is in. That means an agent holds ~ONE redis connection
        # regardless of how many channels it consumes (the old model held one blocking connection PER
        # channel, which exhausted the pool and the broker). The group's stream set is re-snapshotted each
        # loop iteration, so a subscribe/unsubscribe is picked up within one block window (<= block ms)
        # with NO reader teardown. A BLOCKING pool (generous ceiling) still backstops the few transient
        # connections publish/xack/xgroup_create borrow.
        self._max_connections = max(1, int(max_connections))
        self._pool_timeout = float(pool_timeout)
        self._redis = None
        self._group_streams: dict = {}       # group -> {stream: cb}  (the live subscription set)
        self._group_tasks: dict = {}         # group -> the single reader asyncio.Task
        self._group_locks: dict = {}         # group -> lock serializing subscribe/unsubscribe + task start
        self._stream_queues: dict = {}       # (group, stream) -> bounded queue feeding its worker
        self._stream_workers: dict = {}      # (group, stream) -> worker task (cb+ack, serial per stream)
        self._closed = False                 # set by close(); rejects late subscribes (no orphan reader)

    async def _client(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            from redis.asyncio import BlockingConnectionPool
            pool = BlockingConnectionPool.from_url(
                self._url, max_connections=self._max_connections, timeout=self._pool_timeout)
            # from_pool (NOT Redis(connection_pool=...)) so the client OWNS the pool and close()'s
            # aclose() tears its connections down — otherwise an explicit pool leaks on close.
            self._redis = aioredis.Redis.from_pool(pool)
        return self._redis

    def _lock(self, group: str) -> asyncio.Lock:
        lock = self._group_locks.get(group)
        if lock is None:
            lock = self._group_locks[group] = asyncio.Lock()
        return lock

    async def publish(self, channel: str, blob: bytes):
        r = await self._client()
        await r.xadd(STREAM_PREFIX + channel, {"env": blob}, maxlen=self._maxlen, approximate=True)

    async def subscribe(self, channel: str, group: str, cb):
        """Register this member's consumer for ``channel`` under its ``group`` (= agent_id). Idempotent
        per (group, stream) — a re-subscribe just refreshes the callback. The group's streams are READ by
        ONE multiplexed reader (one redis connection); each stream's messages are PROCESSED by its OWN
        worker (serial within a stream, concurrent across streams), so a slow handler on one room never
        stalls the others or the reader."""
        stream = STREAM_PREFIX + channel
        r = await self._client()
        async with self._lock(group):
            if self._closed:
                return
            streams = self._group_streams.setdefault(group, {})
            if stream not in streams:
                # Create the consumer group BEFORE committing the stream: a cancel/failure here then
                # leaves NO orphaned registration, and the shared multi-stream read is never poisoned by
                # a stream whose group is missing. A non-BUSYGROUP failure propagates (callers are
                # fail-open) rather than registering a broken stream.
                try:
                    await r.xgroup_create(stream, group, id="0", mkstream=True)
                except Exception as e:  # noqa: BLE001
                    if "BUSYGROUP" not in str(e):
                        raise
                self._stream_queues[(group, stream)] = asyncio.Queue(maxsize=_QUEUE_MAX)
                self._stream_workers[(group, stream)] = asyncio.create_task(
                    self._stream_worker(group, stream))
            streams[stream] = cb
            t = self._group_tasks.get(group)
            if t is None or t.done():
                self._group_tasks[group] = asyncio.create_task(self._group_loop(group))

    async def _group_loop(self, group: str):
        """ONE reader per group: block on XREADGROUP across ALL the group's streams and ENQUEUE each entry
        to its stream's worker — no callback/ack here, so a slow room can't block the reader or the other
        rooms. Re-snapshots the stream set each iteration (a new stream joins within one block window; an
        unsubscribed one drops out). Exits when the group has no streams (restarted by the next subscribe).
        On a read error, re-ensures the consumer groups — self-heals a NOGROUP that would otherwise stall
        the whole multi-stream read."""
        r = await self._client()
        consumer = f"{group}-c"
        while True:
            streams = list(self._group_streams.get(group) or {})
            if not streams:
                return
            try:
                resp = await r.xreadgroup(group, consumer, {s: ">" for s in streams},
                                          count=_READ_COUNT, block=5000)
                for stream_name, entries in resp or []:
                    sname = (stream_name.decode() if isinstance(stream_name, (bytes, bytearray))
                             else stream_name)
                    q = self._stream_queues.get((group, sname))
                    for msg_id, fields in entries:
                        blob = fields.get(b"env") or fields.get("env")
                        if q is not None:
                            try:
                                q.put_nowait((msg_id, blob))     # hand off to the stream's worker
                                continue
                            except asyncio.QueueFull:
                                log.warning("[nmex] %s worker backlog full — dropping+acking", sname)
                        # no worker (unsubscribed mid-block) or backlog full → consume + ack so this
                        # message isn't redelivered; the reader never blocks on a wedged stream.
                        try:
                            await r.xack(sname, group, msg_id)
                        except asyncio.CancelledError:
                            raise
                        except Exception:  # noqa: BLE001
                            log.warning("[nmex] xack(drop) failed", exc_info=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                # A blocking XREADGROUP that returns nothing within the window surfaces as a TimeoutError
                # (also catches a transport socket timeout) — treat as idle, loop again.
                if e.__class__.__name__ == "TimeoutError":
                    continue
                log.warning("[nmex] read loop error", exc_info=True)
                for s in list(self._group_streams.get(group) or {}):   # self-heal a missing group (NOGROUP)
                    try:
                        await r.xgroup_create(s, group, id="0", mkstream=True)
                    except Exception:  # noqa: BLE001
                        pass
                await asyncio.sleep(1.0)

    async def _stream_worker(self, group: str, stream: str):
        """Process ONE stream's messages IN ORDER: await the callback (bounded by a deadline so a hung
        handler can't wedge the stream forever), then ack. Serial within the stream; different streams'
        workers run concurrently, so one slow room doesn't delay another."""
        r = await self._client()
        q = self._stream_queues.get((group, stream))
        if q is None:
            return
        while True:
            msg_id, blob = await q.get()
            cb = (self._group_streams.get(group) or {}).get(stream)   # live lookup (None → just unsubbed)
            if cb is not None:
                try:
                    await asyncio.wait_for(cb(blob), timeout=_CB_TIMEOUT)
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    log.warning("[nmex] handler timed out on %s", stream)
                except Exception:  # noqa: BLE001
                    log.warning("[nmex] handler error", exc_info=True)
            try:
                await r.xack(stream, group, msg_id)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.warning("[nmex] xack failed", exc_info=True)

    async def unsubscribe(self, channel: str, group: str):
        """Stop delivering ``channel`` to this member (idempotent). Cancels the stream's worker; when the
        group has no streams left its reader is torn down too. The Redis consumer group is left in place —
        harmless and keeps re-subscribe cheap."""
        stream = STREAM_PREFIX + channel
        to_stop = []
        async with self._lock(group):
            streams = self._group_streams.get(group)
            if streams and stream in streams:
                del streams[stream]
            self._stream_queues.pop((group, stream), None)
            w = self._stream_workers.pop((group, stream), None)
            if w is not None:
                to_stop.append(w)
            if streams is not None and not streams:
                self._group_streams.pop(group, None)
                rt = self._group_tasks.pop(group, None)
                if rt is not None:
                    to_stop.append(rt)
        for t in to_stop:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def close(self):
        self._closed = True
        tasks = list(self._group_tasks.values()) + list(self._stream_workers.values())
        self._group_tasks.clear()
        self._stream_workers.clear()
        self._group_streams.clear()
        self._stream_queues.clear()
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
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
        return RedisTransport(cfg["url"], maxlen=int(cfg.get("maxlen", 10000)),
                              max_connections=int(cfg.get("max_connections", 512)),
                              pool_timeout=float(cfg.get("pool_timeout", 20.0)))
    raise ValueError(f"unknown transport kind: {kind}")
