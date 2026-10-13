"""RedisTransport multiplex: ONE reader per group serves ALL its streams (the durable fix for the
connection-per-channel exhaustion). Uses a tiny in-process fake redis — no live broker."""
import asyncio

import pytest

from nmem_exchange.transport import STREAM_PREFIX, RedisTransport


class _FakeRedis:
    """Minimal XREADGROUP-consumer-group fake: per-stream append log + per-stream delivered cursor."""
    def __init__(self):
        self.streams: dict = {}        # stream -> list[(id, {b"env": blob})]
        self.delivered: dict = {}      # stream -> count already handed to the consumer

    async def xgroup_create(self, stream, group, id="0", mkstream=True):
        self.streams.setdefault(stream, [])

    async def xadd(self, stream, fields, maxlen=None, approximate=True):
        lst = self.streams.setdefault(stream, [])
        mid = f"{len(lst) + 1}-0"
        lst.append((mid, {b"env": fields["env"]}))
        return mid

    async def xreadgroup(self, group, consumer, streams_map, count=64, block=5000):
        out = []
        for s in streams_map:                          # s == the real stream key the loop passed
            lst = self.streams.get(s, [])
            start = self.delivered.get(s, 0)
            if start < len(lst):
                self.delivered[s] = len(lst)
                out.append((s.encode(), list(lst[start:])))
        if not out:
            await asyncio.sleep(0.01)                   # emulate a short BLOCK window (yields the loop)
            return []
        return out

    async def xack(self, stream, group, msg_id):
        return None

    async def aclose(self):
        return None


@pytest.mark.asyncio
async def test_one_reader_per_group_serves_many_channels():
    t = RedisTransport("redis://fake/0")
    t._redis = _FakeRedis()                            # inject the fake (bypass real pool)
    got = {"a": [], "b": []}

    async def cb_a(blob):
        got["a"].append(blob)

    async def cb_b(blob):
        got["b"].append(blob)

    await t.subscribe("room:ra", "agentX", cb_a)
    await t.subscribe("room:rb", "agentX", cb_b)
    assert len(t._group_tasks) == 1                    # ONE reader for the whole group, not one/channel
    assert set(t._group_streams["agentX"]) == {STREAM_PREFIX + "room:ra", STREAM_PREFIX + "room:rb"}

    await t.publish("room:ra", b"to-a")
    await t.publish("room:rb", b"to-b")
    for _ in range(100):
        if got["a"] and got["b"]:
            break
        await asyncio.sleep(0.02)
    assert got["a"] == [b"to-a"] and got["b"] == [b"to-b"]   # the single reader multiplexed both streams

    await t.unsubscribe("room:ra", "agentX")
    assert len(t._group_tasks) == 1 and set(t._group_streams["agentX"]) == {STREAM_PREFIX + "room:rb"}
    await t.unsubscribe("room:rb", "agentX")
    assert len(t._group_tasks) == 0 and "agentX" not in t._group_streams   # no streams → reader torn down
    await t.close()


@pytest.mark.asyncio
async def test_resubscribe_refreshes_cb_without_second_reader():
    t = RedisTransport("redis://fake/0")
    t._redis = _FakeRedis()
    hits = []
    await t.subscribe("room:r", "g", lambda b: hits.append(("old", b)))

    async def new_cb(b):
        hits.append(("new", b))

    await t.subscribe("room:r", "g", new_cb)           # re-subscribe same (group, stream)
    assert len(t._group_tasks) == 1                    # still one reader
    await t.publish("room:r", b"x")
    for _ in range(100):
        if hits:
            break
        await asyncio.sleep(0.02)
    assert hits and hits[0][0] == "new"                # the refreshed callback is used
    await t.close()


@pytest.mark.asyncio
async def test_slow_handler_on_one_stream_does_not_block_another():
    # P1 fix: per-stream workers — a wedged handler on room A must not delay delivery on room B.
    t = RedisTransport("redis://fake/0")
    t._redis = _FakeRedis()
    gate = asyncio.Event()
    got_b = []

    async def slow_a(blob):
        await gate.wait()                      # A's worker is stuck here

    async def fast_b(blob):
        got_b.append(blob)

    await t.subscribe("room:a", "g", slow_a)
    await t.subscribe("room:b", "g", fast_b)
    await t.publish("room:a", b"a1")
    await t.publish("room:b", b"b1")
    for _ in range(100):
        if got_b:
            break
        await asyncio.sleep(0.02)
    assert got_b == [b"b1"]                     # B delivered despite A's handler blocking (no head-of-line)
    gate.set()
    await t.close()


@pytest.mark.asyncio
async def test_group_create_failure_propagates_and_registers_nothing():
    # P1 fix: a non-BUSYGROUP create failure must propagate and leave NO registered stream/worker — so a
    # missing consumer group can't poison the shared multi-stream read.
    class _FailCreate(_FakeRedis):
        async def xgroup_create(self, stream, group, id="0", mkstream=True):
            raise RuntimeError("boom")

    t = RedisTransport("redis://fake/0")
    t._redis = _FailCreate()
    with pytest.raises(RuntimeError):
        await t.subscribe("room:x", "g", lambda b: None)
    s = STREAM_PREFIX + "room:x"
    assert s not in t._group_streams.get("g", {})
    assert ("g", s) not in t._stream_workers
    await t.close()
