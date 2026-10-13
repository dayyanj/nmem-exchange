"""Unit tests for RedisTransport's connection-pool sizing (no live broker needed — pool
construction is lazy). Regression guard for the fleet-wide MaxConnectionsError: an agent that
consumes many channels (rooms + DMs) needs ~one blocking connection per channel, which blew past
redis-py's default pool ceiling of 100 and silently killed readers."""
import pytest

from nmem_exchange.transport import RedisTransport, build_transport


@pytest.mark.asyncio
async def test_client_uses_a_generous_blocking_pool():
    from redis.asyncio import BlockingConnectionPool
    t = RedisTransport("redis://localhost:6390/0", max_connections=512, pool_timeout=20.0)
    r = await t._client()                       # lazy: builds the pool, does NOT connect
    pool = r.connection_pool
    assert isinstance(pool, BlockingConnectionPool)   # WAITS for a free conn instead of hard-erroring
    assert pool.max_connections == 512                 # well above real per-agent channel counts
    # built via from_pool → the client OWNS the pool, so close()'s aclose() tears its connections down
    # (guards the regression where Redis(connection_pool=...) would leak the pool on close).
    assert r.auto_close_connection_pool is True
    # idempotent: a second _client() returns the same client (no pool churn)
    assert await t._client() is r


@pytest.mark.asyncio
async def test_default_ceiling_is_well_above_the_redis_py_default_of_100():
    t = RedisTransport("redis://localhost:6390/0")
    r = await t._client()
    assert r.connection_pool.max_connections >= 512    # not the 100 default that caused the outage


def test_build_transport_threads_pool_size_from_cfg():
    t = build_transport({"kind": "redis", "url": "redis://localhost:6390/0",
                         "max_connections": 1024, "pool_timeout": 30.0})
    assert isinstance(t, RedisTransport)
    assert t._max_connections == 1024 and t._pool_timeout == 30.0


def test_build_transport_redis_default_pool_size():
    t = build_transport({"kind": "redis", "url": "redis://localhost:6390/0"})
    assert t._max_connections == 512 and t._pool_timeout == 20.0
