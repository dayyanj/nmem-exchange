"""Live RedisTransport round-trip: two agents over the REAL dedicated Redis do an
encrypted DM. Needs NMEX_REDIS_URL in the env.

  set -a; source ~/michelle/exchange.env; set +a
  python tests/test_redis.py
"""
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange import crypto                    # noqa: E402
from nmem_exchange.transport import RedisTransport   # noqa: E402
from nmem_exchange.client import Exchange            # noqa: E402


async def main():
    url = os.environ["NMEX_REDIS_URL"]
    a = crypto.generate_identity("agentA")
    b = crypto.generate_identity("agentB")
    keyring = {i.agent_id: i.public_bundle() for i in (a, b)}
    ch = f"dm:test:{int(time.time())}"                 # fresh channel per run
    channels = {ch: {"policy": "encrypted", "roster": ["agentA", "agentB"]}}

    exA = Exchange(a, keyring, channels, RedisTransport(url))
    exB = Exchange(b, keyring, channels, RedisTransport(url))

    got = []
    await exB.start(lambda meta, body: got.append((meta["from"], body)) or asyncio.sleep(0))
    await asyncio.sleep(0.6)                           # let the consumer group establish
    mid = await exA.send(ch, "challenge", "hello over real redis")
    print("agentA sent", mid, "on", ch)

    for _ in range(20):
        await asyncio.sleep(0.5)
        if got:
            break
    assert got and got[0] == ("agentA", b"hello over real redis"), f"no delivery: {got}"
    print("REAL REDIS round-trip: OK ->", got[0][1].decode())

    await exA.close()
    await exB.close()


if __name__ == "__main__":
    asyncio.run(main())
