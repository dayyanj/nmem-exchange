"""Exchange client acceptance over the in-process broker: routing (DM private,
group fan-out, open readable) + replay/unknown-sender/stale drops. No Redis.

  python tests/test_client.py
"""
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange import crypto                    # noqa: E402
from nmem_exchange.transport import InMemoryTransport   # noqa: E402
from nmem_exchange.client import Exchange            # noqa: E402


async def main():
    michelle = crypto.generate_identity("michelle")
    djai = crypto.generate_identity("djai")
    alice = crypto.generate_identity("alice")
    keyring = {i.agent_id: i.public_bundle() for i in (michelle, djai, alice)}
    channels = {
        "dm:michelle:djai": {"policy": "encrypted", "roster": ["michelle", "djai"]},
        "grp:council":      {"policy": "encrypted", "roster": ["michelle", "djai", "alice"]},
        "open:lobby":       {"policy": "open",      "roster": ["michelle", "djai", "alice"]},
    }
    broker = InMemoryTransport()
    ex_m = Exchange(michelle, keyring, channels, broker)
    ex_d = Exchange(djai, keyring, channels, broker)
    ex_a = Exchange(alice, keyring, channels, broker)

    got_d, got_a = [], []
    await ex_d.start(lambda meta, body: got_d.append((meta["channel"], body)) or asyncio.sleep(0))
    await ex_a.start(lambda meta, body: got_a.append((meta["channel"], body)) or asyncio.sleep(0))
    await ex_m.start(lambda meta, body: asyncio.sleep(0))

    await ex_m.send("dm:michelle:djai", "challenge", "your plan has no owner")
    await ex_m.send("grp:council", "msg", "council broadcast")
    await ex_m.send("open:lobby", "msg", "public notice")
    await asyncio.sleep(0.2)

    assert ("dm:michelle:djai", b"your plan has no owner") in got_d, "djai missed the DM"
    assert not any(c == "dm:michelle:djai" for c, _ in got_a), "alice received a DM she wasn't in!"
    assert ("grp:council", b"council broadcast") in got_d and ("grp:council", b"council broadcast") in got_a, "group fan-out failed"
    assert ("open:lobby", b"public notice") in got_d, "open channel failed"
    print("routing (DM private / group fan-out / open): OK")

    # replay dedup — same signed envelope twice → handled once
    env = crypto.seal(michelle, {"michelle": michelle.public_bundle(), "djai": djai.public_bundle()},
                      b"once", channel="dm:michelle:djai", kind="challenge", msg_id="fixed1", ts=time.time())
    blob = json.dumps(env).encode()
    n = len(got_d)
    await ex_d._on_blob("dm:michelle:djai", blob)
    await ex_d._on_blob("dm:michelle:djai", blob)
    assert len(got_d) == n + 1, "replay was not deduped"
    print("replay dedup: OK")

    # unknown sender (not in keyring) → dropped
    intruder = crypto.generate_identity("intruder")
    env_i = crypto.seal(intruder, {"djai": djai.public_bundle()}, b"pwn",
                        channel="dm:michelle:djai", kind="x", msg_id="i1", ts=time.time())
    n = len(got_d)
    await ex_d._on_blob("dm:michelle:djai", json.dumps(env_i).encode())
    assert len(got_d) == n, "message from untrusted sender was accepted!"
    print("unknown sender dropped: OK")

    # stale message (outside ts window) → dropped
    env_old = crypto.seal(michelle, {"michelle": michelle.public_bundle(), "djai": djai.public_bundle()},
                          b"old", channel="dm:michelle:djai", kind="challenge", msg_id="old1", ts=time.time() - 100000)
    n = len(got_d)
    await ex_d._on_blob("dm:michelle:djai", json.dumps(env_old).encode())
    assert len(got_d) == n, "stale message was accepted!"
    print("stale message dropped: OK")

    # cross-channel injection: a valid DM envelope shoved into another subscribed
    # stream must be dropped (broker-tampering defense).
    env_x = crypto.seal(michelle, {"michelle": michelle.public_bundle(), "djai": djai.public_bundle()},
                        b"xpost", channel="dm:michelle:djai", kind="challenge", msg_id="x1", ts=time.time())
    n = len(got_d)
    await ex_d._on_blob("grp:council", json.dumps(env_x).encode())   # delivered on the WRONG stream
    assert len(got_d) == n, "cross-channel injected message was accepted!"
    print("cross-channel injection dropped: OK")

    for ex in (ex_m, ex_d, ex_a):
        await ex.close()
    print("\nNMEM-EXCHANGE CLIENT: ALL CHECKS PASS")


if __name__ == "__main__":
    asyncio.run(main())
