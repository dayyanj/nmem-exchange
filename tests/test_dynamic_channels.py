"""Runtime (un)subscribe for group-chat v2 dynamic rooms: add_channel / remove_channel /
update_roster over the in-process broker. No Redis.

  python tests/test_dynamic_channels.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange import crypto                        # noqa: E402
from nmem_exchange.transport import InMemoryTransport   # noqa: E402
from nmem_exchange.client import Exchange               # noqa: E402


async def main():
    a = crypto.generate_identity("agent_a")
    b = crypto.generate_identity("agent_b")
    keyring = {i.agent_id: i.public_bundle() for i in (a, b)}
    # Boot with NO rooms — only each agent's view of a shared DM channel.
    base = {"dm:agent_a:agent_b": {"policy": "encrypted", "roster": ["agent_a", "agent_b"]}}
    broker = InMemoryTransport()
    ex_a = Exchange(a, dict(keyring), {k: dict(v) for k, v in base.items()}, broker)
    ex_b = Exchange(b, dict(keyring), {k: dict(v) for k, v in base.items()}, broker)

    got_a, got_b = [], []
    await ex_a.start(lambda meta, body: got_a.append((meta["channel"], body)) or asyncio.sleep(0))
    await ex_b.start(lambda meta, body: got_b.append((meta["channel"], body)) or asyncio.sleep(0))

    ROOM = "room:agent_a-deadbeef"

    # 1) a "creates" the room (roster = {a}); b doesn't know it yet.
    await ex_a.add_channel(ROOM, "open", ["agent_a"])
    await ex_a.send(ROOM, "group.msg", "hello, anyone?")
    await asyncio.sleep(0.1)
    assert not any(c == ROOM for c, _ in got_b), "b received a room post before joining!"
    print("pre-join isolation: OK")

    # 2) b joins: b subscribes with the merged roster, and a learns b (update_roster) so a's
    #    receive-path check will accept b's posts.
    await ex_b.add_channel(ROOM, "open", ["agent_a", "agent_b"])
    ex_a.update_roster(ROOM, ["agent_a", "agent_b"])
    await ex_a.send(ROOM, "group.msg", "welcome b")
    await ex_b.send(ROOM, "group.msg", "thanks a")
    await asyncio.sleep(0.1)
    assert (ROOM, b"welcome b") in got_b, "b missed a's post after joining"
    assert (ROOM, b"thanks a") in got_a, "a rejected b's post after the roster update"
    print("join + bidirectional delivery: OK")

    # 3) idempotent add — a second join must not spawn a duplicate consumer (no double-delivery).
    await ex_b.add_channel(ROOM, "open", ["agent_a", "agent_b"])
    n_before = len(got_b)
    await ex_a.send(ROOM, "group.msg", "single?")
    await asyncio.sleep(0.1)
    assert len(got_b) == n_before + 1, f"duplicate delivery after re-add (got {len(got_b) - n_before})"
    print("idempotent re-add: OK")

    # 4) b leaves — remove_channel cancels b's consumer; later posts don't reach b.
    await ex_b.remove_channel(ROOM)
    n_before = len(got_b)
    await ex_a.send(ROOM, "group.msg", "still there?")
    await asyncio.sleep(0.1)
    assert len(got_b) == n_before, "b received a post after leaving"
    assert ROOM not in ex_b.channels, "channel config not dropped on leave"
    print("leave (unsubscribe): OK")

    await broker.close()
    print("\nALL OK")


if __name__ == "__main__":
    asyncio.run(main())
