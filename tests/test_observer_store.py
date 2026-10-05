"""Observer store: record → dedup → query → search → leaderboards. Pure stdlib (no Redis).

  python tests/test_observer_store.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange.observer.store import ObserverStore, extract_mentions   # noqa: E402

KNOWN = ["agent-a", "agent-b", "agent-c"]


def test_extract_mentions():
    assert extract_mentions("hi @agent-b and @agent-c!", KNOWN) == ["agent-b", "agent-c"]
    assert extract_mentions("@agent-b.", KNOWN) == ["agent-b"]        # trailing punctuation trimmed
    assert extract_mentions("email a@b.com only", KNOWN) == []        # not a known agent
    assert extract_mentions("@AGENT-A shouts", KNOWN) == ["agent-a"]  # case-insensitive


def _seed(store):
    # agent-a posts twice (one addressing b), agent-b once (addressing a), agent-c once in another room
    store.record_message(msg_id="m1", room="hive-hall", sender="agent-a", ts=1000.0,
                         text="morning fleet", known_agents=KNOWN)
    store.record_message(msg_id="m2", room="hive-hall", sender="agent-b", ts=1001.0,
                         text="@agent-a did the migration land?", known_agents=KNOWN)
    store.record_message(msg_id="m3", room="hive-hall", sender="agent-a", ts=1002.0,
                         text="@agent-b yes, fair-rotation fixed it", known_agents=KNOWN)
    store.record_message(msg_id="m4", room="latency", sender="agent-c", ts=1003.0,
                         text="moved the latency detail here", known_agents=KNOWN)


def test_record_and_dedup():
    s = ObserverStore(":memory:")
    assert s.record_message(msg_id="m1", room="r", sender="agent-a", ts=1.0, text="hi") is True
    assert s.record_message(msg_id="m1", room="r", sender="agent-a", ts=1.0, text="hi") is False  # replay
    assert len(s.messages()) == 1


def test_rooms_and_messages():
    s = ObserverStore(":memory:"); _seed(s)
    rooms = s.rooms()
    names = {r["room"]: r for r in rooms}
    assert set(names) == {"hive-hall", "latency"}
    assert names["hive-hall"]["post_count"] == 3
    assert set(names["hive-hall"]["posters"]) == {"agent-a", "agent-b"}
    assert names["hive-hall"]["last_message"]["sender"] == "agent-a"
    msgs = s.messages("hive-hall")
    assert [m["msg_id"] for m in msgs] == ["m3", "m2", "m1"]          # newest first
    assert msgs[0]["mentions"] == ["agent-b"]


def test_search():
    s = ObserverStore(":memory:"); _seed(s)
    hits = s.search("migration")
    assert [h["msg_id"] for h in hits] == ["m2"]
    assert s.search("fair-rotation")[0]["msg_id"] == "m3"            # punctuation-tolerant
    assert s.search("") == []                                        # empty query → nothing
    assert s.search("migration", room="latency") == []              # room scope excludes it


def test_stats_leaderboards():
    s = ObserverStore(":memory:"); _seed(s)
    st = s.stats()
    assert st["totals"] == {"posts": 4, "agents": 3, "rooms": 2}
    assert st["top_posters"][0] == {"id": "agent-a", "count": 2}
    # a mentioned once (by b), b mentioned once (by a)
    assert {x["id"] for x in st["most_mentioned"]} == {"agent-a", "agent-b"}
    assert {x["id"] for x in st["most_responsive"]} == {"agent-a", "agent-b"}
    assert st["busiest_rooms"][0] == {"id": "hive-hall", "count": 3}
    # time-window scoping
    assert s.stats(since=1002.0)["totals"]["posts"] == 2


def main():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn(); print("ok", name)
    print("PASS test_observer_store")


if __name__ == "__main__":
    main()
