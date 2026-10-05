"""Observer ingest: the envelope trust boundary (verify + unpack open room messages, reject the rest).
Pure crypto + store, no Redis.

  python tests/test_observer_ingest.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange import crypto                                              # noqa: E402
from nmem_exchange.observer.ingest import EnvelopeRejected, decode_room_envelope  # noqa: E402
from nmem_exchange.observer.store import ObserverStore                        # noqa: E402

A = crypto.generate_identity("agent-a")
B = crypto.generate_identity("agent-b")
KEYRING = {A.agent_id: A.public_bundle(), B.agent_id: B.public_bundle()}


def _room_env(sender, text, *, channel="room:hive-hall", kind="group.msg", msg_id="m1"):
    return crypto.seal_open(sender, text.encode(), channel=channel, kind=kind,
                            msg_id=msg_id, ts=time.time())


def test_decode_valid_open_message():
    env = _room_env(A, "hello @agent-b")
    rec = decode_room_envelope(env, KEYRING, expected_channel="room:hive-hall")
    assert rec["sender"] == "agent-a"
    assert rec["room"] == "hive-hall"
    assert rec["text"] == "hello @agent-b"
    assert rec["msg_id"] == "m1"


def _rejects(env, **kw):
    try:
        decode_room_envelope(env, KEYRING, **kw)
    except EnvelopeRejected:
        return True
    return False


def test_rejects_unknown_signer():
    stranger = crypto.generate_identity("agent-x")
    assert _rejects(_room_env(stranger, "hi"))                  # not in keyring


def test_rejects_forged_signature():
    env = _room_env(A, "hi")
    env["pt"] = crypto._b64e(b"tampered body")                 # body changed after signing
    assert _rejects(env)


def test_rejects_non_room_kind():
    assert _rejects(_room_env(A, "snap", kind="room.gossip"))


def test_rejects_channel_mismatch():
    env = _room_env(A, "hi", channel="room:hive-hall")
    assert _rejects(env, expected_channel="room:somewhere-else")


def test_rejects_encrypted_envelope():
    roster = {A.agent_id: A.public_bundle(), B.agent_id: B.public_bundle()}
    env = crypto.seal(A, roster, b"secret", channel="room:hive-hall", kind="group.msg",
                      msg_id="e1", ts=time.time())
    assert _rejects(env)                                        # enc != none → not observable


def test_decode_then_store_roundtrip():
    s = ObserverStore(":memory:")
    rec = decode_room_envelope(_room_env(A, "@agent-b ping", msg_id="r1"), KEYRING)
    assert s.record_message(known_agents=KEYRING.keys(), **rec) is True
    row = s.messages("hive-hall")[0]
    assert row["sender"] == "agent-a" and row["mentions"] == ["agent-b"]


def main():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn(); print("ok", name)
    print("PASS test_observer_ingest")


if __name__ == "__main__":
    main()
