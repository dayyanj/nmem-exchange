"""Security acceptance for nmem-exchange crypto — positive roundtrips AND the
negative cases that prove the guarantees. Pure PyNaCl, no Redis.

  python tests/test_crypto.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nmem_exchange import crypto as cx   # noqa: E402


def _roster(*ids):
    return {i.agent_id: i.public_bundle() for i in ids}


def main():
    agent_m = cx.generate_identity("agent_m")
    agent_b = cx.generate_identity("agent_b")
    intruder = cx.generate_identity("intruder")

    # identity export/import roundtrip
    m2 = cx.identity_from_secret(agent_m.export_secret())
    assert m2.sign_pub == agent_m.sign_pub and m2.box_pub == agent_m.box_pub
    print("identity export/import: OK")

    # ── ENCRYPTED direct message (2-member channel) ──
    roster = _roster(agent_m, agent_b)                    # recipients
    allow_for_agent_b = _roster(agent_m, agent_b)            # agent_b trusts agent_m
    env = cx.seal(agent_m, roster, b"your migration plan has no owner",
                  channel="dm:agent_m:agent_b", kind="challenge", msg_id="m1", ts=time.time())
    assert cx.open_envelope(agent_b, env, allow_for_agent_b) == b"your migration plan has no owner"
    print("encrypted DM agent_m->agent_b: OK")

    # ── ENCRYPTED group (3 members all read the SAME body) ──
    alice = cx.generate_identity("alice")
    g = _roster(agent_m, agent_b, alice)
    allow = _roster(agent_m, agent_b, alice)
    env = cx.seal(alice, g, b"group broadcast", channel="grp:council", kind="msg",
                  msg_id="g1", ts=time.time())
    for member in (agent_m, agent_b, alice):
        assert cx.open_envelope(member, env, allow) == b"group broadcast"
    print("encrypted group (3 members): OK")

    # ── NEGATIVE: non-member cannot read (no wrapped key) ──
    try:
        cx.open_envelope(intruder, env, {**allow, "alice": alice.public_bundle()})
        raise AssertionError("intruder decrypted a group message it wasn't in!")
    except cx.NotAMember:
        print("non-member rejected (NotAMember): OK")

    # ── NEGATIVE: unknown sender (not in allowlist) rejected ──
    env_i = cx.seal(intruder, _roster(agent_b), b"pwn", channel="dm:x", kind="challenge",
                    msg_id="i1", ts=time.time())
    try:
        cx.open_envelope(agent_b, env_i, allow_for_agent_b)   # agent_b does NOT trust intruder
        raise AssertionError("agent_b accepted a message from an untrusted sender!")
    except cx.Unauthorized:
        print("unknown sender rejected (Unauthorized): OK")

    # ── NEGATIVE: tampering breaks the signature ──
    env2 = cx.seal(agent_m, roster, b"real", channel="dm:agent_m:agent_b", kind="challenge",
                   msg_id="m2", ts=time.time())
    env2["ct"] = cx._b64e(b"tampered-ciphertext")
    try:
        cx.open_envelope(agent_b, env2, allow_for_agent_b)
        raise AssertionError("tampered envelope accepted!")
    except cx.Unauthorized:
        print("tamper detected (Unauthorized): OK")

    # ── OPEN channel: signed plaintext, authorship verified, tamper caught ──
    o = cx.seal_open(agent_m, b"public notice", channel="open:lobby", kind="msg",
                     msg_id="o1", ts=time.time())
    assert cx.open_envelope(agent_b, o, allow_for_agent_b) == b"public notice"
    o["pt"] = cx._b64e(b"forged")
    try:
        cx.open_envelope(agent_b, o, allow_for_agent_b)
        raise AssertionError("forged open message accepted!")
    except cx.Unauthorized:
        print("open channel: signed OK + forgery rejected: OK")

    print("\nNMEM-EXCHANGE CRYPTO: ALL CHECKS PASS")


if __name__ == "__main__":
    main()
