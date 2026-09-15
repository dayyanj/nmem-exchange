"""End-to-end crypto for nmem-exchange.

Each agent holds two keypairs (PyNaCl / libsodium):
  - Ed25519 SIGNING key  → authorship (who sent it)
  - X25519 BOX key       → encryption (who can read it)

ENCRYPTED channels use multi-recipient envelope encryption:
  1. the body is sealed ONCE with a fresh random symmetric key (XSalsa20-Poly1305);
  2. that key is wrapped to EACH roster member via an anonymous SealedBox (X25519);
  3. the whole envelope is signed by the sender (Ed25519).
So the broker only ever sees ciphertext + per-recipient wrapped keys; only a
rostered member can unwrap its key and read; the signature proves authorship and
prevents tampering.

OPEN channels are signed-only (plaintext, authorship verified) — for genuinely
public group chat, without a false promise of secrecy.

Inbound is authenticated against an ALLOWLIST of trusted peer public keys: a
message from an unknown signer is rejected outright.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from nacl import public, secret, signing, utils
from nacl.exceptions import BadSignatureError, CryptoError


class ExchangeCryptoError(Exception):
    pass


class Unauthorized(ExchangeCryptoError):
    """Sender not in the allowlist, or bad signature."""


class NotAMember(ExchangeCryptoError):
    """This agent has no wrapped key for an encrypted envelope."""


def _b64e(b: bytes) -> str:
    return base64.b64encode(b).decode()


def _b64d(s: str) -> bytes:
    return base64.b64decode(s)


# ── identity (an agent's own keys) + peer public bundle ──────────────────────
@dataclass
class Identity:
    agent_id: str
    signing_key: signing.SigningKey       # Ed25519 private
    box_key: public.PrivateKey            # X25519 private

    @property
    def sign_pub(self) -> str:
        return _b64e(bytes(self.signing_key.verify_key))

    @property
    def box_pub(self) -> str:
        return _b64e(bytes(self.box_key.public_key))

    def public_bundle(self) -> dict:
        """The public keys to publish into a roster/allowlist."""
        return {"agent_id": self.agent_id, "sign_pub": self.sign_pub, "box_pub": self.box_pub}

    def export_secret(self) -> dict:
        """Serialise the PRIVATE keys for the agent's local keyfile (never shared)."""
        return {"agent_id": self.agent_id,
                "sign_seed": _b64e(bytes(self.signing_key)),
                "box_priv": _b64e(bytes(self.box_key))}


def generate_identity(agent_id: str) -> Identity:
    return Identity(agent_id, signing.SigningKey.generate(), public.PrivateKey.generate())


def identity_from_secret(secret_dict: dict) -> Identity:
    return Identity(
        secret_dict["agent_id"],
        signing.SigningKey(_b64d(secret_dict["sign_seed"])),
        public.PrivateKey(_b64d(secret_dict["box_priv"])),
    )


# ── signing (over the canonical, sig-excluded envelope) ──────────────────────
def _signable(env: dict) -> bytes:
    e = {k: v for k, v in env.items() if k != "sig"}
    return json.dumps(e, sort_keys=True, separators=(",", ":")).encode()


def _sign(sender: Identity, env: dict) -> str:
    return _b64e(sender.signing_key.sign(_signable(env)).signature)


def verify_sig(env: dict, sender_sign_pub: str) -> bool:
    try:
        signing.VerifyKey(_b64d(sender_sign_pub)).verify(_signable(env), _b64d(env["sig"]))
        return True
    except (BadSignatureError, CryptoError, KeyError, ValueError):
        return False


# ── seal / open ──────────────────────────────────────────────────────────────
def seal(sender: Identity, roster: dict, payload: bytes, *, channel: str, kind: str,
         msg_id: str, ts: float, in_reply_to: str | None = None) -> dict:
    """Build an ENCRYPTED envelope readable only by `roster` members.

    roster: {agent_id: {"box_pub": ..., "sign_pub": ...}} of the recipients.
    """
    msg_key = utils.random(secret.SecretBox.KEY_SIZE)
    ct = secret.SecretBox(msg_key).encrypt(payload)         # nonce-prefixed
    wrapped = {}
    for aid, keys in roster.items():
        sealed = public.SealedBox(public.PublicKey(_b64d(keys["box_pub"]))).encrypt(msg_key)
        wrapped[aid] = _b64e(sealed)
    env = {
        "v": 1, "enc": "x25519-secretbox", "channel": channel, "kind": kind,
        "from": sender.agent_id, "to": sorted(roster.keys()),
        "msg_id": msg_id, "in_reply_to": in_reply_to, "ts": ts,
        "ct": _b64e(ct), "keys": wrapped,
    }
    env["sig"] = _sign(sender, env)
    return env


def seal_open(sender: Identity, payload: bytes, *, channel: str, kind: str,
              msg_id: str, ts: float, in_reply_to: str | None = None) -> dict:
    """Build an OPEN envelope: signed (authorship verified) but not encrypted."""
    env = {
        "v": 1, "enc": "none", "channel": channel, "kind": kind,
        "from": sender.agent_id, "msg_id": msg_id, "in_reply_to": in_reply_to,
        "ts": ts, "pt": _b64e(payload),
    }
    env["sig"] = _sign(sender, env)
    return env


def open_envelope(recipient: Identity, env: dict, allowlist: dict) -> bytes:
    """Verify + decrypt an inbound envelope. Raises Unauthorized / NotAMember.

    allowlist: {agent_id: {"sign_pub": ..., "box_pub": ...}} of TRUSTED senders.

    AUTHENTICATION ONLY — this does NOT provide replay resistance. `msg_id` and
    `ts` are signed (so they can't be altered), but dedup + freshness is the
    RECEIVING CLIENT's job (the Exchange client tracks seen msg_ids per
    sender/channel and enforces a ts window); an untrusted broker can otherwise
    replay a previously valid envelope.
    """
    sender = env.get("from")
    peer = allowlist.get(sender)
    if peer is None:
        raise Unauthorized(f"sender {sender!r} not in allowlist")
    if not verify_sig(env, peer["sign_pub"]):
        raise Unauthorized("bad or missing signature")

    if env.get("enc") == "none":
        return _b64d(env["pt"])

    wrapped = (env.get("keys") or {}).get(recipient.agent_id)
    if wrapped is None:
        raise NotAMember(f"no wrapped key for {recipient.agent_id!r}")
    try:
        msg_key = public.SealedBox(recipient.box_key).decrypt(_b64d(wrapped))
        return secret.SecretBox(msg_key).decrypt(_b64d(env["ct"]))
    except CryptoError as e:
        raise ExchangeCryptoError(f"decrypt failed: {e}") from e
