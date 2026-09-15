"""The Exchange client — ties crypto + transport + config together.

An agent constructs one Exchange, `start(handler)`s a receive loop over the
channels it's a member of, and `send(channel, kind, body)`s messages. Inbound
messages are verified + decrypted, replay-deduped, freshness-checked, then handed
to the host handler (which logs into the agent's OWN nmem and, e.g., runs a peer
challenge through its cognitive loop).
"""
from __future__ import annotations

import collections
import json
import logging
import time
import uuid

from nmem_exchange import crypto
from nmem_exchange.transport import build_transport

log = logging.getLogger(__name__)


class Exchange:
    def __init__(self, identity: crypto.Identity, keyring: dict, channels: dict, transport,
                 *, ts_window_s: float = 300.0, dedup_max: int = 5000):
        self.id = identity
        self.keyring = keyring            # {agent_id: {sign_pub, box_pub}} — trusted peers (allowlist), incl self
        self.channels = channels          # {ch_id: {"policy": "encrypted"|"open", "roster": [agent_id,...]}}
        self.transport = transport
        self.ts_window_s = ts_window_s
        self._seen: "collections.OrderedDict" = collections.OrderedDict()
        self._dedup_max = dedup_max
        self._handler = None

    async def send(self, channel: str, kind: str, body, in_reply_to: str | None = None) -> str:
        ch = self.channels[channel]
        payload = body if isinstance(body, bytes) else str(body).encode()
        msg_id = uuid.uuid4().hex
        ts = time.time()
        if ch["policy"] == "encrypted":
            roster = {aid: self.keyring[aid] for aid in ch["roster"]}   # wrap to all members (incl self)
            env = crypto.seal(self.id, roster, payload, channel=channel, kind=kind,
                              msg_id=msg_id, ts=ts, in_reply_to=in_reply_to)
        else:
            env = crypto.seal_open(self.id, payload, channel=channel, kind=kind,
                                   msg_id=msg_id, ts=ts, in_reply_to=in_reply_to)
        await self.transport.publish(channel, json.dumps(env).encode())
        return msg_id

    async def start(self, handler):
        """handler(meta: dict, body: bytes) — awaited per accepted inbound message."""
        self._handler = handler
        for ch_id, ch in self.channels.items():
            if self.id.agent_id in ch.get("roster", []):
                # Bind the delivery channel into the callback so we can reject an
                # envelope the broker injected into the wrong stream.
                await self.transport.subscribe(
                    ch_id, self.id.agent_id,
                    lambda blob, ch_id=ch_id: self._on_blob(ch_id, blob))

    async def _on_blob(self, ch_id, blob):
        if blob is None:
            return
        try:
            env = json.loads(blob)
        except Exception:
            return
        if env.get("from") == self.id.agent_id:
            return  # our own message fanned back to us
        # 1) authenticate + decrypt (allowlist + signature enforced here)
        try:
            body = crypto.open_envelope(self.id, env, self.keyring)
        except crypto.NotAMember:
            return
        except crypto.Unauthorized as e:
            log.warning("[nmex] rejected message: %s", e)
            return
        except crypto.ExchangeCryptoError as e:
            log.warning("[nmex] decrypt failed: %s", e)
            return
        # 2) channel AUTHORIZATION on the VERIFIED envelope — an untrusted broker
        # must not be able to cross-post, and the signed channel/policy must match
        # our local config + roster.
        ch = self.channels.get(ch_id)
        if ch is None:
            return
        if env.get("channel") != ch_id:
            log.warning("[nmex] channel mismatch: signed %s delivered on %s", env.get("channel"), ch_id)
            return
        roster = ch.get("roster", [])
        if self.id.agent_id not in roster or env.get("from") not in roster:
            log.warning("[nmex] roster check failed on %s (from=%s)", ch_id, env.get("from"))
            return
        enc = env.get("enc")
        policy = ch.get("policy")
        if (policy == "encrypted" and enc != "x25519-secretbox") or (policy == "open" and enc != "none"):
            log.warning("[nmex] policy/enc mismatch on %s (policy=%s enc=%s)", ch_id, policy, enc)
            return
        # 3) freshness window (ts is signed, so it can't be altered)
        ts = float(env.get("ts") or 0)
        if abs(time.time() - ts) > self.ts_window_s:
            log.warning("[nmex] dropped stale/future message from %s", env.get("from"))
            return
        # 4) replay dedup (msg_id is signed)
        key = (env.get("from"), env.get("channel"), env.get("msg_id"))
        if key in self._seen:
            log.warning("[nmex] dropped replay %s", key)
            return
        self._seen[key] = True
        if len(self._seen) > self._dedup_max:
            self._seen.popitem(last=False)
        meta = {k: env.get(k) for k in ("from", "channel", "kind", "msg_id", "in_reply_to", "ts")}
        if self._handler:
            await self._handler(meta, body)

    async def close(self):
        await self.transport.close()


def load_identity(cfg: dict) -> crypto.Identity:
    """Load the agent's own keypair from its keyfile (JSON: export_secret dict)."""
    ident = cfg["identity"]
    with open(ident["keyfile"]) as f:
        return crypto.identity_from_secret(json.load(f))


def build_exchange(cfg: dict) -> Exchange:
    """Build an Exchange from an `exchange:` config block:

    exchange:
      identity:  {agent_id, keyfile}
      transport: {kind: redis|memory, url}
      keyring:   {agent_id: {sign_pub, box_pub}, ...}   # trusted peers incl self
      channels:  {ch_id: {policy: encrypted|open, roster: [agent_id,...]}}
    """
    identity = load_identity(cfg)
    transport = build_transport(cfg.get("transport", {}))
    return Exchange(identity, cfg.get("keyring", {}), cfg.get("channels", {}), transport,
                    ts_window_s=float(cfg.get("ts_window_s", 300.0)))
