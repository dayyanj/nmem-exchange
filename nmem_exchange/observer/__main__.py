"""Run the exchange observer: ``python -m nmem_exchange.observer [options]``.

Point it at any fleet member's exchange config (for the keyring + broker URL) and a database file;
it tails the open room channels into the store and serves the dashboard.

  python -m nmem_exchange.observer --config exchange.yaml --db fleet.db --port 8900

``--config`` reads the ``exchange:`` block (``transport.url`` + ``keyring``), expanding ``${ENV}``
placeholders. ``--url`` / a keyring file can override. The observer needs read access to the same
Redis broker the fleet uses; it holds no private keys.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys

from nmem_exchange.observer.ingest import RoomTap
from nmem_exchange.observer.store import ObserverStore

log = logging.getLogger("nmem_exchange.observer")

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _redact(url: str) -> str:
    """Mask the password in a broker URL so it never reaches logs (``redis://:pw@host`` → ``:***@``)."""
    return re.sub(r"(://[^:@/]*:)[^@]*(@)", r"\1***\2", url or "")


def _expand(v):
    """Expand ${ENV} placeholders in a string using the environment (unset → left as-is)."""
    if not isinstance(v, str):
        return v
    return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), m.group(0)), v)


def _yaml():
    try:
        import yaml
        return yaml
    except ModuleNotFoundError:  # pragma: no cover
        sys.exit("reading YAML needs PyYAML — install the extra: pip install 'nmem-exchange[observer]'")


def _load_config(path: str) -> dict:
    """An exchange config (``exchange.transport.url`` + ``exchange.keyring``)."""
    with open(path) as f:
        doc = _yaml().safe_load(f) or {}
    ex = doc.get("exchange", doc)                              # tolerate a bare or nested block
    url = _expand(((ex.get("transport") or {}).get("url")))
    return {"url": url, "keyring": ex.get("keyring", {}) or {}}


def _keyring_from_hive(path: str) -> dict:
    """Build the trusted-signer keyring from a HIVE DESCRIPTOR — the artifact a studio/hive fleet
    already has (``hive.members[]`` with ``sign_pub``/``box_pub``). Lets the observer run as a studio
    sidecar with no bespoke exchange.yaml: ``--hive /data/<agent>/hive.yaml`` + ``NMEX_REDIS_URL``."""
    with open(path) as f:
        doc = _yaml().safe_load(f) or {}
    hive = doc.get("hive", doc)
    members = hive.get("members") or []
    return {m["agent_id"]: {"sign_pub": m["sign_pub"], "box_pub": m.get("box_pub", "")}
            for m in members if m.get("agent_id") and m.get("sign_pub")}


def _parse_args(argv):
    p = argparse.ArgumentParser(prog="nmem_exchange.observer",
                                description="Read-only observer for open exchange rooms.")
    p.add_argument("--config", help="exchange YAML config (for keyring + broker url)")
    p.add_argument("--hive", help="hive descriptor YAML — build the keyring from its members "
                   "(studio/hive fleets); broker url then comes from --url or $NMEX_REDIS_URL")
    p.add_argument("--url", help="redis broker url (overrides --config / $NMEX_REDIS_URL)")
    p.add_argument("--db", default="nmem-observer.db", help="SQLite file (default: nmem-observer.db)")
    p.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=8900, help="bind port (default: 8900)")
    p.add_argument("--rescan", type=float, default=15.0, help="room re-discovery interval, seconds")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


async def _serve(store, tap, host, port):
    from aiohttp import web

    from nmem_exchange.observer.server import LiveHub, build_app
    hub = LiveHub()
    tap._on_message = hub.publish                             # wire live fan-out into the tap
    app = build_app(store, hub=hub)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    log.info("observer dashboard: http://%s:%d", host, port)
    tap_task = asyncio.create_task(tap.run())
    try:
        await asyncio.Event().wait()                          # run until cancelled (Ctrl-C)
    finally:
        tap_task.cancel()
        await tap.stop()
        await runner.cleanup()


def main(argv=None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    url, keyring = args.url, {}
    if args.config:
        cfg = _load_config(args.config)
        url = url or cfg["url"]
        keyring = cfg["keyring"] or keyring
    if args.hive:
        keyring = _keyring_from_hive(args.hive) or keyring
    url = url or os.environ.get("NMEX_REDIS_URL")              # studio/hive sidecar sets this in-env
    if not url:
        sys.exit("no broker url — pass --url, a --config, or set NMEX_REDIS_URL")
    if not keyring:
        log.warning("empty keyring — every envelope will be rejected as an unknown signer; "
                    "pass --config with the fleet's exchange.keyring")
    store = ObserverStore(args.db)
    tap = RoomTap(url, keyring, store, rescan_interval=args.rescan)
    log.info("observing rooms on %s → %s", _redact(url), args.db)
    try:
        asyncio.run(_serve(store, tap, args.host, args.port))
    except KeyboardInterrupt:
        pass
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
