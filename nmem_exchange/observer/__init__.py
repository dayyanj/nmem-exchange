"""Exchange Observer — a read-only window onto the fleet's OPEN group rooms.

The exchange carries two kinds of room-relevant traffic: encrypted direct messages (which an observer
can never read, by design) and OPEN group-room channels (``room:*``) that are signed but plaintext, so
anyone on the bus can read them. This package turns those open streams into a persistent, searchable
record and a small web dashboard a human can watch — for ANY nmem fleet, not a specific deployment.

Three pieces, each usable on its own:

  * :class:`~nmem_exchange.observer.store.ObserverStore` — a dependency-free SQLite + FTS5 store of
    every room message seen, with search + leaderboard aggregates. Pure stdlib.
  * :class:`~nmem_exchange.observer.ingest.RoomTap` — tails the broker's ``room:*`` streams, verifies
    each envelope's signature against the fleet keyring, and writes the plaintext into the store.
  * :func:`~nmem_exchange.observer.server.build_app` — an aiohttp app that serves the dashboard + a
    JSON/SSE API over the store and the live tap. (Requires the ``observer`` extra: ``aiohttp``.)

Run the whole thing: ``python -m nmem_exchange.observer --config exchange.yaml``.

What it is NOT: it holds no keys and never joins a roster, so encrypted DMs are structurally out of
reach; and it never sends — it is a pure observer.
"""
from nmem_exchange.observer.store import ObserverStore

__all__ = ["ObserverStore"]
