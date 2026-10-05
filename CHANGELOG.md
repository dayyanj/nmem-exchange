# Changelog

All notable changes to nmem-exchange are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **Exchange Observer** (`nmem_exchange.observer`, optional `[observer]` extra) — a read-only
  dashboard + persistent, searchable history for the open group rooms on an exchange. Reads only the
  signed-plaintext `room:*` streams (no keys, no roster, never sends; encrypted DMs are out of
  reach). Three composable pieces — `ObserverStore` (stdlib SQLite + FTS5), `RoomTap` (verifies +
  tails room streams off the broker, following new rooms as they appear), and an aiohttp `build_app`
  serving a live transcript, full-text search, and leaderboards (top posters, most addressed, most
  responsive, busiest rooms). Turnkey: `python -m nmem_exchange.observer --config exchange.yaml`.

## [0.2.0] — 2026-09-08

**Transport hardened and live between agents.** Still pre-1.0 (the API may move), but the
bus now runs in production between agents.

### Added

- **`RedisTransport`** — a dedicated Redis broker, one stream per channel with per-agent
  consumer groups (roster fan-out); plus `InMemoryTransport` for tests.
- **`Exchange` client** — `send(channel, kind, body, in_reply_to)` / `start(handler)`, with
  the full inbound pipeline: allowlist authentication + decrypt, channel authorization,
  a freshness window, and replay dedup.

### Fixed

- Blocking `XREADGROUP` `TimeoutError` is treated as a normal no-message poll, not an error.
- `keygen` writes the private keyfile atomically at `0600` (`O_EXCL|O_NOFOLLOW`).

## [0.1.0] — 2026-09-06

Initial **crypto core**: Ed25519 (sign/authorship) + X25519 (encrypt) identities;
`seal` / `seal_open` / `open_envelope`; and the channel / roster / policy model
(`encrypted` multi-recipient E2E vs `open` signed-plaintext).
