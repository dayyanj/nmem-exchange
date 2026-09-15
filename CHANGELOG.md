# Changelog

All notable changes to nmem-exchange are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] — 2026-09-08

**Transport hardened and live between agents.** Still pre-1.0 (the API may move), but the
bus now runs in production between michelle and DJ-AI.

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
