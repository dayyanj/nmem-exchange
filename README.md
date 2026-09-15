# nmem-exchange

A standalone, **pluggable, end-to-end-secured message bus** for nmem agents
(michelle, DJ-AI, refinery agents, and whatever comes next). Sibling to
[nmem](https://github.com/dayyanj/nmem)'s agent core: the core is how you *build*
an agent; the exchange is how agents *talk* — without sharing memory.

## Model: everything is a channel

- A **channel** has an id, a **policy**, and a **roster** (member `agent_id → public keys`).
- A **direct message** is a 2-member channel; a **group** is N members; a
  **fleet broadcast** is a channel whose roster is every agent. One mechanism.
- **Per-channel policy:**
  - `encrypted` — multi-recipient E2E. Body sealed once with a random symmetric
    key; that key wrapped to each member's X25519 key; whole envelope signed
    (Ed25519). Only rostered members can read; sender is authenticated. The
    broker sees only ciphertext.
  - `open` — signed but plaintext (authorship verified, anyone on the bus can
    read). For genuinely public group chat, without a false promise of secrecy.

## Security

- Per-agent keypairs: **Ed25519** (sign/authorship) + **X25519** (encrypt) — PyNaCl.
- Inbound is authenticated against a **config allowlist** of trusted peer public
  keys → no unauthorised bot/human can inject a valid message.
- The broker (Redis) is **untrusted transport**: E2E means a Redis compromise
  leaks nothing and can't forge anything. Redis also gets AUTH + LAN-bind.
- Memory isolation is preserved: the bus is transport only; each agent logs
  exchanges into its **own** nmem.

## Transports (pluggable)

`Transport` is an interface; `RedisTransport` (dedicated Redis on the GPU server,
streams + per-agent consumer groups) is the first implementation. Swapping the
transport touches nothing in the agents' cognitive loops.

## Status

**0.2.0 — early but live.** The crypto core, `RedisTransport`, and the `Exchange` client
are built and running in production between michelle and DJ-AI. The API is still **pre-1.0**
and may change. Not yet adopted by the refinery agents (they still use a plaintext Redis
group channel — migration tracked in the refinery's nmem-agent-core plan). See the
[changelog](CHANGELOG.md).

## The nmem suite

nmem-exchange is one library in a family of composable, framework-agnostic cognitive layers for AI agents. Each is standalone — mix in only the ones you need.

| Repo | Layer | What it does |
|------|-------|--------------|
| [nmem](https://github.com/dayyanj/nmem) | Memory | Hierarchical, self-refining cognitive memory — 6 tiers + a consolidation engine |
| [nmem-sym](https://github.com/dayyanj/nmem-sym) | Reasoning | Symbolic cognition — typed knowledge graph, spreading activation, drives, prediction |
| [nmem-sym-sensor](https://github.com/dayyanj/nmem-sym-sensor) | Perception | Sensory memory — unlabeled visual/audio primitives clustered into grounded concepts |
| [nmem-identity](https://github.com/dayyanj/nmem-identity) | Perception | Self-supervised person identity (voice + face) — learns who it's talking to, no manual tagging |
| [nmem-act](https://github.com/dayyanj/nmem-act) | Action | Typed action/outcome contract + tiered autonomy gate — act to learn, safely |
| [nmem-sandbox](https://github.com/dayyanj/nmem-sandbox) | Action | LLM-agnostic computer-use sandbox — a headless desktop a vision model drives |
| [nmem-exchange](https://github.com/dayyanj/nmem-exchange) | Comms | End-to-end-secured message bus — agents talk without sharing memory |
| [nmem-immune](https://github.com/dayyanj/nmem-immune) | Integrity | Memory immune system — poisoning detection, drift, quarantine |
| [nmem-viz](https://github.com/dayyanj/nmem-viz) | Tooling | Real-time 3D "brain" visualization of any nmem agent |

**Just want to run one?** [**nmem-studio**](https://hub.docker.com/r/dayyanj/nmem-studio) is the pull-and-run appliance — a single Docker image that stands up one fully-configured agent from a web wizard (memory + reasoning, plus optional identity and embodied perception), no config files by hand.
