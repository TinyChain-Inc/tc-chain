# Contributing to tc-chain

`tc-chain` is the control-plane ledger surface, so every change must preserve
compatibility with existing hosts and clients. Use this checklist whenever you
extend the crate.

## Prerequisites

- Read the workspace `AGENTS.md`, `ARCHITECTURE.md`, and this crate’s
  `AGENTS.md` to understand the invariants (3-second synchronous budget, queue
  model, MetricsChain reporting).
- Follow the shared style guide in `CODE_STYLE.md`—grouped imports, rustfmt, and
  clippy-clean builds apply here as well.

## Development workflow

1. **Plan the change.** Document ledger/telemetry updates in `ROADMAP.md` or
   `METRICSCHAIN_GUIDELINES.md` before coding so downstream adapters can track
   the migration.
2. **Keep the API transport-agnostic.** New structs or fields must compile on
   `no_std`-friendly targets and avoid pulling in host-only dependencies.
3. **Testing.**
   - Run `cargo test -p tc-chain` for every change.
   - Add serialization round-trip tests whenever you touch `destream` or `serde`
     impls so clients can rely on the same envelope.
   - If the change affects queue discipline or MetricsChain counters, include
     focused unit tests exercising those paths.
4. **Documentation.** Update `README.md`, `AGENTS.md`, or
   `METRICSCHAIN_GUIDELINES.md` when contracts change. Call out any required
   host/client coordination explicitly.
5. **No fallback paths.** Prefer direct fixes or refactors instead of layering
   compatibility shims—`tc-chain` defines the single correct flow.

## Before opening a PR

- Ensure `cargo fmt` and `cargo clippy --all-targets --all-features` pass.
- Confirm tests pass locally.
- Include a summary of how the change affects hosts, clients, and any rollout
  considerations (dual-write, queue migrations, etc.).

## Rights and licensing

By contributing to this crate you represent that (a) you authored the work (or
otherwise have the rights to contribute it) and (b) you transfer and assign all
right, title, and interest in the contribution to the TinyChain Open-Source
Project for distribution under the TinyChain open-source license (Apache 2.0,
see the root `LICENSE`). Contributions must be free of third-party claims or
encumbrances.
