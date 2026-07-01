# tc-chain Agent Notes

This crate is the control-plane ledger and transaction coordinator. Keep it minimal
and protocol-focused so other runtimes and clients can consume its contracts without
pulling in host-specific details.

## Design and contracts

- Treat `tc-chain` as the canonical control-plane surface. Avoid introducing runtime-
  specific types—express new capabilities in terms of the existing ledger structures
  and IR-friendly claims/links.
- Maintain compatibility with the v1 transaction envelope and MetricsChain reporting.
  When a change affects attestation or telemetry, update `METRICSCHAIN_GUIDELINES.md`
  alongside code and README notes so validators stay aligned.
- Prefer feature flags over new crates for optional adapters. Default features should
  preserve the minimal kernel so downstream hosts can embed it without extra surface
  area.

## Testing and documentation

- Run `cargo test -p tc-chain` for changes touching the ledger, transaction routing,
  or storage. Add targeted unit tests instead of introducing fallback logic.
- Document observable behavior changes in the crate README or roadmap before relying
  on them in other layers.
