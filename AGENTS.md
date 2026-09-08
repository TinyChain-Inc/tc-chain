# tc-chain invariants

Follow the workspace [`AGENTS.md`](../AGENTS.md) and the canonical
[`CHAIN_CONTRACT.md`](CHAIN_CONTRACT.md).

- Own durable ordered history, deterministic replay, canonical-state selection,
  divergence reconciliation, and coordinated resynchronization.
- Remain transport-neutral and express behavior through shared IR, State, Route,
  Handler, Transact, and collection contracts.
- Preserve the original protocol `TxnId`; do not create subtransactions,
  transaction aliases, or a second transaction manager.
- Treat snapshots and indexes as derived. Never let a materialized collection or
  host cache become the authoritative replay source.
- Fail closed on missing, corrupt, conflicting, or unverifiable history. Do not
  skip records or add local repair heuristics.
- Apply the workspace backpressure and deadline contracts to ingest, replay,
  validation, and resynchronization.
- Implement SyncChain, BlockChain, and MetricsChain as policies over the one
  Chain boundary. Do not create variant-specific adapters, routing trees, or
  transaction lifecycles.
- Keep proposed schemas and policies in the roadmap until code and conformance
  tests make them real.
