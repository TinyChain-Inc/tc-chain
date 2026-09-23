# tc-chain invariants

The repository-local [`CHAIN_CONTRACT.md`](CHAIN_CONTRACT.md) is the canonical
behavioral contract. The parent
[workspace invariants](https://github.com/TinyChain-Inc/tcv2/blob/main/AGENTS.md)
are non-normative integration context when this repository is used as a submodule.

- Own durable ordered history, deterministic replay, canonical-state selection,
  divergence reconciliation, and coordinated resynchronization.
- Remain transport-neutral and express behavior through shared IR, State, Route,
  Handler, Transact, and collection contracts.
- Preserve the original protocol `TxnId`; do not create subtransactions,
  transaction aliases, or a second transaction manager.
- Preserve the recovery source defined by the variant: SyncChain's durable
  persistent subject plus retained request WAL, or a retained-history variant's
  complete history. Never reinterpret a replay conflict as success.
- Fail closed on missing, corrupt, conflicting, or unverifiable history. Do not
  skip records or add local repair heuristics.
- Apply the workspace backpressure and deadline contracts to ingest, replay,
  validation, and resynchronization.
- Implement SyncChain, BlockChain, and MetricsChain as policies over the one
  Chain boundary. Do not create variant-specific adapters, routing trees, or
  transaction lifecycles.
- Keep proposed schemas and policies in the roadmap until code and conformance
  tests make them real.
