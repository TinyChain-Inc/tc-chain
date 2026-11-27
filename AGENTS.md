# tc-chain invariants

The repository-local [`CHAIN_CONTRACT.md`](CHAIN_CONTRACT.md) is the canonical
behavioral contract.

- Own durable ordered history, deterministic replay, canonical-state selection,
  divergence reconciliation, and coordinated resynchronization.
- Remain transport-neutral and express behavior through shared IR, State, Route,
  Handler, Transact, and collection contracts.
- Preserve the original protocol `TxnId`; do not create subtransactions,
  transaction aliases, or a second transaction manager.
- Preserve the recovery source defined by the variant: SyncChain's durable
  persistent subject plus retained request WAL, or a retained-history variant's
  complete history. Never reinterpret a replay conflict as success.
- Fail closed on missing referenced data or observed corrupt, conflicting, or unverifiable history. Do not
  skip records or add local repair heuristics.
- Apply the local [failure and resource semantics](CHAIN_CONTRACT.md#failure-and-resource-semantics)
  to ingest, replay, validation, and resynchronization. Callers supply deadlines
  and bounded capacity; Chain does not choose host policy.
- Implement SyncChain, BlockChain, and MetricsChain as policies over the one
  Chain boundary. Do not create variant-specific adapters, routing trees, or
  transaction lifecycles.
- Keep proposed schemas and policies in the roadmap until code and conformance
  tests make them real.
