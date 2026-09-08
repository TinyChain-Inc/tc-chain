# tc-chain

`tc-chain` is the transport-neutral owner of TinyChain durable history and
reconciliation semantics. The implementations are still planned; the canonical
[Chain contract](CHAIN_CONTRACT.md) fixes the boundary they must share.

Chain does not replace resource routing, host transaction allocation, or local
collection behavior. A Service owns a public named resource, a Cluster owns its
leadership and propagation, Chain owns ordered replayable history, and the
collection owns local state transitions.

## Status

- Common Chain ownership contract: documented.
- SyncChain: planned.
- BlockChain: planned.
- MetricsChain: planned.
- Host resynchronization driven by Chain history: planned.

Until these exist, other crates fail closed on ambiguous history rather than
adding a WAL, replay log, majority heuristic, or recovery shim.

## Development

```bash
cargo test --all-targets --all-features
```

See [crate invariants](AGENTS.md), the [roadmap](ROADMAP.md), and the workspace
[architecture](../ARCHITECTURE.md).
