# tc-chain

`tc-chain` is the transport-neutral owner of TinyChain durable history and
reconciliation semantics. `SyncChain` provides a PUT/DELETE write-ahead
log for BTree, Table, and Tensor subjects. The canonical [Chain contract](CHAIN_CONTRACT.md)
defines durability, recovery, and ownership.

Chain does not replace resource routing, host transaction allocation, or local
collection behavior. A Service owns a public named resource, a Cluster owns its
leadership and propagation, Chain owns ordered replayable history, and the
collection owns local state transitions.

## Status

- Common Chain ownership contract: documented.
- SyncChain WAL and restart replay: implemented.
- Native transactional snapshot replacement: implemented for BTree, Table, and Tensor.
- BlockChain: planned.
- MetricsChain: planned.
- Host resynchronization driven by Chain history: planned.

Public Service hosting and cross-host synchronization are not supported by this
crate-level implementation. Other crates delegate recovery to Chain rather than
adding their own logs or repair policy.

## Native use

Create or strictly load a persistent BTree, Table, or Tensor using its native collection API.
Pass that subject, an empty caller-owned `freqfs::DirLock<ChainFile>`, an empty
`freqfs::DirLock<Txn::File>` for captured collection values, and a positive
per-transaction capacity to `SyncChain::create(subject, wal, values, capacity)`.
Creation synchronizes the
initial canonical subject; it does not publish pending versions. Handing the subject
to Chain transfers exclusive mutable ownership: callers relinquish mutable aliases
and must not independently reopen its storage. Every application mutation passes
through Chain; returned collection views are read-only. Selection-write conveniences
address the original owner with explicit bounds.

Use ordinary `Route<State>`, `Public<State>`, `IntoView`, and `Transact`.
`SyncChain::hash(&txn)` delegates transaction-visible subject hashing to the collection.
`SyncChain::restore_from(&txn, &snapshot)` records native replacement through the
same WAL and transactional Collection boundary; kind and schema must match.

Reopen WAL and value-store handles through fresh caller-owned caches and call
`SyncChain::load::<State<_>, _, _, _, _>(subject_loader, wal, values, capacity, resolver)`. The async
loader strictly reconstructs durable native versions after Chain validates its
frontier. The resolver supplies original-ID capabilities only for decisions that
still need replay. Chain never constructs caches, chooses host paths, or allocates IDs.

Storage admission belongs to the delegated cache and filesystem. Chain bounds
pending mutations per transaction. Its in-memory transaction
records remain retained until rollback or finalization as described by the
[resource contract](CHAIN_CONTRACT.md#failure-and-resource-semantics).

Scalar preparation allocates no WAL files. Each mutating commit durably publishes
one immutable transaction file. Collection arguments materialize directly into
transaction-owned capture storage and are applied without another copy or reload.
Rollback and finalization reclaim captures during live operation. File codecs own
serialization and checksums. The WAL retains requests through finalization.
Native commit and compaction recover through Collection's immutable publication.
See the
[Chain contract](CHAIN_CONTRACT.md) for durability, cleanup and recovery boundaries.
Automatic Service/Cluster resynchronization remains unimplemented.

Each transaction file must fit delegated cache admission. The caller bounds live
transactions and advances finalization. Recreate development fixtures from earlier
WAL and native layouts; there is no migration or compatibility reader.

## Development

The path dependencies in `Cargo.toml` require sibling TinyChain crate checkouts
and the listed repositories under `../deps`. See [CONTRIBUTING.md](CONTRIBUTING.md)
for correctness, lint, and normal-stack validation commands.

Run the filesystem timing comparison separately from correctness tests:

```bash
cargo test wal_batch_benchmark -- --ignored --nocapture
```

See [crate invariants](AGENTS.md) and the [roadmap](ROADMAP.md).
