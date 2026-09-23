# tc-chain

`tc-chain` is the transport-neutral owner of TinyChain durable history and
reconciliation semantics. `SyncChain` provides a PUT/DELETE write-ahead
log for BTree and Table subjects. The canonical [Chain contract](CHAIN_CONTRACT.md)
defines durability, recovery, and ownership.

[Crate-level acceptance evidence](ACCEPTANCE.md) records recovery tests and
durability measurements for Issue #2.

Chain does not replace resource routing, host transaction allocation, or local
collection behavior. A Service owns a public named resource, a Cluster owns its
leadership and propagation, Chain owns ordered replayable history, and the
collection owns local state transitions.

## Status

- Common Chain ownership contract: documented.
- SyncChain WAL and restart replay: implemented.
- Native transactional snapshot replacement: implemented for BTree and Table.
- BlockChain: planned.
- MetricsChain: planned.
- Host resynchronization driven by Chain history: planned.

Public Service hosting and cross-host synchronization are not supported by this
crate-level implementation. Other crates delegate recovery to Chain rather than
adding their own logs or repair policy.

## Native use

Create or strictly load a persistent BTree/Table using its native collection API.
Pass that subject, an empty caller-owned `freqfs::DirLock<ChainFile>`, an empty
`freqfs::DirLock<Txn::File>` for captured collection values, and a fresh
`TxnTaskQueue<MutationRecord>` constructed with the caller's positive per-transaction
capacity to `SyncChain::create(subject, wal, values, queue)`. Creation synchronizes the
initial canonical subject; it does not publish pending versions.

Use ordinary `Route<State>`, `Public<State>`, `IntoView`, and `Transact`.
`SyncChain::hash(&txn)` delegates transaction-visible subject hashing to the collection.
`SyncChain::restore_from(&txn, &snapshot)` records native replacement through the
same WAL and transactional Collection boundary; kind and schema must match.

Reopen WAL and value-store handles through fresh caller-owned caches and call
`SyncChain::load(subject_loader, wal, values, fresh_queue, resolver)`. The async
loader strictly loads the subject only after Chain checks for unfinished
materialization. The resolver supplies original-ID capabilities and fresh delegated
workspaces. Chain never constructs caches, chooses host paths, or allocates IDs.

Storage admission belongs to the delegated cache and filesystem. Chain adds no
transaction-count, mutation-count, or WAL-byte quota. Its in-memory transaction
records remain retained until rollback or finalization as described by the
[resource contract](CHAIN_CONTRACT.md#failure-and-resource-semantics).
Delegate each queue exclusively to one Chain; do not independently drive its
permits or share it across resources. Queues with existing transactions or a
finalized frontier are rejected at creation/loading.

Scalar writes allocate no additional WAL files. Commit atomically replaces the
committed block with its retained history plus the new ordered records, referencing
native collection copies captured earlier. It does not copy those arguments again.
File codecs own serialization and checksums. The single WAL retains requests
through finalization. An unfinished materialization marker requires authoritative
resynchronization; local loading refuses the resource. See the
[Chain contract](CHAIN_CONTRACT.md) for durability, cleanup and recovery boundaries.
Automatic Service/Cluster resynchronization remains unimplemented.

Each mutating commit rewrites retained history. The WAL must fit delegated cache
admission including replacement headroom. Recreate development fixtures from earlier
WAL and native layouts; there is no migration or format-version field.

## Development

The path dependencies in `Cargo.toml` require sibling TinyChain crate checkouts
and the listed repositories under `../deps`. Run these commands from this crate:

```bash
cargo test --all-targets --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Run the filesystem timing comparison separately from correctness tests:

```bash
cargo test wal_batch_benchmark -- --ignored --nocapture
```

See [crate invariants](AGENTS.md) and the [roadmap](ROADMAP.md). The TinyChain
[workspace architecture](https://github.com/TinyChain-Inc/tcv2/blob/main/ARCHITECTURE.md)
is non-normative integration context.
