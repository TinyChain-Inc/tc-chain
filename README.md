# tc-chain

`tc-chain` is the transport-neutral owner of TinyChain durable history and
reconciliation semantics. `SyncChain` provides a PUT/DELETE write-ahead
log for BTree and Table subjects. The canonical [Chain contract](CHAIN_CONTRACT.md)
defines durability, recovery, and ownership.

Chain does not replace resource routing, host transaction allocation, or local
collection behavior. A Service owns a public named resource, a Cluster owns its
leadership and propagation, Chain owns ordered replayable history, and the
collection owns local state transitions.

## Status

- Common Chain ownership contract: documented.
- SyncChain WAL and restart replay: implemented.
- Snapshot import and replacement: unsupported.
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
Existing Chain subjects cannot be replaced from snapshots.

Reopen the subject, WAL, and value-store roots through fresh caller-owned caches,
strictly load the collection, then call
`SyncChain::load(subject, wal, values, fresh_queue, resolver)`. The resolver
supplies capabilities carrying the original recorded transaction IDs. Chain never
constructs a cache, chooses host paths, or allocates transaction IDs.

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
File codecs own serialization and checksums. Finalization synchronizes native canonical storage before
removing covered WAL records, without serializing a full checkpoint.
Unused captured collections are reclaimed only after successful recovery during
loading. Rollback and finalization leave those files in place; coordinate
maintenance by closing and reopening the Chain. Storage admission continues to
apply while unused captures remain. See the [Chain contract](CHAIN_CONTRACT.md)
for the lifecycle and capture isolation guarantees.

This direct request log does not provide arbitrary partial-finalization recovery.
For example, replaying a strict Table insert after its canonical effect became
durable can conflict. Loading then fails and retains the WAL. The contract
documents this open acceptance requirement; production Service/Cluster support
also remains unimplemented.

The WAL uses the shared semantic block format described in the
[Chain contract](CHAIN_CONTRACT.md), in one `committed.chain_block` file.
The retained block must fit delegated cache admission, including replacement
headroom. Mutating commits rewrite retained history, trading write traffic for
fewer files and simpler publication. Recreate development fixtures from earlier
index/batch layouts instead of migrating them.

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
