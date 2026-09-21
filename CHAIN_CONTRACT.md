# Chain contract

This document defines the ownership boundary for TinyChain durable history and
the SyncChain write-ahead log. Public Service hosting is a separate integration.

## Ownership

Filesystem writeback and durability are distinct: ordinary `freqfs::sync()` and
eviction are buffered. Chain requests `sync_all()` for captured payloads, the committed
block, and canonical finalization. Cleanup uses `sync_deleted()` to durably
remove unreferenced payloads without synchronizing retained contents. Recursive
canonical synchronization includes unchanged files; selective canonical flushing
requires an authoritative changed-file set at its storage owner, not mirrored
cache durability flags. Committed-block replacements use
`replace_all()` so eviction cannot publish them before the explicit durability
operation. Callers supply durably established storage roots. These primitives
do not strengthen the partial-finalization recovery guarantee described below.

A persistent named resource composes as:

```text
Cluster<Service>
  -> Service member
  -> Chain
  -> BTree, Table, or Tensor
```

- `Service` owns the public application name, member routing, and application
  authorization.
- `Cluster<T>` owns exact-resource claims, leadership, and replica propagation.
- `TxnServer` owns transaction ID allocation, fixed expiry, the finalized
  frontier, cutoff scheduling, readiness, and workspace cleanup.
- `Chain` owns durable ordered history, replay, canonical-state selection,
  divergence evidence, and coordinated resynchronization.
- A collection owns local data representation, transactional versions, concurrent
  mutation isolation, conflict ordering, and deterministic `Transact` behavior.

No layer may duplicate an adjacent owner's route, transaction lifecycle, log,
or recovery policy.

Collection concurrency and WAL coordination are separate concerns. Chain records
requests and coordinates their complete capture with commit, durable publication,
and reclamation. It does not schedule collection mutations or determine which
keys, rows, ranges, or transactions conflict. Those decisions belong to the
transactional collection and its locking primitives. Ordering records for replay
does not require globally serializing execution of collection handlers.

SyncChain bridges the interval between a transaction's committed visibility and
durable canonical materialization. It publishes recovery records before delegating
subject commit and retains them until canonical finalization is durable. The
collection supplies transactional visibility; WAL publication supplies recovery
evidence. Neither substitutes for the other, and the partial-finalization
limitation below still applies.

## Common behavior

An ordinary mutation produces pending transactional work. A successful Chain
commit acknowledges both durable recovery evidence and completed logical
visibility in its collection. Canonical materialization may follow later; it is
not required for commit acknowledgement when retained history can recover the
committed effects.

Every Chain is an ordinary recursively routed State resource. It preserves the
original protocol `TxnId`, accepts ordinary PUT/DELETE requests, orders them
deterministically, and materializes state by delegating to its collection.
SyncChain recovers by replaying its retained request WAL against its persistent subject. A
retained-history variant may instead make complete history authoritative.

Recovery replays only the complete validated history required by its variant. Missing,
conflicting, corrupt, or unverifiable history fails closed. Chain policy selects
canonical history and coordinates replacement; collections and hosts do not
guess, vote, truncate, or locally repair it.

Chain work uses the ordinary `Handler<State>`, `Route<State>`, and `Transact`
contracts. It does not introduce another transaction identity, transaction
manager, adapter endpoint family, storage transaction trait, or client-managed
lifecycle.

## Variants

- `SyncChain` supplies the minimal ordered history and replay required to
  synchronize a mutable BTree or Table subject. Its WAL is temporary, and its
  persistent subject holds mutations already removed from the WAL.
- `BlockChain` adds cryptographic predecessor/evidence validation to the common
  ordered history.
- `MetricsChain` specializes validation, retention, and reliability policy for
  telemetry and accounting events.

Variants may differ only where their implemented policy requires it. They reuse
the same routing, transaction, admission, replay, and materialization boundaries.
Schemas, quorum rules, retention intervals, and deployment policy are documented
only when their implementations exist.

## SyncChain WAL

PUT and DELETE are the write boundary. Records retain the subject-relative route,
key, PUT value, original protocol transaction ID, and admission order within the
transaction. Replay orders transactions by their complete `TxnId`, without
assuming that transaction IDs form a consecutive numerical sequence. GET is
read-only. POST composes writes through the owning resource's PUT/DELETE boundary;
it may not mutate storage directly. The proposed MetricsChain exception to GET
read-only behavior is not implemented.

The caller delegates a fresh bounded `TxnTaskQueue<MutationRecord>` to one Chain.
The queue owns pending records, admission, live transaction decisions, and WAL
preparation/lifecycle exclusion; the persisted committed block owns durable history.
Queue decision receipts are not a second durable history: publication alone does
not prove completion of subject commit. Historical replay initializes receipts
without consuming pending-task capacity.

Observations check owner status, the cutoff, and existing decisions without
registering pending work. A previously unseen transaction may commit or roll back:
the queue then retains its decision receipt until finalization. Read-only decisions
delegate to the subject without publishing mutation records.

Scalar mutations are admitted and captured in memory. Collection-valued arguments
are captured by native streamed collection copying to retain their value
at invocation. Neither path creates a detached task. A failed or cancelled
mutation prevents that transaction from committing; it may be discarded by
rollback or cutoff finalization.
The caller-held task permit reserves capacity before capture, records the request
before invoking the selected handler, and requires explicit successful completion.
Recording releases preparation exclusion before collection execution. The queue's
`Running` status identifies unfinished requests; completion restores `Active`,
and unfinished drop marks `Failed`. Chain keeps no separate execution token.
The queue's internal mutex is never held across callbacks or awaits. Armed lifecycle
permits latch interruption for all live clones; resource shutdown and recovery
remain caller policy.

Commit synchronizes each distinct captured collection argument, adds the ordered
transaction records to the committed block, and atomically durably replaces that
file before committing the collection. The collection's in-memory visibility
transition completes before acknowledgement; it is not background work. An empty
transaction does not rewrite the WAL. Repeated commit does not repeat its
mutations. A durable commit cannot be reversed by rollback. Errors or cancellation
during publication or subsequent subject commit stop access to that owner until
reload, without acknowledging success. Recovery uses the committed file, never a
guessed outcome.

Finalization delegates the cutoff to the subject, synchronizes its canonical
storage, and atomically durably publishes the new frontier and remaining records.
It runs later under caller coordination and does not determine the commit decision.
It remains durability-critical: canonical merging and synchronization must succeed
before covered recovery records can be retired.
Rollback and finalization release transaction state and covered WAL history, but
leave captured collection storage in place. Reclamation runs only after successful
recovery, before the loaded Chain is exposed to requests, and retains the captures
referenced by the persisted committed block. Callers coordinate maintenance by
closing and reopening the owner; there is no live cleanup API or worker.
Unused captures occupy storage until reopening. Delegated storage admission still
applies; there is no automatic capacity expansion or fallback.
A duplicate cutoff is a no-op. Finalization does not encode or copy the full
subject. The subject's native storage is the canonical materialization.

Creation receives an unpublished persistent subject, empty delegated WAL storage,
and empty delegated collection-value storage. It synchronizes that initial subject
before publishing the empty committed block.
Loading receives a strictly loaded subject and requires the committed file,
a valid SHA-256 file checksum, a null predecessor, unique transaction entries,
and nonempty records for every transaction strictly beyond the finalized frontier.
Transactions retain their original IDs. Collection values use native scoped GET
references whose ID is the hexadecimal SHA-256 identity, suffix is the collection
class path, and key is the native schema.
Identity combines the hash of class and semantic schema with the hash of ordered
contents; native collection copying, loading, and hashing belong to the collection
owner. Chain validates records and referenced collections before
replaying ordinary PUT/DELETE handlers. The caller supplies each original
transaction capability; an ID mismatch fails. Unreferenced staging files can be
reclaimed only after successful recovery. Missing subject roots or required files are
errors at the collection/storage boundary, never instructions to create state.

Snapshot import and replacement are not supported.
Native views retain v1 behavior: mutations through escaped raw collection
handles are outside the WAL's coverage.

The caller owns persistent subject storage, durable WAL storage, and transaction
allocation contexts. Chain delegates cache, file-handle, and disk-reserve admission
to storage. It adds no transaction-count, mutation-count, or retained-byte quota.
History uses a shared `ChainBlock`: a 32-byte predecessor field and an ordered
map from original transaction IDs to ordered mutation records. Its format-neutral
codec encodes `(previous_hash, mutations)`. DELETE encodes `(path, key)` and PUT
encodes `(path, key, stored_value)`; a scalar null remains an ordinary PUT value.
Duplicate transaction IDs and malformed records are rejected.

SyncChain keeps one `committed.chain_block` file encoding
`(finalized_frontier, ChainBlock)`. The frontier belongs to SyncChain, outside
the shared block representation. SyncChain requires the null predecessor
(32 zero bytes); the shared codec also supports nonzero predecessors.
BlockChain predecessor validation and retention policy remain unimplemented.
There is no publication index, per-transaction file, or format-version field.
Atomic durable replacement publishes the entire retained history and frontier
together. Collection arguments occupy their separate native storage root.
The cache retains semantic PUT/DELETE records, not encoded chunks. File codecs
own JSON encoding and SHA-256 framing. These file checksums protect
encoded storage; they are not semantic BlockChain hashes. Collection identities
include semantic schema and contents, excluding cache configuration and physical
layout. Semantic BlockChain hashing remains unimplemented;
encoded JSON and file checksums must not substitute for it.
Measurement and persistence borrow semantic records through their native codecs;
publication still prepares an independent replacement snapshot.
The storage boundary measures
encoded file size for cache admission before persistence; mutation routing performs
no measurement encoding. Collection values occupy native collection storage and
are synchronized without re-encoding or copying them at commit.
Each mutating commit rewrites all retained records. The retained block must fit
the delegated cache's per-file admission bound, including replacement headroom.
Finalization removes covered records; there is no automatic rotation, alternate
layout, or background flushing. Fewer files and publication operations do not
imply less cumulative write traffic.

Collection handlers in different transactions may execute concurrently. Earlier
pending records do not exclude later requests; collections decide which mutations
conflict. Within one transaction, the queue admits the next request only after
the prior one finishes, preserving the same request order in execution and replay.
Commit/rollback reject unfinished work in their transaction; finalization rejects
unfinished work through its cutoff. Later executing requests do not exclude an
earlier decision. These checks cover intercepted mutation tasks, not observations
or escaped native handles and streams. The caller must prevent new work and finish
or cancel affected operations and release their streams before a lifecycle
decision, as required by the collection contract. Native collection lifecycle
errors still propagate.

Capture and lifecycle preparation remain exclusive. Capture exclusion prevents
reuse of an incompletely copied argument; live lifecycle methods do not reclaim
captures. This exclusion ends before the selected
collection handler executes. It is WAL storage coordination, not collection
conflict detection. Publication and interruption protection remain unchanged.
A further v1 integration port must address native collection copying and
transactional directory ownership together. Removing capture exclusion requires
demonstrated incomplete-copy isolation, not an assumed filesystem capability.
Reads retain collection ordering. Calls inherit cancellation
and the caller's absolute deadline. There is no background flusher or group-commit
queue. Lifecycle calls use the shared fallible `Transact` trait; finalization
needs only the caller's cutoff ID. The local persisted cutoff is not another
transaction allocator or host finalized-frontier authority.

### Recovery boundary

The request-log model does not make multi-file collection finalization atomic.
After partial finalization, replay may fail because native storage is inconsistent
or because an already-applied strict insert conflicts. Such failures retain the
WAL and return an error. Recovery never skips a conflict, converts insert to
upsert, or claims success from inferred state. Arbitrary partial-finalization
recovery remains unimplemented and must not be included in Issue #2's completed
acceptance claims.

The WAL stores ordinary Scalars directly, reserving top-level scalar references
for captured collections. Other reference forms are rejected. The value store
strictly loads and verifies existing identities before reuse; multiple requests
may share one capture. Load-time reclamation preserves every published capture. Resolution
never evaluates references through routing or the network.
The native copy must reproduce the source's ordered-content hash. Reordered views
whose order is lost during copying fail closed; this format does not preserve view
ordering or implement order canonicalization.

The format uses shared semantic blocks and structural mutation records, records
publication through one atomic committed-file replacement, and delegates mutation-route encoding and validation
to `pathlink::PathBuf`. Earlier unpublished formats are unsupported.
Development fixtures must be recreated; there is no compatibility reader or migration.

## Failure and resource semantics

- Required recovery data is lossless. Storage errors propagate to the caller;
  there is no retry loop, weaker Chain fallback, or alternate storage root.
- The delegated queue bounds pending outputs per transaction before work begins.
  Saturation rejects work without selecting a transaction outcome; there is no
  default capacity or unbounded fallback. The caller separately bounds live
  transactions, payload sizes, and timely finalization. Queue counts and cache
  admission do not constitute end-to-end retained-memory admission.
- Best-effort behavior is allowed only when a concrete variant declares it and
  exposes bounded loss observability.
- Replay preserves request semantics and order. Unknown, conflicting, or ambiguous
  records fail loading instead of being skipped.
- Reconciliation is externally coordinated and never hidden behind host startup,
  HTTP replication, `txfs`, collection storage, or a process-global registry.
- The three-second synchronous budget still applies; longer workflows use the
  canonical persistent `While` queue.

## Promotion criteria

A Chain variant is not supported for public hosting until it has tests for deterministic ordering,
duplicate delivery, gaps, conflicts, restart replay, corrupt history,
backpressure, cancellation, materialization, and resynchronization through the
same Service and Cluster path.

Crate-level SyncChain support covers BTree and Table recovery and failure boundaries.
Snapshot import and replacement, executable Services, host readiness,
cross-host synchronization, and persistent Tensor subjects remain outside that
support claim.
