# Chain contract

This document defines the ownership boundary for TinyChain durable history and
the SyncChain write-ahead log. Public Service hosting is a separate integration.

## Ownership

Filesystem writeback and durability are distinct: ordinary `freqfs::sync()` and
eviction are buffered. Chain synchronizes captures before atomically publishing
requests with `replace_all()`. Collection commit establishes in-memory visibility;
Chain retains durable requests until canonical materialization completes.
Callers supply durable storage roots. Unreferenced captures are reclaimed only
after successful coordinated recovery.

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

SyncChain is the sole durable transaction authority until finalization. Collection
has no acceptance manifest or independent durable transaction ledger. Transaction
workspaces retain committed deltas until successful recursive finalization.

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

Commit synchronizes each distinct capture, atomically publishes the ordered
requests, delegates Collection's in-memory commit, then acknowledges. Requests
remain in the WAL. Empty and duplicate commits and rollback perform no WAL writes;
their live receipts are not a durable historical decision ledger. Publication
followed by failed or cancelled subject commit requires reopening, never an inferred
rollback. Armed lifecycle interruption makes the live owner unusable.

Finalization derives covered mutations from retained requests. When any are covered,
it durably publishes materialization intent before native writes, delegates in-place
Collection finalization, durably synchronizes canonical storage, then atomically
advances the cutoff, clears intent, and removes covered requests. Only then does it
finalize queue receipts. With no covered mutations it delegates logical cutoff
cleanup and publishes the frontier without intent or canonical synchronization.
Duplicate cutoffs perform no durable writes. Finalization is durability-critical
but does not determine commitment. It does not serialize the whole subject.

Rollback and finalization leave unused captures until successful reopening. Storage
admission continues to apply; there is no automatic expansion or fallback.

Creation receives an unpublished persistent subject, empty delegated WAL storage,
and empty delegated collection-value storage. It synchronizes that initial subject
before publishing the empty committed block.
Loading receives an asynchronous subject loader and requires the committed file,
a valid SHA-256 file checksum, a null predecessor, unique transaction entries,
and nonempty records for every transaction strictly beyond the finalized frontier.
Transactions retain their original IDs. Collection values use native scoped GET
references whose ID is the hexadecimal SHA-256 identity, suffix is the collection
class path, and key is the native schema.
Identity combines the hash of class and semantic schema with the hash of ordered
contents; native collection copying, loading, and hashing belong to the collection
owner. Chain validates all records and captures before requesting replay capabilities.
Every retained ID replays ordinary PUT/DELETE or native restoration and commits
Collection visibility. Capabilities must preserve original IDs; an ID mismatch
fails. Recovery retains requests and reconstructs only mutating commit receipts
and the finalized cutoff. Unreferenced captured files can be
reclaimed only after successful recovery. Missing subject roots or required files are
errors at the collection/storage boundary, never instructions to create state.

`restore_from(&txn, &snapshot)` records a captured collection reference and
delegates native transactional restoration. Kind and semantic schema must match.
It adds no public restoration route or network fetching.
Native views retain v1 behavior: mutations through escaped raw collection
handles are outside the WAL's coverage.

The caller owns persistent subject storage, durable WAL storage, and transaction
allocation contexts. Chain delegates cache, file-handle, and disk-reserve admission
to storage. It adds no transaction-count, mutation-count, or retained-byte quota.
History uses a shared `ChainBlock`: a 32-byte predecessor field and an ordered
map from original transaction IDs to ordered mutation records. Its format-neutral
codec encodes `(previous_hash, mutations)`. DELETE encodes `(path, key)` and PUT
encodes `(path, key, stored_value)`; a scalar null remains an ordinary PUT value.
Native restoration encodes `(stored_collection_reference,)`.
Duplicate transaction IDs and malformed records are rejected.

SyncChain keeps one `committed.chain_block` file encoding
`(finalized_cutoff, materializing_cutoff, ChainBlock)`. The optional materializing
cutoff records an unfinished canonical update. Chain rejects retained requests
at or below the finalized cutoff. SyncChain requires the null predecessor
(32 zero bytes); the shared codec also supports nonzero predecessors.
BlockChain predecessor validation and retention policy remain unimplemented.
There is no publication index, per-transaction file, or format-version field.
Atomic durable replacement publishes the entire retained request history.
Collection arguments occupy their separate native storage root.
The cache retains semantic mutation records, not encoded chunks. File codecs
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
Requests are retained until finalization; there is no automatic
rotation, alternate layout, or background flushing. Fewer files and publication operations do not
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

`SyncChain::hash` validates the caller's transaction as an observation, then
delegates to the subject's native hash of class, semantic schema, and visible
contents. It does not register pending work or hash the WAL representation.

### Recovery boundary

Loading validates the WAL before invoking the subject loader. Any materialization
intent returns a recovery-required error without native loading, capabilities,
replay, or reclamation, even if canonical storage appears structurally valid.
With a clean record, strict native loading validates tree structure and Table index
consistency; all retained requests and captures are validated before capabilities
are requested. Collection and queue cutoffs are initialized from the WAL. Every
retained transaction is replayed in original order into fresh caller-delegated
workspaces with its original ID, then committed in memory. Recovery retains the WAL.
Only successful recovery permits orphaned capture cleanup.

Interrupted in-place materialization cannot be repaired by ordinary request replay
or native restoration. The resource remains unavailable. Future Service/Cluster
integration must select authoritative state using authenticated resource membership
and appropriate history/checkpoint evidence, construct a replacement in fresh
storage, and retain old evidence until replacement is durable. A reachable peer
alone is not authority. Libraries return errors; the host owns readiness withdrawal
or shutdown. Chain is not a cross-service transaction coordinator.

Restart fixtures are not physical power-loss validation. Durability assumes the
filesystem honors atomic replacement and file/directory synchronization.

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
Executable Services, automatic authoritative resynchronization, host readiness,
cross-host synchronization, and persistent Tensor subjects remain outside that
support claim.
