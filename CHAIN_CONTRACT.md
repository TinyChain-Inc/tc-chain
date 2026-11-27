# Chain contract

This document defines the ownership boundary for TinyChain durable history and
the SyncChain write-ahead log. Public Service hosting is owned by the caller.

## Ownership

Filesystem writeback and durability are distinct: ordinary `freqfs::sync()` and
eviction are buffered. Chain synchronizes captures before atomically publishing
each transaction file with a file/directory durability barrier, then delegates
Collection's independently durable commit. Chain retains requests until native
finalization and its own frontier publication complete.
Callers supply durable storage roots. Rollback and finalization reclaim their
transaction-owned captures; successful recovery removes orphaned captures.

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

Every application mutation of a persistent mutable collection must pass through
its owning Chain. Returned views grant read access, not mutation authority.
Chain records accepted mutations and owns their durable recovery; collections
apply local transactional changes. Full reads, slices, and transformed views
cannot expose or regain the mutable subject. Selection-write conveniences send
PUT/DELETE to the original owner with explicit bounds, rather than mutating a
returned alias. POST composes those owner-bound operations.

Creation transfers exclusive ownership of an unpublished subject to Chain.
Trusted native callers must relinquish mutable aliases and must not independently
reopen its storage. This contract adds no runtime alias-revocation mechanism.
Fresh request-local collections and explicit independent copies remain writable;
they do not grant write access to the source.

Collection concurrency and WAL coordination are separate concerns. Chain records
requests and coordinates their complete capture with commit, durable publication,
and reclamation. It does not schedule collection mutations or determine which
keys, rows, ranges, or transactions conflict. Those decisions belong to the
transactional collection and its locking primitives. Ordering records for replay
does not require globally serializing execution of collection handlers.

Collection owns immutable native versions, acceptance receipts, and atomic cutoff
publication. These describe local storage, not a second request WAL or transaction
coordinator. Chain owns ordered request history and replay across the WAL-to-native
handoff; it does not synchronize collection internals.

## Common behavior

An ordinary mutation produces pending transactional work. A successful Chain
commit acknowledges both durable recovery evidence and completed durable
publication and visibility in its collection. Canonical materialization may follow later; it is
not required for commit acknowledgement when retained history can recover the
committed effects.

Every Chain is an ordinary recursively routed State resource. It preserves the
original protocol `TxnId`, accepts ordinary PUT/DELETE requests, orders them
deterministically, and materializes state by delegating to its collection.
SyncChain recovers by replaying its retained request WAL against its persistent subject. A
retained-history variant may instead make complete history authoritative.

Recovery validates the history required by its variant before applying each mutation.
Missing referenced data, conflicting, corrupt, or unverifiable history fails closed. Chain policy selects
canonical history and coordinates replacement; collections and hosts do not
guess, vote, truncate, or locally repair it.

Chain work uses the ordinary `Handler<State>`, `Route<State>`, and `Transact`
contracts. It does not introduce another transaction identity, transaction
manager, adapter endpoint family, storage transaction trait, or client-managed
lifecycle.

## Variants

- `SyncChain` supplies the minimal ordered history and replay required to
  synchronize a mutable BTree, Table, or Tensor subject. Its WAL is temporary, and its
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

The caller supplies a positive per-transaction mutation capacity. Each Chain
constructs its own bounded queue.
The queue owns pending records, admission, live transaction decisions, and WAL
admission/lifecycle exclusion; immutable transaction files own durable history.
Queue decision receipts are not a second durable history: publication alone does
not prove completion of subject commit. Historical replay initializes receipts
without consuming pending-task capacity.

Observations check owner status, the cutoff, and existing decisions without
registering pending work. A previously unseen transaction may commit or roll back:
the queue then retains its decision receipt until finalization. Read-only decisions
delegate to the subject without publishing mutation records.

Every collection argument delegates to `CopyFrom::copy_from` directly into
`values/<original-txn-id>/<capture-id>`. The storage owner supplies a unique
capture ID; it has no ordering meaning. Materialization creates independent
native storage and preserves logical ordering. The local capture digest includes
collection class, the native reopening descriptor, and contents. Replica state
comparison uses the Collection's logical digest. Capture
returns the retained native value together with its stored description; the
selected handler applies that value, including exact floating-point results.
There is no cross-transaction capture sharing, second copy, or live reload.
Scalar arguments remain native scalar data, including ordinary scalar references;
references in scalar data are never interpreted as capture descriptions.

Admission reserves queue capacity and marks the transaction `Running` before any
capture allocation. Its short lifecycle exclusion ends before source evaluation,
so capture may await earlier collection decisions without blocking them. Within
one transaction, another unfinished task is rejected. Recording occurs exactly once before the
selected mutation executes. Completion restores `Active`; unfinished drop marks
`Failed`. Chain adds no mirrored execution status or detached work.

Commit synchronizes the transaction's captures, durably publishes its immutable
`<full-txn-id>.chain_block` file, delegates Collection's durable commit, then
acknowledges. The file retains the original ID and ordered records. It never
rewrites another transaction's history. Existing publication conflicts fail rather
than overwrite. Read-only and duplicate commits publish no files. Publication
followed by failed or cancelled subject commit requires reopening, never inferred
rollback. Armed lifecycle interruption makes every live clone unusable.

Rollback delegates Collection rollback, removes that transaction's captures,
durably synchronizes removal, and then completes the queue decision. Finalization
checks covered tasks and records, delegates durable Collection finalization,
publishes the Chain frontier, and deletes covered transaction files and capture
directories. Only successful cleanup completes queue finalization. A restart
between native and Chain frontier publication is safe: native recovery recognizes
the accepted IDs or cutoff. Duplicate cutoffs perform no durable writes. Later
transactions own independent captures and survive earlier cutoff cleanup.

Creation requires an already constructed unpublished persistent subject, empty
delegated WAL and capture roots, and a fresh queue. It publishes
`committed.chain_block`; Collection construction owns initial native durability.
Recovery validates each transaction file before invoking Collection's recovery
operation. Only missing native decisions request an original-ID replay capability
and verify/load their captures before mutation. Filenames must be canonical full
transaction IDs and agree with their payloads; records are nonempty and captures
resolve only within their original transaction. Replay orders parsed transaction
IDs, never filename strings.

Files at or below a durably published frontier are obsolete cleanup residue;
they are removed after successful recovery without decoding or replay. Capture
directories without a retained transaction record are also removed then. A missing
capture required for replay or an observed corrupt, conflicting, or unverifiable file fails recovery. Required native
storage is strictly loaded, never recreated. Recovery retains accepted history
and reconstructs mutating decision receipts without pending-task capacity.

`restore_from(&txn, &snapshot)` records a captured collection reference and
delegates native transactional restoration. Kind and semantic schema must match.
It adds no public restoration route or network fetching.
Returned native views reject mutations and cannot drive the source's lifecycle.
Replay and canonical finalization remain internal applications of accepted
history; they do not create another application mutation path.

The caller owns persistent subject storage, durable WAL storage, and transaction
allocation contexts. Chain delegates cache, file-handle, and disk-reserve admission
to storage. It adds no transaction-count, mutation-count, or retained-byte quota.
The format-neutral `ChainFile` codec distinguishes two data alternatives:
`("frontier", finalized_cutoff)` in
`committed.chain_block`, and `("transaction", original_id, ordered_records)` in
`<full-txn-id>.chain_block`.
PUT encodes `("put", path, key, stored_value)`, DELETE encodes
`("delete", path, key)`, and restoration encodes `("restore", capture)`.
Stored values are `("scalar", scalar)` or `("collection", capture)`; a capture
contains `(capture_id, collection_class_path, native_schema, capture_digest)`.
Null is ordinary scalar data. Native schema conversion belongs to Collection;
mutation paths use `pathlink::PathBuf`. Unknown tags, malformed fields, and
incorrect arity fail decoding. SyncChain has no speculative predecessor field;
BlockChain semantics remain unimplemented.

The cache retains semantic records, not encoded chunks. File codecs select JSON
and SHA-256 checksum framing at the storage boundary. Checksums protect encoded
files; they are not semantic collection or BlockChain hashes. Each transaction
file must fit the delegated per-file admission bound. Total live files are bounded
by caller-owned transaction admission and timely expiry/finalization. An ordered
recovery index contains file handles, never a second retained history payload.
Cutoff finalization prepares only covered transaction identities, without sorting
or retaining handles for later history. Recovery and finalization share native
entry validation. Capture cleanup walks the delegated directory without a
second retained reference index.
Persistence borrows records. `GetSize` accounts actual retained container capacity;
before executing each selected mutation, Chain admits the complete pending
transaction's native record payload and vector capacity against that same
per-file bound, including the delegated file composition's inline envelope and
the shared vector container used after publication.
Recording charges actual vector capacity and grows within the admitted bound;
it does not repeatedly compact earlier records. Completed decoding compacts
containers once, preserving native scalar variants and allowing an admitted
batch to reopen under the same native memory budget. Encoded file length is a
separate quantity; application manifest and module byte limits do not apply to
native history files.
Published transaction records are shared immutably between the cache and replay.
Replay releases its file guard before capability acquisition and native work,
retains one shared transaction vector, and clones only the selected record for
its consuming invocation. Cache eviction does not invalidate that snapshot.
Predictable oversize rejects the task before its mutation, preserving ordinary
caller-owned rollback. Cache contention and storage failures remain fallible at
commit; admission does not reserve residency or disk space.
Decoding and construction may allocate before admission. Cache residency is not a
process-wide memory ceiling. Collections stream materialization, hashing, loading,
and native mutation; commit synchronizes existing captures without copying them.

Collection handlers in different transactions may execute concurrently. Earlier
pending records do not exclude later requests; collections decide which mutations
conflict. Within one transaction, the queue admits the next request only after
the prior one finishes, preserving the same request order in execution and replay.
Commit/rollback reject unfinished work in their transaction; finalization rejects
unfinished work through its cutoff. Later executing requests do not exclude an
earlier decision. These checks cover intercepted mutation tasks, not observations
or retained read-only values and streams. Before commit, the caller prevents new
mutations and finishes mutating operations in that transaction. Immutable result
snapshots and their streams may survive commit. Before rollback, the caller
finishes or cancels all affected operations and releases their streams and guards;
before finalization, this applies to transactions through the cutoff. These are
caller-owned preconditions from the collection contract, not additional Chain
read tracking. Native collection lifecycle errors still propagate.

Admission and lifecycle decisions share one queue semaphore. Waiting admission
registers no work and inherits the caller's deadline. Capture and selected handler
execution retain their task, not the semaphore. Transaction-owned directories
isolate incomplete captures; failed or cancelled work cannot publish a transaction.
Lifecycle exclusion prevents new admission while a decision or cleanup runs, and
covered unfinished tasks reject the decision. Armed interruption closes the
semaphore and requires reopening. Reads retain collection ordering. There is no
background flusher, group-commit queue, or additional transaction allocator.

`SyncChain::hash` validates the caller's transaction as an observation, then
delegates to the subject's native hash of class, semantic schema, and visible
contents. It does not register pending work or hash the WAL representation.

### Recovery boundary

Opening validates the frontier before invoking the strict native subject loader.
Collection loading reconstructs durable native visibility and cutoff. Recovery
makes one ordered pass over retained transaction files, validating each required
file before delegating `Collection::recover`. A native accepted decision skips
replay and capability acquisition. A missing decision is replayed with its
original-ID capability and durably committed before processing the next record.
Collection and queue cutoffs are initialized from the WAL; a newer native cutoff
is safe when its Chain publication was interrupted. Recovery retains the WAL.
Only successful recovery permits orphaned capture cleanup. A late failure can
leave earlier accepted records durably completed in native storage. The owner
remains unpublished and interrupted; reopening recognizes those decisions and
continues without reapplying them.

Callers which must assemble an execution runtime before replay use `open` followed
by `recover`. The opened owner remains unpublished: no requests, expiry processing,
or readiness announcement may run before recovery succeeds. `load` delegates to
these same operations. Native invocation is generic over the caller's State;
the delegated WAL file composition implements `ChainFileType`. Chain owns the
record codec and checksum framing regardless of that composition.

Interrupted native commit or compaction recovers through immutable payloads and
atomic descriptors. Missing referenced or corrupt storage still fails closed.
Automatic authoritative replacement remains unsupported: future Service/Cluster
integration must select authenticated canonical state and construct a replacement
in fresh storage, retaining evidence until publication is durable. Libraries
return errors; the host owns readiness withdrawal or shutdown. Chain is not a
cross-service transaction coordinator.

Chain and its integrating host exclusively own the delegated storage. Durability
covers interruption of their operations and assumes the filesystem honors atomic
replacement and file/directory synchronization. Checksums and strict loading detect
observed inconsistencies and missing referenced data; they do not establish that
no complete transaction file was externally removed or that storage was not
externally rolled back. There is no independent inventory for detecting those
out-of-process changes. Restart fixtures are not physical power-loss validation.

History resolution strictly loads native captures and verifies their integrity
digests; it never evaluates scalar references through routing or the network.
Capture delegates independent native construction to Collection's `CopyFrom`
implementation. Earlier unpublished
layouts are rejected; development fixtures must be recreated. There is no
compatibility reader or migration.

Native collection ordering refines complete collation ties without changing
range equivalence. Strict capture loading rejects noncanonical native tie order;
Chain delegates ordering and validation to Collection. Correcting previously
incomplete or lifecycle-dependent tied-row results can change their digests.

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
- The caller supplies execution deadlines and budgets. `While` executes within
  its request; Chain does not automatically queue long-running workflows or split
  them into transactions.

## Promotion criteria

A Chain variant is not supported for public hosting until it has tests for deterministic ordering,
duplicate delivery, gaps, conflicts, restart replay, corrupt history,
backpressure, cancellation, materialization, and resynchronization through the
same Service and Cluster path.

Crate-level SyncChain support covers BTree, Table, and Tensor recovery and failure
boundaries. Executable Services, host readiness, and authenticated snapshot transport
belong to the integrating host. This crate exposes generic native State/file
delegation, unpublished opening followed by recovery, and transactional restoration
for those owners. Automatic authoritative replacement of damaged storage remains
unsupported.
