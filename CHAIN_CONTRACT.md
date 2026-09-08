# Chain contract

This document defines the common ownership boundary for TinyChain durable
history. The implementations are planned; this contract prevents temporary
recovery or consistency mechanisms from appearing in other crates meanwhile.

## Ownership

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
- A collection owns local data representation, transactional versions, and
  deterministic `Transact` behavior.

No layer may duplicate an adjacent owner's route, transaction lifecycle, log,
or recovery policy.

## Common behavior

Every Chain is an ordinary recursively routed State resource. It preserves the
original protocol `TxnId`, accepts events through bounded admission, orders them
deterministically, and materializes state by delegating to its collection. Its
history is the authoritative replay source; snapshots and indexes are derived
and replaceable.

Recovery replays only a validated contiguous canonical history. Missing,
conflicting, corrupt, or unverifiable history fails closed. Chain policy selects
canonical history and coordinates replacement; collections and hosts do not
guess, vote, truncate, or locally repair it.

Chain work uses the ordinary `Handler<State>`, `Route<State>`, and `Transact`
contracts. It does not introduce another transaction identity, transaction
manager, adapter endpoint family, storage transaction trait, or client-managed
lifecycle.

## Variants

- `SyncChain` supplies the minimal ordered history and replay required to
  synchronize a mutable Service member.
- `BlockChain` adds cryptographic predecessor/evidence validation to the common
  ordered history.
- `MetricsChain` specializes validation, retention, and reliability policy for
  telemetry and accounting events.

Variants may differ only where their implemented policy requires it. They reuse
the same routing, transaction, admission, replay, and materialization boundaries.
Schemas, quorum rules, retention intervals, and deployment policy are documented
only when their implementations exist.

## Failure and resource semantics

- Authoritative history is lossless and bounded. Saturation propagates to the
  caller according to the workspace backpressure contract.
- Best-effort behavior is allowed only when a concrete variant declares it and
  exposes bounded loss observability.
- Replay is deterministic and idempotent. Unknown or ambiguous records stop
  readiness instead of being skipped.
- Reconciliation is externally coordinated and never hidden behind host startup,
  HTTP replication, `txfs`, collection storage, or a process-global registry.
- The three-second synchronous budget still applies; longer workflows use the
  canonical persistent `While` queue.

## Promotion criteria

A Chain variant is not supported until it has tests for deterministic ordering,
duplicate delivery, gaps, conflicts, restart replay, corrupt history,
backpressure, cancellation, materialization, and resynchronization through the
same Service and Cluster path.
