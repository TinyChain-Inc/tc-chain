# tc-chain roadmap

This roadmap contains unimplemented work. All variants must satisfy the common
[`Chain contract`](CHAIN_CONTRACT.md).

## SyncChain integration

- Integrate caller-owned admission for retained transaction memory before claiming
  end-to-end bounded resource use; storage cache admission alone does not cover it.
- Demonstrate coordinated snapshot synchronization through the Service/Cluster
  path for corrupt or divergent native storage while preserving the original transaction capability. Select authoritative
  replacement using authenticated membership and history/checkpoint evidence;
  resources with corrupt referenced storage remain unavailable until this exists.
- Extend restart and syscall-failure evidence with deployment-filesystem
  power-loss validation of WAL publication, native receipt, and atomic compaction ordering.
- Evaluate bounded group commit against measured workloads without adding a
  second persistence model or weakening durability.

## BlockChain

- Extend the common history with implemented cryptographic predecessor and
  attestation validation.
- Define governance event schemas only alongside their first consumers and
  language-neutral fixtures.
- Demonstrate deterministic canonical selection and replacement without host or
  collection repair heuristics.

## MetricsChain

- Define the minimal implemented telemetry event and reliability policies.
- Reuse Chain ingest, ordering, replay, backpressure, and Service routing.
- Distinguish lossless accounting from explicitly bounded best-effort telemetry
  without adding a separate queue or transport path.

## Integration

- Integrate Chain-coordinated recovery and resynchronization with host readiness.
- Add cross-host acceptance proving all variants preserve the original TxnId and
  use one Cluster/Service/Chain/collection path.
