# tc-chain roadmap

This roadmap contains unimplemented work. All variants must satisfy the common
[`Chain contract`](CHAIN_CONTRACT.md).

## SyncChain

- Port the minimal v1 ordered append, visibility, replay, and finalization
  behavior through ordinary State and collection contracts.
- Make validated history authoritative and materialized collection state
  rebuildable.
- Add deterministic duplicate, gap, conflict, restart, corruption, and
  resynchronization tests before integration with executable Services.

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

- Add the Service-owned composition before exposing named persistent collection
  URIs.
- Integrate Chain-coordinated recovery and resynchronization with host readiness.
- Add cross-host acceptance proving all variants preserve the original TxnId and
  use one Cluster/Service/Chain/collection path.
