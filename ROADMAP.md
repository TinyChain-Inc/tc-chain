# tc-chain Roadmap

This roadmap covers the ledger services owned by `tc-chain`:

1. **MetricsChain** – a metering ledger for installs, invocations, and billing/analytics counters.
2. **BlockChain** – a governance/attestation ledger used by the control plane to anchor publisher keys, rotations, and rollout evidence (mirrors the `tinychain/host/chain` reference implementation).
3. **LogChain** – a diagnostics ledger/topic feed for operational logs, incident replay, and cross-host troubleshooting.

All chain services must share infrastructure (consensus, storage, CI) and expose HTTP+IR APIs so runtimes, publishers, and validators interact with them uniformly.

`tc-chain` remains the ledger substrate for metering/governance. Pricing policy,
quote/settlement orchestration, and invoice/reconciliation workflows are owned
by the standard billing service (`/service/std/billing`) in the control-plane
suite, which consumes MetricsChain/BlockChain evidence rather than replacing it.

LogChain remains diagnostics-focused: it is not a billing ledger and not
commit-proof transaction evidence.

Rollout business logic is owned by the control-plane rollout service
(`/service/std/rollout`). `tc-chain` owns ledger evidence only: rollout stage
events, policy references, and cross-ledger links required for audit and
rollback provenance.

---

## LogChain deliverables

### MVP scope

1. **Event schema + taxonomy contract.**
   - Define a versioned log envelope with deterministic correlation IDs and custody metadata.
   - Require taxonomy labels for structured log fields; unknown user-supplied fields default to PII classification and must be sanitized before append.
   - Support extensible, namespaced application-specific labels in addition to core platform classes.
2. **Ingress/reliability model.**
   - Preserve 3-second route budgets via queue-backed async ingestion.
   - Define explicit reliability behavior under pressure (best-effort diagnostics with observable backpressure/drop signals).
3. **Read APIs.**
   - Topic discovery, capability-scoped streaming subscription, and signed batch export.
   - Cursor semantics suitable for cross-host incident replay.
4. **PII safety guarantees.**
   - Enforce pre-ingest sanitization policy so raw PII never lands in LogChain payload storage.
   - Add conformance tests for taxonomy validation and PII leak prevention.

---

## MetricsChain deliverables

### MVP scope

1. **Usage event schema (v0).**
   - Reuse the existing guidelines (manifest linkage, tenant IDs, resource counters, deterministic time windows, runtime attestation, short-lived runtime keys).
   - Encode events as `destream`-serializable structs so both Rust hosts and PyO3/WASM tooling can emit them without bespoke serializers.
   - Classify events as either `engagement` (best-effort telemetry) or `billing` (lossless metering) so reliability guarantees are explicit in the schema.
   - For `billing` events, require canonical minimum fields: `tenant_id`, `service_uri`, `endpoint_uri`, `meter_type`, `quantity`, `unit`, `rate_id`, `pricing_version`, `authorization_link`, `dedup_key`, `trace_id`, and deterministic window metadata.
   - Support endpoint-scoped and policy-versioned rates so validators can reconcile per-endpoint billing deterministically.
   - Support optional FLOP meter units for ML execution with estimator provenance (`estimator_version`, `shape_fingerprint`) to allow recomputation during audits.
2. **Runtime client library.**
   - Provide a Rust crate (`tc-metrics-client`) that runtimes link to. It batches counters per manifest/window, signs them with runtime-issued keys, and handles retries.
   - Expose bindings for PyO3 so the Python client can emit the same counters during integration tests.
3. **Validator ingest + batching.**
   - Implement the validator service that receives runtime submissions, validates signatures/policies, batches events, and publishes batch proofs (hash/Merkle root) with `kid` metadata.
   - Batches must reference predecessor hashes for replay protection.
   - Billable events must fail closed: if validator ingest cannot durably accept them, return a structured error and apply backpressure instead of dropping data.
   - Billable events must include a resolvable BlockChain authorization link at ingest/finalization time.
4. **Read APIs.**
   - `/metrics/events` – paginated, filterable view of raw events (manifest, tenant, window).
   - `/metrics/batches` – validator attestations with proofs and replay pointers.
   - Expose filters by reliability class (`engagement` vs `billing`) and an audit view that joins billing events to BlockChain authorization links.
5. **Failure handling.**
   - Invalid events return structured errors; duplicates are ignored.
   - Validators that miss a window resync by fetching prior batches.
   - Engagement events may be dropped under bounded queue pressure, but the system must emit explicit drop/backpressure counters.
   - Billing events may not be dropped; retries must remain idempotent via deterministic dedup keys.
   - Structured backpressure errors should include deterministic retry guidance (`retry_after`, `reason_code`) so runtimes enforce one cross-service policy.

### Phase 2: Control-plane integration

1. **Counter alignment.** Ensure the minimal counters emitted during Phase 2 of the control-plane bootstrap (`install_attempts`, `install_success`, `token_validation_fail`, `invocation_success`, `invocation_denied`) map directly onto MetricsChain schemas.
2. **Custody tags.** Attach custody/host metadata so audits can tie usage to attested runtimes (needed by the control-plane rollout service).
3. **CI coverage.** `tc-server` CI must run integration tests that install a WASM library, issue tokens, execute requests, and assert that MetricsChain receives the expected counters.

### Deferred work

- Advanced analytics (custom counters, aggregation windows beyond one minute).
- External auditor export formats (CSV/Parquet snapshots).
- Long-term retention/compaction policies.
- Billing reconciliation reports that prove one-to-one correspondence between accepted billing events and BlockChain authorization links.

---

## BlockChain deliverables

BlockChain extends the reference `tinychain/host/chain` behavior to anchor control-plane governance events: publisher registry updates, key rotations (scheduled + lost-key), rollout attestations, and validator proofs. TinyChain is designed as a hierarchy of chains (a Merkle forest), not a single global blockchain; this shard is one branch dedicated to governance. Cross-links (hashes, custody tags) keep shards coherent while letting the network scale to arbitrarily many chains.

### MVP scope

1. **Ledger contract.**
   - Mirror the v1 `Block`/`Chain` types from the `host/chain` crate (same consensus and serialization).
   - Treat the first v2 implementation as a parity-first v1 port with minimal implementation churn to reduce performance/reliability regression risk.
   - Defer non-essential refactors/optimizations until parity gates pass for append, replay, and finalize behavior.
   - Define `BlockPayload` variants for:
     - `PublisherRegistered`
     - `KeyRotated` (scheduled)
     - `KeyYanked` / `KeyLost`
     - `RolloutEvent` (start, soak, promote, rollback)
     - `MetricsLink` (hash pointer into MetricsChain batches for cross-ledger provenance)
       - `OrganizationCreated` / `OrganizationAdminBootstrapped`
       - `UserProvisioned` / `CredentialChanged`
       - `PasswordResetRequested` / `PasswordResetApplied`
       - `GroupMembershipChanged`
       - `RoleBound` / `RoleRevoked`
       - `PublishScopeGranted` / `PublishScopeRevoked`
       - `ChildOrgCreated` / `ChildOrgDelegationChanged`
       - `MfaPolicyChanged` / `MfaEnrollmentChanged`
       - `IdpLinkChanged`
       - `SessionIssued` / `SessionRevoked` / `SessionKeyRotated`
2. **Registry API.**
   - `/blockchain/publishers` – current view of publisher metadata (id, pubkey, capability scopes, status, key history).
   - `/blockchain/events` – paginated stream of governance events with proofs.
3. **Rotation workflow.**
   - Scheduled rotation: publish `rotation_plan` blocks describing activation/expiry windows. Runtimes accept both keys during the overlap.
   - Emergency/lost-key rotation: publish `KeyLost` blocks signed by a validator quorum; runtimes revoke the old key immediately.
4. **Control-plane hooks.**
   - The control-plane server writes every registry change and rollout milestone into BlockChain.
   - `/host/time` + `/service/std/rollout` return the corresponding BlockChain event IDs so callers can cross-check.
   - ORM/graph upgrade rollouts record stage transitions and rollback reasons as
     `RolloutEvent` evidence; chain services do not execute rollout decisions.
    - `RolloutEvent` evidence should include policy snapshot references
       (effective defaults/overrides), gate result summaries, and correlation keys
       to MetricsChain/LogChain records used by user-visible rollout monitoring.

### Integration requirements

1. **MetricsChain cross-reference.** BlockChain events that refer to usage data (e.g., rollout analysis) must embed the MetricsChain batch ID so auditors can trace consumption.
2. **Shared rollout/mesh correlation envelope.** When rollout and mesh participate in the same upgrade window, emitted evidence should share a correlation envelope (rollout id, stage id, policy snapshot ref, reason code taxonomy, chain refs) so auditors can reconstruct one coherent timeline across BlockChain/MetricsChain/LogChain and cluster-local SyncChain state.
3. **Mesh policy and gateway event correlation.** Mesh policy decisions (route selection, deny decisions, mTLS/authz policy outcomes) and ingress/egress gateway events should emit compatible correlation references so auditors can align data-plane behavior with rollout stage evidence and gate evaluations.
4. **Tensor policy and migration correlation.** Tensor datastore policy decisions (taxonomy admit/reject, sanitization outcomes) and rollout migration health for Tensor schema/index changes should emit compatible correlation references so auditors can reconstruct model/query outcomes alongside rollout stage evidence.
5. **CI tests.** Add end-to-end tests that:
   - Register a publisher via the bootstrap TOML file, migrate it into BlockChain, and query it via the API.
   - Perform both scheduled and lost-key rotations, ensuring runtimes reject old signatures immediately.
   - Trigger a rollout (dual-write → soak → promote → rollback) and confirm BlockChain records each milestone.
   - Bootstrap an organization + initial org-admin and verify corresponding identity-governance events.
   - Change local credentials and verify stale-session/token rejection is auditable.
   - Run org-admin lost-password reset and verify forced rotation + session/token revocation events.
   - Revoke a role/membership and verify authorization failure plus ordered governance events.
   - Grant namespace-scoped publish rights and verify publish succeeds only for authorized `/lib/...` and `/service/...` prefixes.
   - Create child org namespace delegation and verify child-authorized publishers are denied outside child scope.
   - Enforce MFA-required operation policy and verify denial events when assurance is missing.
   - Execute CLI-first login/session issuance and verify session issue/revoke events are auditable.
6. **Secret management.** The validator keys used by BlockChain live alongside the control-plane secrets (Kubernetes sealed secrets / TPM-backed stores). Document the operational procedure in `control-plane/README.md` and ensure CI never logs key material.

### Formal out-of-order consistency model (high-traffic distributed commits)

1. **Identity + ordering key.** Define canonical commit identity using
   `(chain_id, predecessor_hash, txn_id, participant_set_hash)`. Duplicate
   evidence with the same identity is idempotent.
2. **Pending admission on gaps.** If a commit's predecessor is missing, store it
   durably in a pending index but keep it out of the visible committed prefix.
3. **Visibility rule.** A commit becomes externally visible only when it lies on
   the contiguous canonical prefix and carries the required attestations.
4. **Deterministic reconciliation.** When gaps close, replay pending commits in
   predecessor order and promote deterministically. Competing commits for the
   same predecessor follow the chain's canonical conflict rule; non-canonical
   branches remain non-visible.
5. **Regression gates.** Add ordering-permutation, duplicate-delivery, partition
   heal, and restart-replay tests to prove monotonic frontier advancement,
   deterministic replay, and exactly-once logical commit per canonical identity.

### Future work

- Multi-shard BlockChain deployment (per region / tenant).
- ZK/succinct proofs for third-party auditors.
- Replay tooling that reconstructs the publisher registry from genesis.

---

## Open actions

1. **Author `authorized_publishers.toml` schema & validator tool** (Phase 1 dependency).
2. **Stand up `tc-chain` CI** that runs both MetricsChain and BlockChain integration tests in the same Kubernetes pipeline used by `tc-server`.
3. **Document secret handling** for validator and publisher keys (tie into the control-plane roadmap and CI instructions).
4. **Cross-link** this roadmap from `ROADMAP.md` and `control-plane/README.md` so contributors understand the dependency tree.
