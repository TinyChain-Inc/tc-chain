# tc-chain

`tc-chain` defines the TinyChain control-plane ledger: attestation records,
transaction envelopes, and queue plumbing that every host references when
authorizing installs or replaying history. Keep this crate minimal and
transport-agnostic—other runtimes (HTTP, PyO3, future WebTransport hosts) link to
its types directly, so new behaviors must remain broadly reusable.

## What this crate provides

- Canonical ledger primitives (`Claim`, `TxnRecord`, MetricsChain entries) that
  mirrors the v1 control-plane contract while we migrate into the v2 graph model.
- Helpers for interpreting transaction flows (begin/commit/rollback) and
  encoding attestation metadata without depending on host-specific storage.
- Queue orchestration scaffolding used by `tc-server` to drive the 3-second
  synchronous budget and defer longer work into TinyChain `While` loops.

See `AGENTS.md` for the design guardrails that keep the ledger consistent with
downstream hosts, and `ARCHITECTURE.md` for the big-picture control-plane model.

## Building & testing

```bash
cargo build -p tc-chain
cargo test  -p tc-chain
```

Run the tests any time you touch ledger structures, serialization contracts, or
transaction routing. If your change affects MetricsChain semantics or attestor
records, also update `METRICSCHAIN_GUIDELINES.md` before shipping.

## Development workflow

1. **Design first.** Changes that alter transaction envelopes, claims, or queue
   semantics should be documented in `ROADMAP.md` (or crate-specific notes) so
   adapters and clients can prepare for the updated contract.
2. **Stay IR-friendly.** Ledger structs must remain deserializable without host
   context. Favor primitives already defined in `tc-ir`; when introducing new
   fields, ensure they round-trip through `destream`/`serde` tests.
3. **No bespoke storage helpers.** The crate does not write to disk; hosts are
   responsible for persisting ledger state. Keep `tc-chain` focused on data
   definitions and pure logic, leaving IO to higher layers.
4. **Queue discipline.** Reuse the shared `While`-loop guidance from
   `AGENTS.md`: single shared queue per workflow, state stored in standard
   TinyChain collections, retries handled by the host scheduler.

## Related references

- Workspace `ARCHITECTURE.md` – control-plane, ledger, and queue sections.
- `METRICSCHAIN_GUIDELINES.md` – telemetry schema for chain events.
- `tc-server/README.md` – shows how the host consumes the ledger contracts.
- `client/py` and `client/js` roadmaps – describe how clients rely on these
  primitives for attestation and transaction lifecycles.
