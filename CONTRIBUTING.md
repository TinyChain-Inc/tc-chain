# Contributing to tc-chain

Read this repository's [invariants](AGENTS.md) and canonical
[Chain contract](CHAIN_CONTRACT.md).

Before opening a pull request, run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --lib --release tests::stack_tests::
```

New behavior must remain transport-neutral and include focused ordering, replay,
corruption, backpressure, and restart tests appropriate to the implemented
capability. Update the Chain contract only for a stable ownership or semantic
change; keep proposals in the roadmap.

Capture, native hashing, replay, and restoration run with ordinary harness and
Tokio worker stacks in both debug and release builds. Keep scenario phases small;
do not increase thread stack sizes to make a regression pass.

Cancellation tests use delegated storage and workspace locks to exercise selected
boundaries without instrumentation in SyncChain. Restart and recovery-state fixtures
cover retained WAL behavior and fail-closed conflicts; they do not constitute
exhaustive persistence-stage fault injection or power-loss validation.

Replay releases its WAL read guard before applying native mutations. An immutable
record vector is shared with the cache for this independent replay lifetime; only
the selected invocation clones its consumed record. Measure snapshot acquisition,
per-record copying, and retained payload separately from replay I/O:

```bash
cargo test --release --lib replay_record_clone_benchmark -- --ignored --nocapture
cargo test --release --lib pending_record_retention_benchmark -- --ignored --nocapture
cargo test --release --lib cutoff_preparation_benchmark -- --ignored --nocapture
```

Retained-record fixtures report native payload size for two independent queues;
this is not a host-wide admission guarantee. Cutoff preparation compares the
ordered recovery index with covered-identity preparation, excluding durability
I/O. Keep these measurements outside canonical stack-regression modules.
