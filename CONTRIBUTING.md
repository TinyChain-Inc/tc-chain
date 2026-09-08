# Contributing to tc-chain

Follow the workspace [contributor guide](../CONTRIBUTING.md), this crate's
[invariants](AGENTS.md), and the canonical [Chain contract](CHAIN_CONTRACT.md).

Before opening a pull request, run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

New behavior must remain transport-neutral and include focused ordering, replay,
corruption, backpressure, and restart tests appropriate to the implemented
capability. Update the Chain contract only for a stable ownership or semantic
change; keep proposals in the roadmap.
