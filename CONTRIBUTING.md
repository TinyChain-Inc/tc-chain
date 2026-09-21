# Contributing to tc-chain

Read this repository's [invariants](AGENTS.md) and canonical
[Chain contract](CHAIN_CONTRACT.md). The parent workspace
[contributor guide](https://github.com/TinyChain-Inc/tcv2/blob/main/CONTRIBUTING.md)
is non-normative integration context for a superproject checkout.

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
