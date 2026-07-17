# DTGProxy

DTGProxy is a distributed bitemporal property-graph middleware written in Rust. It owns valid-time and transaction-time semantics above pluggable ordinary KV and graph database backends.

The repository is being delivered in independently verifiable phases. Phase 0 contains:

- `temporal-types`: bitemporal primitives and canonical values;
- `temporal-model`: executable in-memory semantic oracle;
- `storage-api`: backend-neutral committed-mutation contract;
- `adapter-memory`: atomic and idempotent reference adapter;
- `dtgproxy`: executable product entry point.

The approved architecture and remaining distributed phases are specified in [the detailed design](docs/superpowers/specs/2026-07-17-dtgproxy-design.md).

## Development

```bash
cargo test --workspace
cargo run -p dtgproxy -- --version
```

