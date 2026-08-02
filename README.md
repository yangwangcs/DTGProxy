# DTGProxy

DTGProxy is a distributed temporal property-graph middleware with a clean-break layered
architecture. The workspace contains a minimal kernel, a language layer that stops at normalized
logical IR, an execution layer, a storage layer, and four thin process assemblies.

## Architecture

```text
dtg-kernel
  ├─ dtg-language-ir <- dtg-language
  ├─ dtg-storage <- native and remote providers
  └─ execution capabilities <- dtg-execution
                               ├─ dtg-gateway
                               ├─ dtg-data
                               ├─ dtg-meta
                               └─ dtg-controller
```

- Language owns parsing, semantic validation, and deterministic normalized logical IR only.
- Execution owns planning, bounded query execution, temporal snapshot-isolation transactions,
  Shard/Raft, consistent reads, logical replica snapshots, Snapshot CSR algorithms, durable
  asynchronous analytics, the control plane, and generational online migration.
- Storage exposes one logical replica contract with capability-controlled pushdown. Fjall,
  PostgreSQL, and Kuzu are native in-process providers. Third-party providers use the versioned
  Remote protocol.
- Gateway, Data, Meta, and Controller are thin process composition roots, not service copies of the
  three logical layers.

## Invariants

- One placement epoch has exactly one active backend generation.
- Every replica in one Raft group and generation uses the same backend class.
- Every replica owns a distinct physical namespace.
- One Data process may host independent Shards backed by different provider classes.
- Follower reads require an authenticated proof and a real quorum ReadIndex.
- Only the closed built-in algorithm catalog is executable; arbitrary user code is rejected.

## Build and test

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
bash scripts/check-layered-architecture.sh
bash scripts/check-clean-break-removal.sh
```

Run the repeatable local certification:

```bash
scripts/certify-clean-break.sh --local
```

It writes a versioned JSON evidence manifest under `target/clean-break-certification/`.

## Process fixtures

The deployment descriptors in `config/examples/clean-break-cluster/` define one Meta process, one
Controller process, one Gateway process, and two Data processes. See [deployment](docs/deployment.md)
for process configuration and [storage](docs/storage.md) for provider setup.

For a disposable local cluster, `scripts/local-cluster.sh start --managed-postgres` starts a
loopback-only PostgreSQL instance and the four DTG process roles. Fjall and Kuzu are embedded in
the three Data processes; PostgreSQL is the only managed external service. Use
`scripts/local-cluster.sh stop` to stop only processes recorded by that launcher.

## Documentation

- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Storage providers](docs/storage.md)
- [Online migration](docs/migration.md)

Historical design and execution records remain under `docs/superpowers/` and `docs/audit/`; they are
not current runtime instructions.
