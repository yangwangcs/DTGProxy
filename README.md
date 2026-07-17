# DTGProxy

DTGProxy is a distributed bitemporal property-graph middleware written in Rust. It owns valid-time and transaction-time semantics above pluggable ordinary KV and graph database backends.

The repository is being delivered in independently verifiable phases. The implemented kernel contains:

- `temporal-types`: bitemporal primitives and canonical values;
- `temporal-model`: executable in-memory semantic oracle;
- `storage-api`: backend-neutral committed-mutation contract;
- `adapter-memory`: atomic and idempotent reference adapter;
- `adapter-rocksdb`: durable RocksDB 0.24.0 adapter with atomic cross-keyspace writes,
  idempotent replay, restart recovery, snapshot reads, and checkpoints;
- `temporal-storage`: deterministic graph key/value codecs, Current/History rewriting, typed
  vertex and edge reads, double adjacency, AS OF, and temporal DIFF;
- `dtgproxy`: executable product entry point.

Phase 0, the Phase 1A durable adapter, and the Phase 1B temporal persistence slice are
implemented and tested. Bounded Anchor+Delta history, replicated shard runtime, distributed
transactions, query execution, external database adapters, and analytics integration remain
active implementation phases described by the design.

The durable layout has eight stable RocksDB Column Families: `meta`, `identity`, `current`,
`adj_out`, `adj_in`, `history`, `temporal_index`, and `txn`. The RocksDB `default` Column
Family is also present because RocksDB requires it, but DTGProxy does not place logical data
there.

The approved architecture and remaining distributed phases are specified in [the detailed design](docs/superpowers/specs/2026-07-17-dtgproxy-design.md).

## Temporal persistence slice

`TemporalStore` accepts typed vertex or edge mutations with a valid-time interval plus a
transaction context. It validates immutable identity and temporal conflicts, rewrites the change
into disjoint valid-time segments, and sends one deterministic `CommittedMutationBatch` to the
selected adapter. That batch atomically contains:

- the graph-scoped identity record;
- the latest Current projection;
- an immutable transaction-time History anchor;
- both source- and destination-oriented adjacency records for edges;
- adapter replay metadata and the applied log index.

The same backend-neutral API reads Current vertices/edges, transaction-time `AS OF` views,
valid-time-filtered incoming/outgoing adjacency, and coalesced `Added`/`Removed`/`Changed` DIFF
ranges. Durable records have explicit tags, magic, format versions, big-endian fixed-width
fields, length-delimited canonical payloads, and checksums. The randomized TCK drives the same
fixed-seed corrections through the semantic model, Memory Adapter, and RocksDB after every
commit.

Phase 1B intentionally stores a full projection anchor for every changed element commit. This
is the correctness baseline and makes read-back simple and auditable, but write amplification is
linear in an element's valid-time segment count. Its initial AS OF implementation scanned the
complete element prefix. Phase 1C now uses a bounded reverse-time seek and decodes exactly one
eligible full anchor; Anchor+Delta replay and compaction remain the next storage-format step.

## Performance probe

Run the release-mode RocksDB microbenchmark with:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
DTGPROXY_BENCH_ITERS=200 \
cargo bench -p temporal-storage --bench roundtrip
```

Warm-cache measurements on 2026-07-17 using Apple M2, macOS 27.0, Rust 1.93, and RocksDB
0.24.0 were:

| Operation | Dataset | Nanoseconds/op |
|---|---:|---:|
| Current vertex point lookup | 32 anchors | 7,785 |
| AS OF latest snapshot | history depth 1 of 32 | 222,166 |
| AS OF oldest snapshot | history depth 32 | 230,516 |
| Retroactive correction with synchronous WAL | 8 measured commits | 480,421 |
| Current outgoing expansion | degree 32 | 29,541 |

The benchmark database occupied 290,152 bytes. These are development-host microbenchmarks, not
service SLOs. The nearly identical depth-1/depth-32 AS OF cost exposed the original full-prefix
scan.

After adding prefix-constrained range seek plus a one-record scan limit, the same command measured
11,462 ns/op for the latest snapshot and 2,828 ns/op for the oldest snapshot. That is a 94.8% and
98.8% reduction respectively versus the Phase 1B baseline. Current lookup remained 7,697 ns/op,
degree-32 expansion 29,440 ns/op, and synchronous correction 424,380 ns/op. The next benchmark
gate is bounded Anchor+Delta replay and write bytes, not further tuning of the full-anchor format.

## Development

The RocksDB binding compiles native C++ and bindgen code, so a C++17-capable Clang and
libclang are required. On this macOS development host, the Command Line Tools compiler cannot
resolve the standard C++ headers for the current SDK; Homebrew LLVM is the verified toolchain:

```bash
brew install llvm
export CXX=/opt/homebrew/opt/llvm/bin/clang++
export LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib
```

Run the durable adapter tests explicitly with:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p adapter-rocksdb
```

For the complete workspace:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test --workspace
cargo run -p dtgproxy -- --version
```
