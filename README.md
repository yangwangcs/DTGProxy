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
- `temporal-ir`: versioned, validated backend-neutral plans for point lookup, expansion, AS OF,
  and DIFF;
- `query-executor`: deterministic local execution with typed vertex, edge, and change records;
- `dtgproxy`: executable product entry point.

Phase 0, the Phase 1A durable adapter, and the Phase 1B temporal persistence slice are
implemented and tested. Bounded Anchor+Delta history, atomic multi-element single-node
transactions, typed Temporal IR, and the local executor are also implemented in Phase 1C. The
text query frontend, replicated shard runtime, distributed transactions, external database
adapters, and analytics integration remain active implementation phases described by the design.

The durable layout has eight stable RocksDB Column Families: `meta`, `identity`, `current`,
`adj_out`, `adj_in`, `history`, `temporal_index`, and `txn`. The RocksDB `default` Column
Family is also present because RocksDB requires it, but DTGProxy does not place logical data
there.

The approved architecture and remaining distributed phases are specified in [the detailed design](docs/superpowers/specs/2026-07-17-dtgproxy-design.md).

## Temporal persistence slice

`TemporalStore` accepts a `TemporalTransaction` containing typed vertex and edge mutations with
valid-time intervals plus one transaction context. It validates immutable identity, interval-level
write conflicts, and temporal referential integrity; rewrites all changes into disjoint valid-time
segments; and sends exactly one deterministic `CommittedMutationBatch` to the selected adapter.
Single-vertex and single-edge methods are wrappers around this transaction API. The batch
atomically contains:

- the graph-scoped identity record;
- the latest Current projection;
- immutable transaction-time History Anchor/Delta entries;
- both source- and destination-oriented adjacency records for edges;
- adapter replay metadata and the applied log index.

The same backend-neutral API reads Current vertices/edges, transaction-time `AS OF` views,
valid-time-filtered incoming/outgoing adjacency, and coalesced `Added`/`Removed`/`Changed` DIFF
ranges. Durable records have explicit tags, magic, format versions, big-endian fixed-width
fields, length-delimited canonical payloads, and checksums. The randomized TCK drives the same
fixed-seed corrections through the semantic model, Memory Adapter, and RocksDB after every
commit.

Operations are normalized by graph, partition, kind, and element before mutation sequence numbers
are assigned. A transaction may create both endpoints and their edge together. Every edge valid
interval must be fully covered by both endpoint projections after all staged vertex changes.
Conversely, deleting part of a vertex lifetime is rejected if an incident edge would become
dangling, unless that edge is coordinately rewritten in the same transaction. Validation failure,
log-order failure, or replay mismatch leaves Current, History, both adjacency directions, and the
applied log index unchanged. Memory and RocksDB execute the same transaction contract tests.

Phase 1B intentionally stores a full projection anchor for every changed element commit. This
is the correctness baseline and makes read-back simple and auditable, but write amplification is
linear in an element's valid-time segment count. Its initial AS OF implementation scanned the
complete element prefix. Phase 1C now uses a bounded reverse-time seek and decodes exactly one
eligible record. History now starts with an Anchor, writes at most 15 bounded Deltas, and forces a
new Anchor when either that replay count or a 64 KiB encoded-delta budget would be exceeded.
Readers seek directly to the requested transaction time and replay backward only to the nearest
Anchor. Missing or over-limit chains fail closed. Phase 1B full-anchor records remain readable and
can serve as migration anchors for new Deltas.

## Temporal IR and local execution

`TemporalPlan` version 1 makes graph/partition scope, point valid time, Current versus transaction
`AS OF`, result bounds, and DIFF transaction bounds explicit. Required selectors cannot be omitted
from the typed plan. Validation rejects unknown versions, zero/unbounded limits, and reversed DIFF
bounds before storage is accessed.

`LocalExecutor` executes only through `TemporalStore` and returns canonical typed vertex, edge, or
change records. Expansion applies valid-time filtering, de-duplicates `Both` direction self-edges,
orders by stable element identity, and applies limits after ordering. Historical expansion does not
reuse Current adjacency: it scans the immutable edge identity directory and reconstructs each
candidate at the requested transaction time, so fully deleted edges remain visible in older
snapshots. This is a correctness-first fallback; Phase 2+ can add a versioned adjacency index and
capability-aware pushdown without changing IR semantics.

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

With 1,008 commits on one element, the Anchor+Delta benchmark measured 5,684 ns/op at replay
depth 1, 20,457 ns/op at the maximum replay depth 16, and 13,528 ns/op for a snapshot 1,000
versions behind the current state. Actual History values occupied 100,827 bytes (100 bytes/record)
versus 148,077 hypothetical bytes if every commit were a full Anchor, a 31.9% reduction for this
single-segment workload. Current lookup measured 3,095 ns/op, synchronous correction 131,046
ns/op, and degree-32 expansion 29,456 ns/op. These results are the baseline for multi-segment and
transaction benchmarks, not production SLOs.

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
