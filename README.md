# DTGProxy

DTGProxy is a distributed bitemporal property-graph middleware written in Rust. It owns valid-time and transaction-time semantics above pluggable ordinary KV and graph database backends.

The repository is being delivered in independently verifiable phases. The implemented kernel contains:

- `temporal-types`: bitemporal primitives and canonical values;
- `temporal-model`: executable in-memory semantic oracle;
- `storage-api`: backend-neutral prepared/committed mutation contracts;
- `adapter-memory`: atomic and idempotent reference adapter;
- `adapter-rocksdb`: durable RocksDB 0.24.0 adapter with atomic cross-keyspace writes,
  idempotent replay, restart recovery, snapshot reads, and checkpoints;
- `temporal-storage`: deterministic graph key/value codecs, Current/History rewriting, typed
  vertex and edge reads, double adjacency, AS OF, and temporal DIFF;
- `temporal-ir`: versioned, validated backend-neutral plans for point lookup, expansion, AS OF,
  and DIFF;
- `query-executor`: deterministic local execution with typed vertex, edge, and change records;
- `temporal-query`: bounded hand-written parser compiling a small temporal syntax to the IR;
- `dtgproxy`: executable product entry point.

Phase 0, the Phase 1A durable adapter, and the Phase 1B temporal persistence slice are
implemented and tested. Bounded Anchor+Delta history, atomic multi-element single-node
transactions, typed Temporal IR, the local executor, and the minimal text query frontend are also
implemented in Phase 1C. The replicated shard runtime, distributed transactions, external
database adapters, and analytics integration remain active implementation phases described by the
design. Phase 2 has started with a deterministic prepare/apply boundary: temporal validation and
graph rewriting produce a log-position-independent `PreparedMutationBatch`, while only committed
state-machine application assigns the Raft log index and touches the Adapter.

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

## Minimal temporal query syntax

The Phase 1C frontend intentionally accepts only fixed-shape ID lookup, one-hop expansion, and
element DIFF. Keywords are case-insensitive; identifiers and times are decimal integers;
transaction timestamps are `physical_micros:logical`. The complete forms are:

```text
VERTEX <id> GRAPH <graph> PARTITION <partition>
  FOR VALID TIME <micros> CURRENT LIMIT <n>

EDGE <id> GRAPH <graph> PARTITION <partition>
  FOR VALID TIME <micros> AS OF TRANSACTION TIME <physical>:<logical> LIMIT <n>

EXPAND OUT|IN|BOTH FROM <vertex-id> GRAPH <graph> PARTITION <partition>
  FOR VALID TIME <micros> CURRENT|AS OF TRANSACTION TIME <physical>:<logical> LIMIT <n>

VERTEX|EDGE <id> GRAPH <graph> PARTITION <partition>
  DIFF TRANSACTION TIME <from-physical>:<from-logical>
  TO <to-physical>:<to-logical> LIMIT <n>
```

Input is capped at 4,096 bytes and 64 tokens. Integer widths, unknown statements, missing clauses,
trailing tokens, result bounds, and reversed DIFF ranges produce structured errors. Cypher/GQL
constructs such as `MATCH` are deliberately rejected rather than partially interpreted.

Execute a query against a RocksDB database or checkpoint with:

```bash
dtgproxy query --db /path/to/db --text \
  "VERTEX 7 GRAPH 1 PARTITION 0 FOR VALID TIME 5 CURRENT LIMIT 1"
```

The CLI emits canonical JSON. Graph/element identifiers and times are decimal strings to avoid
consumer precision loss; canonical property payloads are DTP1 bytes encoded as lowercase hex, so
all graph value types round-trip without lossy JSON coercion.

## Phase 1C isolation and deployment boundary

Phase 1C is a complete single-node semantic/product slice, not yet a distributed deployment. Its
transaction contract is Temporal Snapshot Isolation for written element/valid-time intervals:
overlapping intervening writes conflict, while disjoint valid-time corrections may commit. It also
enforces immutable identities and strict same-partition edge lifetime coverage. It does not yet
detect arbitrary read/write predicates or provide Temporal Serializable isolation.

`CommitContext` timestamps, transaction IDs, shard IDs, and log indices are supplied by the caller.
The next-log-index barrier models one ordered state machine and prevents a successful transaction
from preparing over an unapplied lower log entry. Phase 2 must replace this local ordering
assumption with replicated Raft proposal/apply and safe-time tracking; Phase 3 must add the global
timestamp oracle, intents, cross-shard 2PC, epoch checks, recovery, and edge guard locks. Endpoint
references currently use one graph partition; cross-partition edge projections are therefore not
claimed by Phase 1C.

The Phase 2 `PrepareContext` deliberately omits a log index. `prepare_transaction` is read-only and
deterministic for one applied shard state; `commit_transaction` remains a compatibility wrapper
that prepares and locally applies. The replicated runtime will serialize state-dependent prepare
operations per shard before proposing them so concurrent proposals cannot validate against the
same stale applied frontier.

Only Memory and RocksDB adapters are implemented. Neo4j and other graph backends remain subject to
the same capability contract and TCK. Historical expansion is semantically complete but performs a
partition edge-identity scan; it is not a production complexity target until versioned adjacency
indexes and capability-aware pushdown are added.

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

The Phase 1C acceptance run on the same development host and toolchain, after adding local
transactions and query execution, measured:

| Operation | Acceptance dataset | Nanoseconds/op |
|---|---:|---:|
| Current vertex point lookup | 1,016 versions | 2,797 |
| AS OF replay depth 1 | 1,008-version history | 5,005 |
| AS OF replay depth 16 | maximum configured chain | 24,360 |
| AS OF snapshot age 1,000 | old bounded seek | 15,600 |
| Retroactive correction, sync WAL | 8 commits | 153,562 |
| Current outgoing expansion | degree 32 | 35,193 |
| Historical outgoing expansion | 32 edge identities | 163,302 |
| Atomic two-vertex/one-edge transaction | 11 mutations, sync WAL | 182,671 |

History values remained 100,827 bytes versus 148,077 hypothetical all-anchor bytes. The final
database occupied 1,164,438 bytes because this acceptance probe additionally persists 200 atomic
three-element transactions. Historical expansion's 163 µs result quantifies the documented
identity-scan fallback and is a Phase 2 indexing target. Differences from earlier micro-runs are
treated as host/run variance unless reproduced by a dedicated benchmark harness; none of these
numbers are service SLOs.

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
