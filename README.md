# DTGProxy

DTGProxy is a distributed bitemporal property-graph middleware written in Rust. It owns valid-time and transaction-time semantics above pluggable ordinary KV and graph database backends.

The 1.0 prototype implements the complete main path:

- bitemporal Current/History storage, bounded Anchor+Delta reconstruction, `AS OF`, `DIFF`, and
  double adjacency;
- independent Raft groups with durable WAL, checkpoint/suffix recovery, epoch fencing, ReadIndex,
  and follower safe-time reads;
- durable timestamp allocation and distributed temporal transactions with Home decisions,
  participant intents, 2PC recovery metadata, and a single-Shard fast path;
- `PrimaryReplica` and rendezvous-routed `SharedNothing` deployment modes, including
  cross-partition edge projections;
- one Adapter SPI over Memory, RocksDB, PostgreSQL, Neo4j transactional Query API, and remote
  Sidecar backends;
- logical snapshots, stateful Sidecar export/restore, online dual-apply backend migration, and
  generation-based catalog publication;
- typed Temporal IR, interval-preserving temporal rows, distributed fragments, and Cypher
  compilation;
- Cypher/Bolt execution through `gateway-node`, plus a `dtgproxy` CLI for initialization,
  serving, transaction submission, backend verification, and migration.

The durable layout has eight stable RocksDB Column Families: `meta`, `identity`, `current`,
`adj_out`, `adj_in`, `history`, `temporal_index`, and `txn`. The RocksDB `default` Column
Family is also present because RocksDB requires it, but DTGProxy does not place logical data
there.

The current architecture and implementation scope are defined by the
[Temporal Cypher and analytics design](docs/superpowers/specs/2026-07-19-dtgproxy-temporal-cypher-analytics-design.md)
and its [master implementation plan](docs/superpowers/plans/2026-07-19-dtgproxy-temporal-cypher-analytics-master.md).
Protocol specifications for Raft commands, state-machine recovery, snapshots, transactions,
control-plane state, Adapter SPI, and Sidecar transport are under [`docs/`](docs/).

The independently deployable cluster path consists of `dtgproxy-meta`, `dtgproxy-data`,
`dtgproxy-gateway`, and `dtgproxy-controller`. A loopback two-Data-node configuration set is under
[`config/examples/cluster-dev`](config/examples/cluster-dev/README.md). The cluster Controller can
move a live Shard with resumable snapshot transfer, learner catch-up, joint consensus, epoch
lineage, and cleanup pins; see the [P0 verification record](docs/verification/dtgproxy-p0-cluster-runtime.md).

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

History starts with an Anchor, writes at most 15 bounded Deltas, and forces a new Anchor when
either the replay count or a 64 KiB encoded-delta budget would be exceeded. Readers seek directly
to the requested transaction time and replay backward only to the nearest Anchor. Missing,
over-limit, or obsolete record formats fail closed.

## Temporal Cypher execution

The only query surface is Temporal Cypher through the Cypher/Bolt gateway. Compilation lowers
queries into the current typed Temporal IR, then into bounded distributed physical fragments.
Point reads produce ordinary Cypher rows; interval reads retain valid-time and transaction-time
regions through temporal joins until projection.

## Prototype 1.0 isolation and deployment boundary

The transaction contract is Temporal Snapshot Isolation for written element/valid-time intervals:
overlapping intervening writes conflict while disjoint valid-time corrections may commit. A durable
timestamp oracle assigns transaction time; multi-Shard transactions prewrite epoch-fenced intents,
persist their final decision on the Home Shard, and then resolve every participant. Single-Shard
transactions use one replicated command. Vertex/edge identity and lifetime validation includes
cross-partition OUT/IN edge projections.

This prototype does not claim predicate-level Temporal Serializable isolation. Historical
expansion is semantically complete but still uses an edge-identity scan; global scans fan out and
merge in the middleware rather than pushing a distributed plan into every backend. PostgreSQL and
Neo4j have environment-gated live integration tests because the default workspace test does not
provision external services. Production hardening findings are recorded separately after the main
path acceptance run.

## Performance probe

Run the release-mode RocksDB microbenchmark with:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
DTGPROXY_BENCH_ITERS=200 \
cargo bench -p temporal-storage --bench roundtrip
```

With 1,008 commits on one element, the Anchor+Delta benchmark measured 5,684 ns/op at replay
depth 1, 20,457 ns/op at the maximum replay depth 16, and 13,528 ns/op for a snapshot 1,000
versions behind the current state. Actual History values occupied 100,827 bytes (100 bytes/record)
Current lookup measured 3,095 ns/op, synchronous correction 131,046 ns/op, and degree-32
expansion 29,456 ns/op. These results are development-host microbenchmarks, not service SLOs.

The current acceptance run on the same development host and toolchain, after adding local
transactions and query execution, measured:

| Operation | Acceptance dataset | Nanoseconds/op |
|---|---:|---:|
| Current vertex point lookup | 1,016 versions | 2,797 |
| AS OF replay depth 1 | 1,008-version history | 5,005 |
| AS OF replay depth 16 | maximum configured chain | 24,360 |
| AS OF snapshot age 1,000 | bounded seek | 15,600 |
| Retroactive correction, sync WAL | 8 commits | 153,562 |
| Current outgoing expansion | degree 32 | 35,193 |
| Historical outgoing expansion | 32 edge identities | 163,302 |
| Atomic two-vertex/one-edge transaction | 11 mutations, sync WAL | 182,671 |

History values remained 100,827 bytes versus 148,077 hypothetical all-anchor bytes. The final
database occupied 1,164,438 bytes because this acceptance probe additionally persists 200 atomic
three-element transactions. Historical expansion's 163 µs result quantifies the documented
identity-scan fallback and is a future indexing target. Differences from earlier micro-runs are
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
