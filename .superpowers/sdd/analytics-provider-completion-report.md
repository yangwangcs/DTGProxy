# Analytics and Provider completion report

## Implemented main functionality

- Canonical graph models: `SnapshotGraph`, `PartitionedSnapshotGraph`, `EventGraph`,
  `IntervalGraph`, and `DeltaGraph`.
- Bounded storage projection retains stable vertex/edge identity, full before/after payloads,
  parallel edges, interval segments, and exact scan-byte accounting.
- Gateway projection supports all four external graph models:
  - Snapshot remains partitioned when no transaction overlay is present.
  - Event histories merge deterministically across shards.
  - Interval segments merge across all shards before endpoint coverage validation.
  - Delta compares two valid-time endpoints at one fixed transaction snapshot, matching the
    approved temporal ownership model.
- Cross-shard projection budgets are shared. Interval usage charges the greater of identity entries
  scanned and segments materialized; Delta shares one entry/byte budget across both views. A budget
  exhausted before the final shard fails before another shard scan begins.
- Interval `CALL ... YIELD` execution is supported by the temporal coordinator and preserves the
  input `TemporalRegion` and provenance on every provider row.

## Provider and algorithm support

- Distributed-native deterministic kernels: Degree, WCC, and PageRank.
- Bounded canonical gathered fallback for other Snapshot algorithms.
- Implemented Snapshot algorithms: BFS, DFS, weighted SSSP, bounded weighted all-pairs shortest
  path, WCC, SCC, PageRank, degree centrality, weighted betweenness, weighted closeness, triangle
  count, clustering coefficient, K-core, label propagation, and deterministic hierarchical Louvain.
- Weighted Brandes rejects non-positive weights and counts parallel shortest paths by edge identity.
  Louvain preserves aggregate self-loop mass in degree totals while excluding it from local-move
  candidate-community weight, so higher-level merges are not biased toward staying.
- Implemented Event algorithms: temporal reachability, earliest arrival, latest departure, fastest
  path, minimum-hop temporal path, temporal degree, temporal closeness, temporal betweenness,
  temporal PageRank, burstiness, temporal clustering coefficient, topological overlap, windowed
  components, windowed triangle count, change-point score, and temporal motif count.
- Implemented Interval/Delta consumers: interval components and delta summary.
- Temporal motif counting is vertex-renaming invariant for equal-time events, accepts connected
  three-event endpoint unions, retains a 4,096-event input cap, and enforces a 1,000,000 candidate
  triple budget before canonical allocation.
- Local BFS/SSSP/WCC/SCC/PageRank and temporal PageRank event projection poll cooperative
  cancellation inside dense inner loops.

## Analytics jobs and typed procedures

- `dtg.analytics.submit`, `status`, `results`, and `cancel` execute through normal typed
  `CALL ... YIELD`.
- Job handles carry manager affinity and a process nonce.
- Result pages expose `columns`, `row`, `rowIndex`, and `hasMore`, with stable rejection of negative
  offsets and zero limits.
- Canceled providers retain capacity until their worker actually exits.
- Provider `DTG-*` errors are preserved through Procedure Runtime, Query Executor, Gateway, and
  Bolt; arbitrary provider detail still falls back to `DTG-PROCEDURE-PROVIDER`.

## Fresh functional verification (2026-07-21)

- Combined suites passed for `temporal-storage`, `analytics-api`, `analytics-runtime`,
  `procedure-runtime`, `query-executor`, `cypher-sema`, `cypher-compiler`, and `cypher-engine`.
- `gateway-node --lib`: 11/11 passed (one pre-existing unused-import warning remains).
- PrimaryReplica analytics integration: 1/1 passed.
- Shared-Nothing Gateway service integration, including Interval/Delta calls and analytics job
  pagination negatives: 1/1 passed.
- Independent reviews approved the algorithm corrections, Interval/Delta projection semantics,
  cross-shard budget accounting, cancellation polling, and error-code propagation.

## Remaining main functionality before boundary certification

1. Complete Shard-Raft checkpoint/result artifact storage and replace the process-local Procedure
   Runtime manager with the Meta-ledger Gateway scheduler. The replicated ledger, fencing, RPC,
   restart, and Meta leader-failover foundation is complete, but Gateway takeover is not yet wired.
2. Extend native distributed checkpoint/resume beyond Degree/WCC/PageRank; other Snapshot
   algorithms currently use bounded gathered fallback.
3. Run and close the complete RocksDB/Neo4j/PostgreSQL by PrimaryReplica/SharedNothing analytics
   equivalence matrix.

The unified boundary, license/SBOM, full clippy, performance, and cleanup phase remains deliberately
deferred until these main-function items are complete.
