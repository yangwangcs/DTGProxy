# DTGProxy 1.0 prototype quickstart

DTGProxy exposes one temporal graph contract over RocksDB, PostgreSQL, Neo4j, or a remote Sidecar. The runnable prototype supports `PrimaryReplica` and `SharedNothing`, durable bitemporal transactions, graph point/adjacency queries, global scans, backend verification, and logical hot migration.

Read the [final boundary audit](dtgproxy-v1-boundary-audit.md) before exposing the prototype outside
a trusted development network.

## Build and run

The minimum local path uses RocksDB and requires a C++17 toolchain. On Homebrew macOS:

```bash
export CXX=/opt/homebrew/opt/llvm/bin/clang++
export LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib
cargo build -p dtgproxy
```

Use either runnable example:

- [`examples/primary-replica`](../examples/primary-replica/README.md)
- [`examples/shared-nothing`](../examples/shared-nothing/README.md)

`serve` is the long-running process. `transaction`, `query`, and `backend migrate` send bounded, versioned JSON frames to it.

## Backend profiles

RocksDB needs no secret:

```bash
dtgproxy init ... --backend rocksdb
```

PostgreSQL keeps its connection string outside the catalog:

```bash
export DTG_POSTGRES_URL='postgresql://user:password@127.0.0.1/dtgproxy'
dtgproxy init ... --backend postgresql --backend-secret-ref env:DTG_POSTGRES_URL
```

Neo4j uses its official HTTP Query API v2. The password is resolved locally and only a reference is persisted:

```bash
export DTG_NEO4J_PASSWORD='change-me'
dtgproxy init ... --backend neo4j \
  --backend-endpoint http://127.0.0.1:7474 \
  --backend-database neo4j --backend-user neo4j \
  --backend-secret-ref env:DTG_NEO4J_PASSWORD
```

The Neo4j mapping uses reserved nodes and unique constraints. Keys are hexadecimal strings so byte ordering is preserved; values remain canonical DTP1 bytes encoded as Base64. Apply, fingerprints, and applied-index CAS occur in one parameterized Cypher statement. See the [Neo4j Query API](https://neo4j.com/docs/query-api/current/) and [Cypher `MERGE` guidance](https://neo4j.com/docs/cypher-manual/current/clauses/merge/).

Sidecar profiles use `--backend sidecar --backend-endpoint host:port`; the stateful protocol supports logical export and restore without sending backend credentials over the Sidecar wire.

For the independently deployed Meta/Data/Controller path, including durable per-replica receipts,
Raft-fenced dual apply, the `dtgproxy-admin` start/status/abort commands, and per-Shard PostgreSQL or
Neo4j Sidecars, use the [backend migration runbook](backend-migration-runbook.md).

## Verify and migrate

Live verification opens every configured replica Adapter, validates the SPI capability requirement, and reports the backend family and durable index:

```bash
dtgproxy backend verify --config node.json
```

With `serve` running, migrate RocksDB online:

```bash
dtgproxy backend migrate --config node.json \
  --provider rocksdb --backend-path backends/generation-2
```

For PostgreSQL or Neo4j, supply the target endpoint/public parameters and an `env:` or `file:` secret reference. The Gateway exports every source replica at its applied-index fence, restores all targets, enables dual apply, cuts over while requests are serialized, and publishes the new catalog generation.

## Query syntax

```text
VERTEX 7 GRAPH 1 PARTITION 0 FOR VALID TIME 5 CURRENT LIMIT 1
EDGE 9 GRAPH 1 PARTITION 0 FOR VALID TIME 5 AS OF TRANSACTION TIME 100:2 LIMIT 1
EXPAND BOTH FROM 7 GRAPH 1 PARTITION 0 FOR VALID TIME 5 CURRENT LIMIT 100
SCAN VERTICES GRAPH 1 FOR VALID TIME 5 CURRENT LIMIT 100
SCAN EDGES GRAPH 1 FOR VALID TIME 5 AS OF TRANSACTION TIME 100:2 LIMIT 100
```

Global scans fan out to all physical Shards, validate one topology epoch and temporal selector, merge by canonical `(kind, graph, partition, element-id)` order, deduplicate, and only then apply the global limit.
