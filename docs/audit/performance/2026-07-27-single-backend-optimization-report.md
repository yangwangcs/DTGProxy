# DTGProxy Single-Backend Performance Optimization Report

Date: 2026-07-27

## Scope and conclusion

DTGProxy now requires an explicit logical backend selection and initializes only that backend in
normal operation. RocksDB, PostgreSQL, and Neo4j were measured in separate, sequential local runs.
The baseline identified PostgreSQL canonical scanning as the dominant middleware bottleneck. The
page query was changed to return already-canonical stored values for Current, History, and Opaque
records, eliminating one reload query per returned entry while preserving the fallback encoder for
other keyspaces.

Under the identical shortened diagnostic protocol, PostgreSQL Adapter Direct throughput increased
from 0.333333 to 100.333333 operations per second (301 times the baseline; +30,000%), while p50
latency decreased from 2,650.087042 ms to 9.485041 ms (-99.642%). All compared observations had zero
errors and the same result identity. This is strong diagnostic evidence for the selected
optimization, but it is not a formal production or multi-node performance claim.

## Method

- Host: Apple M2, 16 GiB RAM, arm64 macOS.
- Toolchain: Rust 1.93.0 and Cargo 1.93.0.
- Backend order: RocksDB, PostgreSQL 17, Neo4j 5.26 Community.
- Isolation: PostgreSQL and Neo4j managed containers were stopped before RocksDB; only PostgreSQL
  was running for its measurement; PostgreSQL was stopped before Neo4j was started. The final
  Neo4j container was stopped after capture.
- Dataset: graph 7 with 4,096 current vertices.
- Query: fixed-snapshot current vertex count.
- Paths: Backend Direct and production Adapter Direct.
- Concurrency: 1.
- Per observation: 1-second warmup and 3-second measurement.
- Repetitions: 3 per path and backend.
- Correctness: six raw observations per backend, zero errors, identical one-row result digest.
- Integrity: every raw file's SHA-256 was recomputed and compared with its diagnostic manifest.

This protocol is intentionally labeled `diagnostic`. It is shorter and smaller than the formal
paper protocol of 30-second warmup, 60-second measurement, five repetitions, the full dataset,
concurrency 1/8/32/64, and real 1/4/8 Data Node topologies.

## Data and artifact locations

Baseline manifests:

- `target-codex-paper-diagnostics/2026-07-27-single-backend/rocksdb/diagnostic-manifest.json`
- `target-codex-paper-diagnostics/2026-07-27-single-backend/postgresql/diagnostic-manifest.json`
- `target-codex-paper-diagnostics/2026-07-27-single-backend/neo4j/diagnostic-manifest.json`

After manifests:

- `target-codex-paper-diagnostics/2026-07-27-single-backend/after/rocksdb/diagnostic-manifest.json`
- `target-codex-paper-diagnostics/2026-07-27-single-backend/after/postgresql/diagnostic-manifest.json`
- `target-codex-paper-diagnostics/2026-07-27-single-backend/after/neo4j/diagnostic-manifest.json`

The rejected Neo4j invocation used a malformed endpoint before measurement and is preserved under
`target-codex-paper-diagnostics/2026-07-27-single-backend/after/neo4j-failed-endpoint-format/`.
It contains no successful diagnostic manifest and is excluded from all results.

## Baseline bottleneck

| Backend | Path | Throughput (ops/s) | p50 (ms) | p95 (ms) |
|---|---|---:|---:|---:|
| RocksDB | Backend Direct | 277.666667 | 3.760625 | 3.970500 |
| RocksDB | Adapter Direct | 201.222222 | 4.906958 | 5.379333 |
| PostgreSQL | Backend Direct | 1,432.444444 | 0.682917 | 0.888875 |
| PostgreSQL | Adapter Direct | 0.333333 | 2,650.087042 | 2,680.739625 |
| Neo4j | Backend Direct | 177.666667 | 5.335000 | 7.179583 |
| Neo4j | Adapter Direct | 25.000000 | 39.480833 | 46.213334 |

PostgreSQL added about 2,649.404 ms of Adapter p50 time above Backend Direct, versus 34.146 ms for
Neo4j and 1.146 ms for RocksDB. Code inspection tied this to
`PostgresReadSnapshot::scan_canonical_page`: the canonical page SQL returned logical keys, and the
reader called `load_canonical_entry` once per key. A 4,096-row Current scan therefore executed a
page query followed by thousands of reload queries.

## Code change

`POSTGRES_CANONICAL_SCAN_SQL` now returns `(logical_key, canonical_value)`.

- Vertex Current and Edge Current return their stored `projection` bytes.
- History returns its stored `history_value` bytes.
- Opaque keyspaces return their stored `value` bytes.
- Identity, adjacency, replay, and applied-index branches return `NULL::bytea` and retain the
  existing `load_canonical_entry` fallback because they require canonical encoding or lookup.

`PostgresReadSnapshot::scan_canonical_page` constructs `KeyValue` directly when the optional value
is present. Ordering, exclusive end bounds, `max_items + 1` continuation detection, byte charging,
repeatable-read snapshot pinning, Storage SPI types, and result identity are unchanged. The SQL
contract asserts which branches return stored bytes and which preserve the fallback.

## Before and after

### Adapter Direct

| Backend | Before ops/s | After ops/s | Throughput change | Before p50 (ms) | After p50 (ms) | p50 change |
|---|---:|---:|---:|---:|---:|---:|
| RocksDB | 201.222222 | 199.666667 | -0.773% | 4.906958 | 4.957084 | +1.022% |
| PostgreSQL | 0.333333 | 100.333333 | +30,000.000% | 2,650.087042 | 9.485041 | -99.642% |
| Neo4j | 25.000000 | 24.000000 | -4.000% | 39.480833 | 41.493000 | +5.097% |

### Backend Direct environment control

| Backend | Before ops/s | After ops/s | Throughput change | Before p50 (ms) | After p50 (ms) | p50 change |
|---|---:|---:|---:|---:|---:|---:|
| RocksDB | 277.666667 | 273.555556 | -1.481% | 3.760625 | 3.759250 | -0.037% |
| PostgreSQL | 1,432.444444 | 1,304.111111 | -8.959% | 0.682917 | 0.731209 | +7.071% |
| Neo4j | 177.666667 | 118.888889 | -33.083% | 5.335000 | 8.078250 | +51.420% |

RocksDB stayed within about 1.5% throughput drift. PostgreSQL Backend Direct became about 9% slower,
yet Adapter Direct improved by 301 times, so the observed gain cannot be explained by a faster
PostgreSQL service run. Neo4j Backend Direct changed substantially between runs; its small Adapter
regression is therefore treated as environment/service drift, not an effect of the PostgreSQL-only
code change and not a cross-backend performance conclusion.

## Correctness and verification

For both baseline and after phases, each backend contained exactly six raw observations, with three
per path. All raw hashes matched their manifests. Every observation reported `errors = 0`, and all
paths and repetitions produced:

```text
row_count = 1
digest = 3914e62ea65785c975f12b61eb5d6021dcced4e835a3f8768e69f672fcdbc6ca
```

The live PostgreSQL repeatable-read pagination test passed against PostgreSQL 17. The SQL contract,
adapter package tests, diagnostic runner contract, report contract, formatting checks, shell syntax
checks, and whitespace checks form the regression gate recorded with the delivery.

The complete live PostgreSQL package suite passed with `--test-threads=1` (22 tests). A prior
default-parallel attempt produced one PostgreSQL SSI `retryable serialization` conflict between two
otherwise independent native-mapping tests; the failing test passed alone and the full serial suite
passed. This is a disposable-test concurrency limitation, not evidence of a semantic failure or a
benchmark result, and real-backend certification remains serialized.

## Formal lifecycle isolation control

The shortened diagnostic data above was captured with explicit container sequencing. A subsequent
formal-readiness review correctly identified that preparation-time Gateway/Data Node identities and
an “external/unmanaged” PostgreSQL or Neo4j declaration were insufficient to prove which backend
service a formal run actually used. The formal runner was therefore hardened without changing the
reported diagnostic measurements:

- Every prepared backend bundle now seals one executable lifecycle runner by absolute path,
  protocol version 1, and SHA-256.
- All three bundle preflights must pass before the first backend and again after each backend is
  released. Only the selected backend lifecycle runner receives `run`.
- Runtime evidence comes from the executed lifecycle operation, not from preparation-time PIDs.
  PostgreSQL and Neo4j require exactly one `backend_service` identity; embedded RocksDB forbids one.
- The isolated runner validates backend/run identity, role cardinality, executable digests, probe
  schemas, and identity uniqueness. Gateway identities are checked against the sealed runtime and
  preparation evidence; Data Node identities are checked against every verified raw Proxy topology
  observation and the sealed runtime manifest. The runner then independently proves each exact
  PID/start identity retired.
- `combined/isolation-evidence.json` binds the normalized runtime-evidence digest to the verified
  artifact `SHA256SUMS` digest for RocksDB, PostgreSQL, and Neo4j. The combined checksum inventory
  covers this binding and rejects missing or extra files.
- The failure contracts cover changed runner digests, failed preflights, wrong backend/run IDs,
  duplicate identities, missing or forbidden backend services, live identities, changed artifact
  contents, unbound Gateway/Data Node evidence, and corrupt, extra-directory, or symlinked combined
  output. Each initially verified artifact is copied to a private read-only snapshot; combine,
  final full verification, complete checksum-inventory recheck, and binding use only the snapshot.
  The published report then replaces private snapshot paths with digest-checked persistent artifact
  paths. Persistent artifacts receive the same closed-tree and per-file checksum verification both
  before path publication and after combined validation, so no reference becomes invalid or points
  to content that differs from the sealed snapshot when temporary state is removed.

This closes the formal runner's service-identity and retirement-proof gap for future full-matrix
runs. It does not relabel the existing one-second/three-second local captures as formal evidence.

## Reproduction

RocksDB:

```bash
scripts/run-paper-diagnostic.sh \
  --backend rocksdb \
  --output-dir /absolute/new/path/rocksdb \
  --warmup-seconds 1 --measurement-seconds 3 --repetitions 3
```

PostgreSQL:

```bash
DTGPROXY_PAPER_TEST_POSTGRES_URL='<disposable PostgreSQL connection string>' \
DTGPROXY_DIAGNOSTIC_BACKEND_IMAGE='postgres:17' \
scripts/run-paper-diagnostic.sh \
  --backend postgresql \
  --output-dir /absolute/new/path/postgresql \
  --warmup-seconds 1 --measurement-seconds 3 --repetitions 3
```

Neo4j:

```bash
DTGPROXY_PAPER_TEST_NEO4J_ENDPOINT='http://127.0.0.1:7474' \
DTGPROXY_PAPER_TEST_NEO4J_USERNAME='<username>' \
DTGPROXY_PAPER_TEST_NEO4J_PASSWORD='<password>' \
DTGPROXY_PAPER_TEST_NEO4J_DATABASE='neo4j' \
DTGPROXY_DIAGNOSTIC_BACKEND_IMAGE='neo4j:5.26-community' \
scripts/run-paper-diagnostic.sh \
  --backend neo4j \
  --output-dir /absolute/new/path/neo4j \
  --warmup-seconds 1 --measurement-seconds 3 --repetitions 3
```

Every output directory must be new. Run one backend at a time and stop the previous managed service
before advancing. Formal runs must instead use the sealed lifecycle protocol documented in
`docs/paper-performance-artifact.md`; manual service sequencing is not accepted as formal isolation
proof.

## Limitations and next work

- The diagnostic did not execute Proxy, Bolt, distributed fanout, or TTFR paths.
- CPU, RSS, network, and detailed Gateway stage metrics are absent from this small capture.
- No formal full-dataset, higher-concurrency, or real 1/4/8-node matrix was executed.
- PostgreSQL still uses fallback per-entry encoding for identity, adjacency, replay, and applied-index
  keyspaces. Those paths require separate measurement before further optimization.
- Neo4j showed substantial between-run Backend Direct drift and needs a quieter, pinned environment
  for a backend-specific conclusion.

The measured acceptance criterion was met: PostgreSQL Adapter Direct improved beyond the baseline,
with zero errors and unchanged result identity. The evidence supports merging the canonical-page
optimization and then running the formal isolated matrix in a dedicated benchmark environment.
