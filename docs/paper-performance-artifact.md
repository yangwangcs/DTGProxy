# Paper Performance Artifact

## Scope

The paper performance orchestrator creates immutable, identity-checked experiment evidence. It has
two execution paths:

- `run` invokes an explicit real cell executor once for each shuffled matrix entry.
- `simulate` emits a tiny synthetic diagnostic for fixture certification only.

The orchestrator does not start DTGProxy, PostgreSQL, Neo4j, or any other service. Service setup and
the real Backend Direct, Adapter Direct, and Proxy implementations belong to the supplied cell
executor. A failed cell stops the run immediately. There is no automatic retry.

## Formal Protocol

Run mode is derived from the specification. It is not a command-line label. A run is `formal` only
when all of these values match exactly:

- Dataset: 1,000,000 vertices, 5,000,000 edges, and 600,000 temporal updates. The update count is
  10 percent of all vertices and edges.
- `comparison`: all three paths at one data node, production only, concurrency `1,8,32,64`.
- `scale`: Proxy production only at `1,4,8` data nodes, concurrency `1,8,32,64`.
- `ablation`: Proxy only at the 8-node/concurrency-32 anchor, with production and each of the five
  single-disabled optimization modes.
- Warmup: 30 seconds.
- Measurement: 60 seconds.
- Repetitions: 5.
- Execution: a real external cell executor, never the simulator.

Changing any listed value produces a `diagnostic` run. Every suite contains RocksDB, PostgreSQL,
and Neo4j. Only comparison workloads must expose all three semantically equivalent paths. Scale
uses partition-parallel workloads; ablation uses workloads that exercise every measured alternate.

Dataset preparation may be resumable, but an experiment specification is accepted only after its
dataset manifest contains exact completed counts and a 64-hex content digest.

## Artifact Layout

Each successful run is stored at:

```text
artifacts/paper-performance/<run-id>/
  manifest.json
  configs/
    matrix.json
    cells/*.json
  raw/*.json
  summary/
    summary.json
    summary.csv
  figures/
    throughput.csv
  logs/
  SHA256SUMS
```

The run directory must not already exist. Every final file is written to a unique same-directory
temporary file, flushed with `fsync`, and published with a no-replace operation. Existing final
files are never overwritten. `summary/`, `figures/`, and `SHA256SUMS` are generated only after the
complete raw matrix passes schema, cell completeness, repetition uniqueness, required metric, and
three-path result identity validation.

`ResourceMetric::Unavailable` remains explicit in raw JSON. It is never converted to zero. A cell
with an unavailable required CPU, peak RSS, network receive, or network transmit metric cannot
produce a summary.

## Experiment Specification

The `--spec` file is a JSON encoding of `ExperimentSpec`. It contains:

- `schema_version`, `run_id`, source `revision`, and `dirty_worktree_digest`.
- A captured `environment` fingerprint containing OS, architecture, CPU, memory, Rust toolchain,
  RocksDB/PostgreSQL/Neo4j versions, and SHA-256 identities for the executor, Gateway, data-node,
  meta-node, and Bolt loadgen binaries. Its canonical SHA-256 digest is part of the run
  configuration identity. Synthetic or placeholder fingerprints are rejected for formal runs.
- One completed `dataset` manifest.
- One or more workload cases, each with a validated `WorkloadManifest` and pinned `snapshot`.
- Explicit `comparison`, `scale`, and `ablation` suites with purpose-specific axes and workloads.
- Warmup, measurement, repetition, and deterministic shuffle seed values.

Workload and dataset digests are validated before the run directory is created. Suite-local products
are merged, and identical Proxy production cells shared by suites are measured once. Matrix order
uses a deterministic SplitMix64 Fisher-Yates shuffle and is recomputed by offline verification.

## Cell Executor Contract

For each matrix entry the orchestrator invokes the executable exactly once:

```text
<executor> --cell-config <immutable-config.json> --output <unique-temp-output.json>
```

The executor must create the requested output file with one `RawObservation` JSON object. Its
backend, path, workload, node count, concurrency, ablation, repetition, dataset/workload/config
digests, pinned snapshot, parameter digest, and exact timing duration must match the cell config.
All observations for the same workload must have identical result digest and row count across
backend, path, node count, concurrency, ablation, and repetition.

For Adapter Direct, the Rust runner contract requires the production
`&dyn storage_api::ReadSnapshot`; a snapshot label alone does not satisfy the interface.

## Diagnostic Certification

This command runs only the tiny one-second simulator and writes to a temporary directory:

```text
CARGO_TARGET_DIR=target/paper-performance-fixtures \
  scripts/tests/paper-performance-fixtures.sh
```

The fixture checks a valid baseline and rejects an incomplete matrix, duplicate cell, missing
repetition, unequal identity, unavailable required metric, modified raw file, and a summary that
cannot be reproduced from raw observations.

## Formal Invocation

Prepare one sealed bundle per backend. Each specification, runtime inventory, dataset evidence, and
backend evidence must describe only the selected backend. Preparation validates the fixed matrix,
data conditions, independent data-node processes, unique Gateway control sockets, backend version,
and release binary identities. It writes a new sealed bundle but never builds a binary, starts a
service, or runs a matrix:

```text
scripts/prepare-paper-performance.sh \
  --backend rocksdb \
  --spec /absolute/path/to/formal-spec.json \
  --runtime-manifest /absolute/path/to/runtime-manifest.json \
  --dataset-evidence /absolute/path/to/dataset-evidence.json \
  --backend-evidence /absolute/path/to/backend-evidence.json \
  --gateway-build-evidence /absolute/path/to/gateway-build-evidence.json \
  --executor-bin /absolute/path/to/dtgproxy-paper-cell-executor \
  --gateway-bin /absolute/path/to/dtgproxy-gateway \
  --data-node-bin /absolute/path/to/dtgproxy-data \
  --meta-node-bin /absolute/path/to/dtgproxy-meta \
  --bolt-loadgen-bin /absolute/path/to/dtgproxy-bolt-loadgen \
  --orchestrator-bin /absolute/path/to/dtgproxy-paper-benchmark \
  --output-dir /absolute/path/to/prepared/rocksdb
```

Repeat with `--backend postgresql --output-dir /absolute/path/to/prepared/postgresql` and
`--backend neo4j --output-dir /absolute/path/to/prepared/neo4j`, using backend-specific sealed
inputs and process identities. Do not prepare a bundle that marks more than one backend available.

The prepared `formal-spec.json` is not a copy of the input: it contains the captured environment
fingerprint and is the only spec accepted for the formal invocation. Review `READY.json`, verify
`SHA256SUMS`, and execute the complete matrix exactly once. The preparation command invokes the
prebuilt orchestrator's side-effect-free `validate-spec` command before publishing `READY.json`;
it does not invoke Cargo or build during preparation. Do not use `--simulate`:

```text
scripts/run-isolated-paper-performance.sh \
  --rocksdb-bundle /absolute/path/to/prepared/rocksdb \
  --postgresql-bundle /absolute/path/to/prepared/postgresql \
  --neo4j-bundle /absolute/path/to/prepared/neo4j \
  --output-root /absolute/path/to/artifacts/paper-performance
```

The isolated runner executes RocksDB, PostgreSQL, and Neo4j in that fixed order. It verifies each
bundle and completed artifact, binds observations to the selected backend, and confirms that every
exact managed Gateway/Data Node process from the prior run has exited or changed start identity
before advancing. It never searches for or kills arbitrary processes by executable name. A failure
stops the sequence and prevents publication of a combined success package.

With formal defaults, each backend artifact remains under
`artifacts/paper-performance/<run-id>/`; the output root also receives a deterministic `combined/`
package after all three artifacts verify. The combined report preserves separate backend summaries
and never averages different backend families. If a cell fails, preserve the partial run and
failure logs; do not rerun it under the same or a replacement run ID as formal evidence.

## Offline Verification

Verification reads only artifact files. It starts no process except the verifier itself. The
regeneration directory must be new and outside the immutable artifact:

```text
CARGO_TARGET_DIR=target/paper-performance-verify \
  scripts/verify-paper-performance.sh \
  --artifact artifacts/paper-performance/<run-id> \
  --regenerate-dir target/paper-performance-regenerated/<run-id>
```

The verifier rejects regeneration inside the artifact, derives the required file set from the
manifest, and checks SHA-256 contents, strict JSON schemas, deterministic
matrix/config reconstruction, exact cell and repetition coverage, Backend/Adapter/Proxy identity,
required metrics, and statistics. It then compares stored summary and figure data byte-for-byte
with values recomputed from raw JSON and writes regenerated JSON/CSV data to the requested new
directory.

Latency and TTFR retain pooled samples for distribution plots, while inferential statistics use
each repetition's p50/p95/p99 values. Executor failures preserve raw output and `logs/failure.json`,
then seal partial evidence with `PARTIAL_SHA256SUMS`; they never emit a successful summary or seal.
