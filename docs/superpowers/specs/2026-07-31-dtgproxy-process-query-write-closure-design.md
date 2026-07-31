# DTGProxy Process Query and Write Closure Design

Status: approved under the user's continuous-execution authorization

Date: 2026-07-31

## Goal

Make the real four-process path semantically complete for the three fixed diagnostic workloads so
that Fjall, PostgreSQL, and Neo4j are measured through the same production path:

```text
persistent Bolt client -> Gateway -> Data Node -> official provider
```

The fixed workloads remain unchanged:

```text
CREATE (n:Bench {value: 1}) VALID FROM 1
MATCH (n) WHERE n.id = $id RETURN n.id       ($id = 2048)
MATCH (n) RETURN COUNT(*)
```

No benchmark-only semantic branch is permitted.

## Selected architecture

Data remains a fenced storage-fragment worker. It executes one explicitly identified storage access
and returns raw rows carrying their fragment identity. Gateway remains the global coordinator: it
binds request parameters, lowers the retained physical plan, gathers remote fragment batches, runs
the shared `dtg-query` operator DAG, validates the final schema, and returns final rows to Bolt.

The shared query runtime gains a materialized-source entry point. This entry point substitutes
`BatchOperator` sources for local `QueryStorage` sources while reusing the existing Filter, Project,
Aggregate, Sort, Limit, and Unwind implementations. It must not copy expression evaluation into the
Gateway process package.

Global `COUNT(*)` is represented as an aggregate with zero grouping keys and one non-distinct
count-star function. It emits exactly one integer row, including `0` for empty input.

Process writes use the existing distributed transaction and Shard command contracts. Gateway
normalizes the CREATE statement into deterministic logical mutations, begins the transaction,
dispatches the participant write to Data, commits, and acknowledges Bolt only after the durable
outcome. Data does not receive a benchmark-specific create command.

## Alternatives rejected

Executing the full operator DAG in Data is rejected because Filter and Project may be local but
global Aggregate and cross-fragment coordination belong to Gateway. It would also duplicate the
existing query runtime.

Provider-specific predicate and aggregate pushdown is rejected because it would measure three
different semantic paths and make backend comparisons misleading.

Deriving write throughput from direct seeding or provider calls is rejected because it bypasses
Bolt and Gateway transaction coordination.

## Read data flow

1. Bolt decodes the statement and typed parameter map.
2. Gateway compiles and plans the query with a truthful logical scan bound of 4,096.
3. Gateway binds parameters recursively in the physical expressions and rejects missing or invalid
   bindings before reporting success.
4. Gateway sends one storage access per remote fragment and retains the physical plan locally.
5. Data validates the complete storage-fragment envelope, rejects unsupported extra accesses or
   trailing malformed content, performs the fenced provider read, and returns raw rows with the
   fragment ID.
6. Gateway groups returned rows by fragment ID and runs the shared materialized-source query
   runtime.
7. Gateway validates the resulting schema against the planned result schema and converts query
   values to protocol values.
8. Bolt receives only final projected or aggregated rows.

For the point lookup, the exact result is field `n.id` with one integer value `2048`. For the count,
the exact result is one integer value `4096` after preload.

## Write data flow

1. Bolt sends the fixed auto-commit CREATE statement.
2. Gateway compiles the write and constructs a transaction using the current catalog/topology and
   provider fences.
3. Gateway maps the normalized create operation to a unique vertex identity and the canonical
   temporal mutation set.
4. Gateway dispatches the participant through the existing Data/Shard transaction RPC path.
5. Gateway commits and waits for the durable outcome.
6. Bolt receives an empty-row success summary only after commit. Abort or uncertain outcomes remain
   failures and are not counted.

Warmup writes occur in a fresh cell and are discarded with that cell, preserving isolation.

## Correctness and failure handling

- Data must fail closed on a fragment it cannot fully validate or uniquely identify.
- Gateway must reject unknown fragment IDs, duplicate terminal batches, inconsistent fields, missing
  fragments, and final schema drift.
- Point lookup validation checks the exact scalar integer `2048`, not only row count or a stable
  digest.
- Count validation checks the exact scalar integer `4096`.
- CREATE validation requires a successful durable acknowledgement and zero returned rows.
- Any Bolt, planning, binding, transport, transaction, or semantic validation error invalidates the
  cell and prevents combined summary publication.

## Lifecycle isolation

Each cell owns four unique DTGProxy processes and unique storage identities. Ports are reserved by
bound listeners until immediately before the corresponding child starts, with duplicate addresses
rejected inside the cell. PostgreSQL and Neo4j use one disposable container at a time; the current
container is stopped and removed before the next backend starts. Cleanup targets only recorded child
and container handles.

## Testing strategy

Implementation follows red-green-refactor in these independently reviewed increments:

1. Exact `COUNT(*)` parsing/normalization and global aggregate semantics.
2. Parameter binding plus materialized remote-source execution in the shared query runtime.
3. Process Gateway fragment preservation, operator execution, and fail-closed Data fragment
   validation.
4. Real process-mode CREATE transaction dispatch.
5. Exact Bolt correctness gates and collision-safe four-process port lifecycle.
6. Fjall live certification, then isolated PostgreSQL and Neo4j certification.
7. Full 54-cell release-mode diagnostic, artifact recomputation, checksum validation, and retirement
   audit.

The final measurement is publishable only after every increment receives an independent review with
no unresolved Critical or Important findings.
