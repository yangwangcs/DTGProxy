# Task 4B: Complete current Cypher 25 named procedures and CALL YIELD

## Context

The current parser records a shallow named `CALL` descriptor and the compiler emits a placeholder
procedure operator. The query executor rejects that operator. Gateway then bypasses the complete
row pipeline, accepts only `dtg.*`, invokes `BuiltInProvider`, and returns one JSON string column.
As a result, YIELD fields, aliases, following WITH/RETURN, fixed-snapshot transaction visibility,
typed schemas, permissions, resource limits, and normal cancellation semantics are not real.

Replace that bypass with one current-only typed procedure path. Reuse `analytics-api` and
`analytics-runtime::BuiltInProvider` as the first implementation behind the procedure boundary;
do not hard-code provider behavior into the compiler or executor.

## Required behavior

1. Add the current `procedure-runtime` ownership boundary (crate or an equally isolated current
   module matching the approved dependency graph). It owns authoritative `ProcedureDescriptor`,
   catalog/registry lookup, input parameter schema/defaults, output `RowSchema`, effect,
   determinism, permissions, execution placement, supported Cypher profile, and bounded invocation.
   A stable hash alone is not an authority boundary; cache/plan identity includes catalog revision.
2. Represent procedure arguments as structured Cypher expressions, not raw text reparsed by
   Gateway. Support positional arguments, one map argument used by the current analytics catalog,
   parameters, nested lists/maps within existing expression limits, `YIELD field`, `YIELD field AS
   alias`, and `YIELD *`. Reject malformed, duplicate, unknown, missing, extra, or type-incompatible
   arguments/output fields before timestamp allocation or provider invocation.
3. Semantic analysis resolves every named procedure against the catalog and binds exact typed YIELD
   columns into lexical scope. Following WHERE/WITH/UNWIND/RETURN uses those ordinary bindings.
   Procedure effect composes with the surrounding query; a read procedure remains a read query.
4. Enrich the current Temporal IR and physical operator with resolved descriptor identity/revision,
   typed argument expressions, input/output slot mapping, declared placement/effect, and exact
   output schema. Validate all input slots, identities, and bounds.
5. Execute the procedure as a normal bounded row operator. It runs once per upstream row, or once
   for source-free Argument input; preserves upstream columns and appends selected YIELD values.
   Provider output columns/types/cardinality are validated before any row becomes visible.
6. For 1.0, execute registered global analytics procedures on the coordinator. A descriptor may
   request shard-local placement only when the implementation and partition contract explicitly
   support it. Do not multiply source-free procedures by shard count.
7. Adapt `BuiltInProvider`/`AlgorithmResult` to typed procedure rows. Delete the Gateway whole-query
   JSON result short circuit and raw `parse_algorithm_call`; HTTP/Bolt return normal field names and
   typed rows, and following clauses execute. Never send procedure source or Cypher to a backend.
8. A read-only procedure inside an explicit Bolt transaction uses the BEGIN routing/schema/topology
   and transaction snapshot plus the current graph overlay. It cannot allocate a fresh snapshot.
   Procedures with write effects are rejected before execution unless they stage all effects through
   the existing candidate/revision-fenced transaction path; no write provider is required for 1.0.
9. Enforce descriptor permission/capability, security fingerprint, deadline, cancellation,
   invocation count, input/output row count, value bytes, provider result bytes, and fragment memory
   limits before retention. Provider errors expose stable DTG/Cypher errors, not backend strings.
10. Keep only the current Cypher 25/Temporal IR/executor path. No versioned namespace, legacy
    adapter, Gateway JSON compatibility path, synchronous blocking inside Tokio, backend Cypher, or
    placeholder success is permitted.

## Initial stable catalog

Register the algorithms currently exposed by `BuiltInProvider` as `dtg.*` read-only procedures.
Each descriptor must derive its exact input/output schema from the corresponding
`AlgorithmDescriptor`; requested YIELD fields select/alias that schema. `dtg.graph.degree` must at
minimum return typed `vertexId` and `degree` columns through the normal row pipeline.

## Required TDD coverage

Record actual RED and GREEN evidence for at least:

- structured positional/map/parameter arguments, `YIELD *`, aliases, and duplicate alias rejection;
- semantic rejection of unknown procedure, unknown YIELD field, missing/extra/type-invalid input,
  permission denial, and illegal effect ordering;
- compiler/IR validation of correlated input slots, descriptor revision, exact output schema, and
  unknown slots/schema mismatch;
- source-free procedure executes once; correlated procedure executes once per upstream row and
  preserves upstream bindings;
- provider output with a wrong column/type/row bound fails before exposing partial rows;
- deadline, cancellation, memory, invocation, and output-size bounds;
- Bolt `CALL dtg.graph.degree(...) YIELD vertexId, degree AS score WITH ... RETURN ...` returns typed
  fields/rows rather than one JSON string;
- a read procedure in an explicit transaction sees the fixed BEGIN snapshot and staged overlay, or
  fails closed if its descriptor cannot support overlay projection; it must not allocate a new
  transaction snapshot;
- PrimaryReplica and Shared-Nothing return canonical-equivalent procedure rows over equivalent
  projections;
- no provider request contains source Cypher and no backend receives a query string.

Run at minimum:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p analytics-api -p analytics-runtime -p procedure-runtime -p cypher-syntax \
  -p cypher-sema -p cypher-compiler -p temporal-ir -p physical-plan -p query-executor \
  -p query-optimizer -p distributed-query -p cypher-engine -p gateway-node
```

Use `rustfmt --edition 2024` and `git diff --check`. Do not commit. Append RED/GREEN evidence,
implementation notes, self-review, and concerns to
`.superpowers/sdd/task-4b-procedure-yield-report.md`.
