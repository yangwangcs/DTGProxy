# Task 4B: Current Cypher 25 named procedures and `CALL ... YIELD` report

## Result

Task 4B now has one typed, current-only execution path:

```text
Cypher 25 parser -> semantic catalog resolution -> Temporal IR -> physical/distributed plan
                 -> bounded procedure runtime -> ordinary typed HTTP/Bolt rows
```

Alternate query/IR namespaces, placeholder procedure execution, the Gateway analytics JSON short
circuit, raw argument reparser, and query API compatibility paths are absent. External protocol and
persistence identifiers retain their formal names when those names are part of the live contract;
they do not select an older DTGProxy implementation.

## RED evidence

### Structured syntax, semantics, and row composition

The initial implementation stored procedure arguments as raw text, represented only an incomplete
YIELD list, assigned every procedure `QueryEffect::Procedure`, lowered a hash-only placeholder, and
returned `UnsupportedOperator("Procedure")` from the executor. Gateway separately reparsed `dtg.*`
calls and returned one JSON string column. Consequently the new owning tests initially failed to
compile or failed on missing structured fields/catalog APIs; the end-to-end query could not execute
its following `WITH`/`WHERE`/`RETURN` clauses.

The tests introduced for that RED state cover:

- positional, map, parameter, nested collection arguments, `YIELD *`, aliases, and duplicate output
  rejection;
- unknown procedure/input/output, missing/extra/type-invalid input, permission denial, and unstaged
  write-procedure rejection;
- correlated slots, exact provider schema, catalog/procedure revision identity, and invalid plan
  rejection;
- source-free and per-upstream-row execution, normal row composition, and typed Bolt output;
- fixed BEGIN snapshot and staged graph overlay visibility.

### Provider output and execution fences

Malformed provider columns, values, row counts, and byte counts were initially able to bypass the
ordinary query pipeline because Gateway serialized the provider result directly. The new provider
and executor tests were RED until validation moved into a catalog-budgeted output sink and happened
before each row was retained. Cancellation, deadline, memory, invocation, and output limits were
added at the same boundary.

### Independent input-row limit remediation

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p query-executor --test procedures \
  procedure_input_row_limit_is_independent_and_fails_before_provider_invocation
```

Effective RED result: exit 101. Rust reported that `ResolvedProcedure::new` accepted only 12
arguments and that `RuntimeError::ProcedureInputRowLimit` did not exist. Although
`ProcedureLimits` declared `max_input_rows`, the compiler did not carry it into Temporal IR and the
executor incorrectly used only `max_invocations` for incoming rows.

After adding the field through catalog -> compiler -> IR validation -> physical operator ->
executor, the focused test is GREEN: 1 passed, 0 failed. With `max_invocations = 10` and
`max_input_rows = 1`, two input rows fail before invocation and the provider call counter remains
zero.

An earlier invocation of this RED command omitted the required LLVM environment and stopped while
building RocksDB because the default C++ compiler could not find `cstdint`; it did not reach Rust
compilation and is not counted as semantic RED evidence.

### Bounded projection, provider isolation, and request fences

The independent boundary review found three remaining unbounded transitions. Graph projection
limits were not catalog identity, provider code ran synchronously on the async coordinator thread,
and built-in analytics constructed a complete `AlgorithmResult` before procedure limits inspected
its rows. Request Unix deadlines also reached shard workers but were not installed on coordinator
procedure contexts.

Focused RED evidence:

- `procedure-runtime` catalog compilation failed with five missing graph-limit methods. The new
  catalog test requires nonzero vertex, edge, and graph-byte limits and proves changing one changes
  both catalog and procedure revisions.
- The incremental-output test failed with 13 compile errors: no `ProcedureOutput`, the provider
  trait returned a complete result, registry invocation was not a future, and no worker limit API
  existed.
- The query cancellation and coordinator deadline tests failed because the executor and coordinator
  treated the registry future as a synchronous result. The analytics early-stop test failed because
  `AnalyticsOutput`/`execute_into` did not exist. The Cypher deadline test failed because
  `EngineError::DeadlineExceeded` did not exist.

The focused tests are GREEN after adding catalog-owned graph budgets, storage-scan entry/byte
preflight, an incremental typed output sink, bounded `spawn_blocking` isolation, async cancellation
notification, and monotonic deadline contexts. A canceled caller drops its join future promptly,
but the owned semaphore permit remains inside the blocking closure until the provider actually
exits. The analytics provider now stops on the first rejected output row; no result row crosses the
worker boundary before procedure limits accept it.

The physical-plan regression exposed while running the full suite was also closed at its authority:
`Union` requires equal input schemas, `HashJoin` accepts exactly two independently typed schemas,
and other multi-input operators fail closed. The two interval HashJoin coordinator tests, all eleven
physical-plan tests, and physical-plan doc tests are GREEN.

## GREEN evidence

Focused and owning-suite results after implementation:

```text
cypher-syntax parser profiles             9 passed, 0 failed
cypher-sema procedure                      3 passed, 0 failed
cypher-compiler compile                   20 passed, 0 failed
temporal-ir validation                     5 passed, 0 failed
procedure-runtime catalog/invocation      17 passed, 0 failed
query-executor procedures                  8 passed, 0 failed
query-optimizer partitioning               6 passed, 0 failed
distributed-query procedures               2 passed, 0 failed
cypher-engine end-to-end                  29 passed, 0 failed
cypher-engine latest surface               1 passed, 0 failed
gateway-node cross-shard service           1 passed, 0 failed
```

The compiler suite includes a focused `pageRank({damping: $damping})` test and proves the map value
lowers directly to `ScalarExpr::Parameter`, without retaining or reparsing source text.

The Gateway integration exercises normal typed HTTP and Bolt rows, a following query pipeline, a
fixed explicit-transaction snapshot, a staged vertex visible to degree analytics, and an external
post-BEGIN commit that remains invisible. PrimaryReplica and SharedNothing return equivalent typed
procedure rows over equivalent projections.

Required command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p analytics-api -p analytics-runtime -p procedure-runtime -p cypher-syntax \
  -p cypher-sema -p cypher-compiler -p temporal-ir -p physical-plan -p query-executor \
  -p query-optimizer -p distributed-query -p cypher-engine -p gateway-node
```

Result before final formatting: GREEN, exit 0, with every selected unit, integration, process, and
doc test passing. A fresh post-format run is recorded in Final verification.

## Implementation notes

- `procedure-runtime` is the authority for names, exact inputs/outputs, catalog and procedure
  revisions, permissions, effects, placement, supported profile, overlay capability, and limits.
- Built-in procedure descriptors derive from the same `analytics-api::AlgorithmDescriptor` values
  used by `BuiltInProvider`; compiler and provider schemas cannot drift independently.
- The AST owns structured `Expression` arguments and `ProcedureYield`; Gateway never reparses CALL
  source. Semantic analysis binds exact YIELD names/types into ordinary lexical scope.
- Temporal IR carries resolved identity/revisions, typed arguments, selected provider columns,
  output slots, effect, placement, overlay capability, and every declared row/byte limit.
- A source-free procedure receives the single `Argument` row and runs once. A correlated procedure
  runs once per upstream row, preserving existing bindings and appending selected YIELD values.
- Registry preflight validates and default-injects runtime inputs before projection or provider
  admission. Provider columns and each output row are type/width/row/byte checked before retention;
  the executor additionally enforces aggregate input, invocation, fragment memory, deadline, and
  cancellation fences across correlated calls.
- Global analytics is coordinator-owned in both deployment modes. No source-free call is multiplied
  by shard count, and no source Cypher or backend query string exists in a provider request.
- Explicit transactions reuse the BEGIN graph/schema/topology identities and transaction timestamp.
  Snapshot projection merges staged puts/deletes by `ElementRef`; event procedures fail closed when
  an overlay is present.
- HTTP and Bolt use the normal `cypher_result` schema and typed row encoder. No analytics-only JSON
  response remains.

## Self-review

- **Typed schemas:** parser, semantic descriptor, IR output slots, provider schema validation, and
  Bolt fields all use the same exact types. Vertex identifiers are exposed as strings so `u128`
  values are lossless.
- **Identity authority:** query fingerprints include catalog revision; registry invocation validates
  authority key plus catalog/procedure revisions. Invalid and stale identities fail closed.
- **Snapshot and overlay:** projection is fenced to the query timestamp; staged replacements and
  deletes are applied before analytics. Dangling projected edges are removed after vertex deletion.
- **Malformed providers:** wrong column order/name, width, nullability/type, row bound, value bytes,
  or result bytes fail before any batch becomes visible.
- **Async safety:** synchronous provider code runs only in a bounded blocking pool. The owned worker
  permit remains with the blocking closure after caller cancellation, preventing abandoned work
  from reopening capacity. Query cancellation and deadlines race the join future and return without
  waiting for provider exit.
- **Limits:** invocation count and input rows are independent; provider output rows/value bytes/result
  bytes and fragment memory are checked before retention or publication.
- **Deployment equivalence:** global procedures gather and execute on the coordinator in both
  PrimaryReplica and SharedNothing, producing canonical-equivalent typed rows.
- **Latest-only:** scans find no alternate query/IR namespace, `QueryEffect::Procedure`,
  `execute_analytics_call`, `parse_algorithm_call`, `analytics_result`, `is_procedure()`, or raw
  `arguments: String` path. Only the current top-level implementation is compiled.

## Deliberate 1.0 boundaries

- In-process provider code cannot be force-killed safely. A provider that never returns keeps one
  bounded worker permit until process shutdown, while its canceled/deadline-exceeded caller returns
  promptly and cannot admit replacement work into that occupied slot.
- Event analytics with a nonempty transaction overlay fails closed. Snapshot analytics supports the
  staged overlay required by this task.
- Built-in analytics remains coordinator-only. Shard-local placement is represented but requires a
  separately proven partition contract before registration.
- Write-effect procedures without existing transaction staging support are rejected. No placeholder
  success or direct backend mutation path exists.
- Structured `CALL {}`/EXISTS/COUNT and `IN TRANSACTIONS` are Task 4C, not a hidden compatibility
  branch in Task 4B.

These are declared product boundaries, not incomplete Task 4B code paths.

## Hygiene

```text
cargo fmt --all -- --check
# exit 0

git diff --check
# exit 0

latest-only forbidden-symbol and versioned-path scans
# zero internal matches; only current external protocol `.proto` filenames are versioned
```

## Final verification

The required 13-package command completed GREEN with exit 0: 253 passed, 0 failed, 0 ignored, and
all documentation tests passed. It includes 20/20 compiler tests, 29/29 engine end-to-end tests,
8/8 procedure executor tests, 7/7 registry invocation tests, the running-provider cancellation and
coordinator deadline tests, the cross-deployment procedure equivalence test, the latest-only
surface guard, and the Gateway cross-shard transaction/procedure integration test. The only warning
from that run was a private conversion function made unreachable by the current authority path; it
was removed, and a subsequent `cargo check -p cypher-compiler` completed with exit 0 and no warning.

## Files changed

- `Cargo.toml`
- `Cargo.lock`
- `.superpowers/sdd/task-4b-procedure-yield-report.md`
- `crates/analytics-api/src/lib.rs`
- `crates/analytics-api/tests/descriptors.rs`
- `crates/analytics-runtime/src/lib.rs`
- `crates/analytics-runtime/src/provider.rs`
- `crates/analytics-runtime/tests/projection.rs`
- `crates/analytics-runtime/tests/provider.rs`
- `crates/cypher-ast/src/clause.rs`
- `crates/cypher-ast/src/lib.rs`
- `crates/cypher-syntax/src/parser.rs`
- `crates/cypher-syntax/tests/parser_profiles.rs`
- `crates/cypher-sema/Cargo.toml`
- `crates/cypher-sema/src/analyzer.rs`
- `crates/cypher-sema/src/lib.rs`
- `crates/cypher-sema/tests/procedure.rs`
- `crates/cypher-compiler/Cargo.toml`
- `crates/cypher-compiler/src/compiler.rs`
- `crates/cypher-compiler/tests/compile.rs`
- `crates/temporal-ir/src/lib.rs`
- `crates/temporal-ir/src/logical.rs`
- `crates/temporal-ir/src/procedure.rs`
- `crates/temporal-ir/tests/validate.rs`
- `crates/physical-plan/src/lib.rs`
- `crates/procedure-runtime/Cargo.toml`
- `crates/procedure-runtime/src/lib.rs`
- `crates/procedure-runtime/tests/catalog.rs`
- `crates/procedure-runtime/tests/invocation.rs`
- `crates/query-executor/Cargo.toml`
- `crates/query-executor/src/context.rs`
- `crates/query-executor/src/error.rs`
- `crates/query-executor/src/executor.rs`
- `crates/query-executor/src/expression.rs`
- `crates/query-executor/src/lib.rs`
- `crates/query-executor/tests/procedures.rs`
- `crates/query-optimizer/src/lib.rs`
- `crates/query-optimizer/tests/partitioning.rs`
- `crates/distributed-query/Cargo.toml`
- `crates/distributed-query/src/coordinator.rs`
- `crates/distributed-query/src/lib.rs`
- `crates/distributed-query/src/worker.rs`
- `crates/distributed-query/tests/procedures.rs`
- `crates/cypher-engine/Cargo.toml`
- `crates/cypher-engine/src/lib.rs`
- `crates/gateway-node/Cargo.toml`
- `crates/gateway-node/src/service.rs`
- `crates/gateway-node/tests/service.rs`

## Independent-review remediation: write/procedure atomicity

The independent review identified that a read procedure mixed with a write could be accepted while
the write path executed only the prefix before the first write. It also identified that
`write_staging` was a caller-controlled Boolean rather than a typed staging implementation.

### RED

`cargo test -p cypher-sema --test procedure` failed 2 of 3 tests. Both
`CREATE ... CALL readProcedure` and `CALL readProcedure ... CREATE` were accepted as write queries,
and a write-effect procedure declared with `write_staging = true` was accepted. The failures showed
the resulting `AnalyzedQuery` values instead of the required stable errors.

### GREEN

- Removed `write_staging` from `ProcedureDefinition`, `ProcedureDescriptor`, catalog construction,
  and descriptor hashing. There is no Boolean escape hatch in the current API.
- All write-effect procedures fail with `DTG-CYPHER-PROCEDURE-WRITE-UNSUPPORTED` until a typed
  candidate/revision-fenced staging implementation exists.
- Any statement that combines a read procedure with a write clause in either order fails semantic
  analysis with `DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED`, before compilation, timestamp allocation,
  projection, or publication.
- The cross-shard Gateway test submits both orderings, verifies the stable rejection, then queries
  both labels and observes count zero. The first run exposed duplicate numeric request IDs in the
  long test fixture; the TSO correctly rejected the replay. Assigning unused request IDs fixed the
  fixture without changing product code.

Verification:

```text
cargo test -p procedure-runtime -p cypher-sema -p cypher-compiler
# GREEN: compiler 22, sema 19, procedure-runtime 5; zero failures

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p gateway-node --test service \
  remote_gateway_commits_and_queries_a_cross_shard_temporal_transaction
# GREEN: 1 passed, 0 failed

rg 'write_staging|with_write_staging|PROCEDURE-WRITE-NOT-STAGED' crates --glob '*.rs'
# zero matches
```

## Independent-review remediation: row cardinality, physical contract, and YIELD identifiers

### RED

- `procedure_without_yield_preserves_provider_row_cardinality` observed one output row when the
  provider returned two rows; the zero-row case would likewise have produced one row.
- Physical-plan tests showed that a procedure with `max_input_rows = 0`, duplicate argument names,
  an unknown argument slot, or an extra output column could be accepted.
- ``YIELD degree AS `my score``` failed with `DTG-CYPHER-INVALID-YIELD` because the parser split raw
  text on whitespace.

The first query-executor RED attempt omitted the required LLVM environment and failed while building
RocksDB; it is not semantic evidence. The rerun with the required environment reached the focused
test and failed with actual row count 1 versus expected 2.

### GREEN

- Procedure execution now iterates every provider row whether or not any YIELD columns are selected.
  It preserves one upstream row per provider row and filters the upstream row when the provider
  returns none.
- Physical validation tracks the current schema through each fragment, seeds coordinator input from
  incoming exchanges, validates every argument slot/name and nonzero input bound, preserves the
  complete input schema, and permits exactly the appended YIELD bindings with matching type,
  nullability, slot, and order.
- YIELD uses the existing Cypher lexer and accepts both ordinary and escaped identifiers; raw
  whitespace splitting is gone.
- Correlated executor test fragments now explicitly declare their injected input schema with an
  identity Project, matching the contract that production fragments obtain from exchanges.

Verification:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-syntax -p temporal-ir -p physical-plan -p query-executor
# GREEN: all unit, integration, and doc tests passed; zero failures
```

## Second independent-review remediation: nested authority, retention, storage, and schemas

### RED

- Semantic analysis classified an outer write containing a nested named procedure as
  `AnalyzedQuery { effect: Write, procedures: [] }`, so the write/procedure exclusion could be
  bypassed through `CALL {}`.
- Composed procedure output charged memory only after retaining rows. With two upstream rows and a
  ten-row provider fanout, the provider was invoked twice even though the first invocation already
  exceeded the fragment memory budget.
- Procedure preflight skipped every static argument as soon as any correlated row-slot argument was
  present. Oversized and missing static parameters therefore returned `Ok(())` before graph
  projection.
- Storage scans had an entry-count bound but no end-to-end byte bound. Adapter, sidecar, data-node,
  batch decoder, and remote shard error contracts could retain an oversized row or degrade the
  failure to an untyped backend/internal error.
- HashJoin validation substituted the fragment's declared final output for a derived join schema.
  It could not reject missing join keys, malformed output order/types, or non-nullable right-side
  columns on a left join, and a following procedure did not receive an authoritative input schema.
- All six invalid procedure definition cases returned a catalog: empty and duplicate input names,
  empty and duplicate output names, an incompatible default, and a null default on a non-nullable
  field. The catalog revision was computed before these definitions were validated.

The first remote TCK attempt omitted the required LLVM C++ environment and failed while compiling
RocksDB; it is environment evidence only. The LLVM rerun reached the intended RED contract: the
typed `ShardClientError::ScanByteLimit` variant did not exist. The first physical-plan RED compiled
only after adding the expected error variants; the production validator still lacked the required
derivation. The catalog RED executed all six cases and observed six erroneous `Ok` catalogs.

### GREEN

- Outer semantic analysis owns child subquery analyses and merges their effects and procedures.
  Mixed nested write/procedure statements are rejected before compilation or publication; pure
  nested procedure and write subqueries fail closed until Task 4C supplies an executable child-plan
  contract. The Gateway integration proves both clause orderings leave durable label counts at zero.
- Procedure composition estimates and charges each complete retained row before `push`; it stops
  before a later provider invocation once the current invocation exhausts fragment memory.
- Partial preflight defers only explicitly row-dependent argument names. Every other argument still
  receives default injection, required/type validation, and recursive value/result byte checks
  before graph projection. Runtime invocation repeats full preflight.
- The single current `KeySpan` now owns `max_bytes`. Memory, RocksDB, PostgreSQL, Neo4j, sidecar,
  temporal storage, data-node scan batches, and shard clients charge key plus value bytes before
  retention. Remote exhaustion is preserved exactly as
  `AdapterError::ScanByteLimit -> HostError::ScanByteLimit -> ResourceExhausted metadata ->
  ShardClientError::ScanByteLimit -> AdapterError::ScanByteLimit`; the owning TCK asserts
  `{ limit: 1, required: 25 }` at both client layers.
- HashJoin derives its schema from exactly two ordered exchange inputs. Keys must be unique and
  present with equal types on both sides; overlapping non-key slots fail closed; left joins make
  right-only columns nullable. Every fragment's final derived schema must equal its declared output,
  and a following Procedure is validated against the derived join schema.
- `ProcedureCatalog::from_definitions` validates all names, duplicates, default types, and default
  nullability before computing any catalog revision, procedure revision, or authority key.
- The stricter final-schema contract exposed one executor limit fixture whose Finish-only fragment
  declared an injected column without an input-producing operator. The fixture now uses the same
  explicit identity Project as other correlated-input tests; production validation was not relaxed.

Focused verification:

```text
remote shard byte-limit TCK                 1 passed, 0 failed
physical-plan validation                   11 passed, 0 failed
distributed-query interval coordinator      3 passed, 0 failed
procedure-runtime catalog                   10 passed, 0 failed
query-executor limits                        2 passed, 0 failed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p storage-api -p adapter-memory -p adapter-rocksdb -p adapter-sidecar \
  -p data-node -p shard-client -p temporal-storage
# GREEN: exit 0; all selected unit, integration, process, and doc tests passed
```

## Third independent-review remediation: pre-allocation budgets and backend retention

### RED

- The composed-row memory check happened before `push`, but only after cloning the upstream and
  provider values into an owned row. One oversized provider value could therefore allocate before
  the fragment budget rejected it.
- Static argument preflight treated an entire expression as row-dependent when it contained any
  slot. Expressions such as `slot + $static` therefore skipped missing-parameter and recursive-byte
  validation before graph projection.
- PostgreSQL `transaction.query`, Neo4j's generic JSON `Value` response, and sidecar prost decoding
  each materialized a complete scan response before enforcing the scan byte budget. Sidecar also
  reconstructed remote exhaustion as `limit + 1` instead of preserving the exact required size.
- Final-schema validation exposed three interval executor fixtures that declared externally injected
  input without an input-schema-producing operator. A repository-wide test scan found four more
  fixtures with the same implicit contract.

### GREEN

- `estimate_composed_procedure_row` borrows both the upstream `RuntimeValue` slice and provider
  `ProcedureValue` slice, uses the authoritative `ProcedureValue::estimated_bytes`, and checks the
  complete required size before any clone. The single-row regression returns exactly
  `MemoryLimitExceeded { limit: 32, required: 1029 }`.
- `ScalarExpr::visit_parameters` recursively visits parameters independently of slots. Missing and
  oversized parameters inside mixed row/static expressions now fail before TSO allocation or graph
  projection; only the row-dependent evaluation itself is deferred.
- PostgreSQL uses `query_raw` plus `FallibleIterator` and charges each key/value before constructing
  and retaining `KeyValue`. Neo4j applies a checked `Read::take` response cap and a row-at-a-time
  serde visitor, charging decoded key/value bytes before retention. Sidecar bounds the scan frame
  from its header before body allocation, decodes the repeated protobuf rows one at a time, stops
  before decoding a malicious truncated second row once the first row exceeds budget, and carries
  exact `scan_limit`/`scan_required` fields over the wire.
- Seven no-incoming test fragments now begin with an explicit identity Project: five interval-row
  fixtures, one Filter fixture, and one Finish fixture. Existing procedure/limit fixtures were
  already explicit; source-producing scans, source-free Argument pipelines, and fragments with
  incoming exchanges were left unchanged. Production optimizer plans were not modified and the
  final-schema validator was not relaxed.
- The authority refactor left one private `ValueType -> CypherType` converter unused. The complete
  function was deleted after an all-call-site scan; `cargo check -p cypher-compiler` is warning-free.

Fresh verification after these changes:

```text
query-executor focused fixture suites      21 passed, 0 failed
physical-plan + query-executor full        59 passed, 0 failed
adapter-sidecar full                       24 passed, 0 failed
backend/storage affected suite            152 passed, 0 failed, 4 live tests ignored
procedure/IR/executor/memory/RocksDB        95 passed, 0 failed
required 13-package full                  252 passed, 0 failed, 0 ignored
cargo check -p cypher-compiler             exit 0, no warnings
cypher-compiler full after cleanup         22 passed, 0 failed
```

The four ignored live tests require disposable external services: one Neo4j instance and three
PostgreSQL cases. Their deterministic unit/contract paths passed, including exact scan-byte errors;
the absence of external database credentials is recorded rather than counted as execution evidence.

## Final independent-review remediation: Neo4j response-body limits

### RED

- The default procedure projection budget permits 1,000,000 rows and 128 MiB of decoded graph data.
  `neo4j_scan_body_limit` converted those independent logical limits into a roughly 320 MiB
  theoretical JSON bound and rejected the scan with `AdapterError::Backend` before issuing the HTTP
  request, even when Neo4j would return an empty or small result.
- When the HTTP reader actually consumed one byte beyond its 64 MiB body cap, the decoder returned
  an untyped backend string. Reusing `ScanByteLimit` would have falsely described wire JSON bytes as
  retained key/value bytes and fabricated a graph-data `required` value.
- The first typed propagation implementation collapsed the new response-body error into
  `TemporalStoreError::ScanByteLimit`, losing its exact limit, required count, and byte domain. The
  final review required these fields and units to survive through snapshot and event projection.

The Neo4j, Sidecar, TemporalStore, and analytics RED tests all failed to compile on the missing
typed variants/fields. This established the intended API before production changes. The Neo4j
behavior test additionally captured the old request-time `Backend` rejection.

### GREEN

- Neo4j now computes the conservative JSON bound with saturating arithmetic and uses
  `min(calculated, 64 MiB)`. The default 1,000,000-row/128-MiB span therefore executes with a 64 MiB
  reader cap, and an empty canonical response decodes successfully.
- `decode_scan_body` reads at most cap plus one. Only an actually consumed extra byte returns
  `AdapterError::ScanResponseByteLimit { limit, required }`; the owning test reads byte 67,108,865
  and observes `{ limit: 67,108,864, required: 67,108,865 }`. No graph scan-byte value is invented.
- Sidecar error code 10 and dedicated protobuf fields preserve `scan_response_limit` and
  `scan_response_required` independently of graph `scan_limit`/`scan_required`. Canonical decoding
  rejects ambiguous errors that claim both byte domains, and the client restores the exact
  `AdapterError::ScanResponseByteLimit` without parsing strings.
- `TemporalStoreError::ScanResponseByteLimit` and
  `ProjectionError::ResponseByteLimit` retain both numbers and explicitly identify wire response
  bytes in `Display`. Bounded and unbounded snapshot/event projection paths use the typed mapping;
  ordinary retained graph bytes continue to use `ScanByteLimit`/`ByteLimit`.

Final verification:

```text
Neo4j/Sidecar/storage/projection focused     7 passed, 0 failed
related five-package full                 115 passed, 0 failed, 1 live test ignored
data-node + shard-client cargo check        exit 0
required 13-package final                 253 passed, 0 failed, 0 ignored
all selected documentation tests            passed
```

The ignored Neo4j live test requires external credentials and a disposable database. The local
decoder, protocol, adapter, storage, projection, and all required integration paths executed.
