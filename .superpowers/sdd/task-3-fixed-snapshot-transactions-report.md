# Task 3: Fixed-snapshot transactions report

## RED evidence

### Source-stage replacement/suppression before aggregate

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p gateway-node --test service -- --nocapture
```

Result: RED (exit 101). The BEGIN-after-external-commit assertion passed, then the committed-node
update/count assertion failed at `crates/gateway-node/tests/service.rs:878` with:

```text
Gateway query error: Cypher query engine failed:
OverlayExecutionUnsupported("aggregate over an updated or deleted committed element")
```

This is the expected failure of the result-stage `merge_overlay_rows` path: it cannot replace a
committed source row before Filter/Aggregate.

An initial run also exited 101 earlier with `element identity metadata changed`; that was a test
fixture collision caused by two anonymous CREATE bindings sharing the deterministic `__vertex_0`
identity. Naming both test bindings uniquely removed the fixture error before the semantic RED run.

### Cross-partition staged relationship adjacency

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p query-executor --test graph_overlay cross_partition_staged_edge_reaches_both_endpoint_expansion_shards_without_scan_duplicates -- --nocapture
```

Result: RED (exit 101). The source-owner outgoing expansion returned one row, while the
destination-owner incoming expansion returned zero (`left: 0`, `right: 1` at
`crates/query-executor/tests/graph_overlay.rs:204`). This proved that filtering an overlay only by
the edge's canonical owner shard loses the destination-side adjacency.

### Additional pre-semantic fixture RED

The first complete `graph_overlay` run exited 101 because the test used edge id 10 as the memory
adapter log index after vertex indices 1 and 2 (`NonContiguousLogIndex { expected: 3, actual: 10 }`).
The fixture was corrected to log index 3 before recording semantic GREEN results.

## GREEN evidence

### Focused overlay, interval, staging, and Gateway tests

Commands and results:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p query-executor --test graph_overlay
# exit 0: 5 passed, 0 failed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p cypher-engine --test end_to_end interval_query_rejects_a_nonempty_transaction_overlay_instead_of_omitting_it
# exit 0: 1 passed, 0 failed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p gateway-node --lib pending_statement_tests
# exit 0: 3 passed, 0 failed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p gateway-node --test service
# exit 0: 1 passed, 0 failed
```

The Gateway integration test covers: an external commit after BEGIN remaining invisible; staged
create/read/aggregate/ROLLBACK; committed-node update/delete affecting predicate and count without
duplicates; later write MATCH of a staged node; and reuse of the same variable name without
bypassing the later statement's label/property predicate.

### Required full suite

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p temporal-storage -p query-executor -p distributed-query -p cypher-engine -p gateway-node -p dtgproxy
```

Result: GREEN (exit 0). All selected package unit tests, integration tests, and doc tests passed
with zero failures. This includes the new 5 query overlay tests, 17 Cypher end-to-end tests, 8
Gateway unit tests, the Gateway remote service integration test, 9 distributed transaction tests,
and all temporal-storage tests.

Formatting and whitespace verification:

```text
rustfmt --edition 2024 --check <all Task 3 Rust files>
# exit 0

git diff --check
# exit 0
```

## Implementation notes

- Added a bounded `GraphOverlay` carrying canonical `ElementRef`, exact valid interval,
  replacement/delete state, canonical scan owner shard, and endpoint adjacency shards.
- `ExecutionContext` carries the overlay. Local workers select their shard without discarding
  broadcast lookup data. NodeScan and RelationshipScan apply only canonical-owner entries, while
  Expand uses endpoint adjacency placement and broadcast staged vertex lookup. This preserves
  deterministic canonical scans and supports cross-partition OUT/IN expansion without duplicate
  RelationshipScan rows.
- Point reads choose only the last staged entry whose valid interval covers the query valid time.
  Disjoint interval metadata remains in the overlay. Interval execution with a non-empty overlay
  now fails explicitly with `IntervalOverlayUnsupported` rather than returning a false committed-
  only result; full region composition remains for the later TemporalRow task.
- Removed `CypherQueryResponse::merge_overlay_rows`, its aggregate arithmetic, row append,
  re-projection graph, tests, and error variants. No `overlay_rows` fallback remains.
- Removed transaction-wide `staged_bindings`. Every write prefix runs its MATCH projection against
  the fixed BEGIN snapshot plus source-stage graph overlay, and materialization receives only that
  statement's result bindings.
- Pending statement publication is candidate based. Before mutation of pending state, the Gateway
  validates row count, canonical-merges all existing and candidate scoped temporal transactions,
  checks the global operation/constraint item count, rejects conflicting constraint owners, and
  stages a cloned graph overlay. Only then are candidate overlay and prepared writes published.
- BEGIN routing/schema/topology, `start_ts`, `commit_ts`, and transaction id remain fixed. Every
  prepared row uses the same `TransactionContext`, and COMMIT continues to group the complete write
  set into the existing single-shard fast path or home-shard 2PC exactly once.
- Full ROLLBACK removes the pending transaction, including its graph overlay and prepared writes.

## Self-review

- Confirmed no references remain to `merge_overlay_rows`, `append_overlay_rows`, `overlay_rows`,
  `staged_bindings`, or `OverlayExecutionUnsupported` in the scoped crates.
- Confirmed staged edge replacement/delete is distributed to both endpoint expansion shards while
  RelationshipScan remains visible only at the canonical owner.
- Confirmed staged vertices are available for Expand endpoint lookup on a different worker but are
  emitted by NodeScan only on their canonical owner.
- Confirmed statement limit/constraint errors occur before both `pending.graph_overlay = candidate`
  and `pending.writes.extend(prepared)`.
- Confirmed the existing final commit rejects mixed contexts and the new test proves statement
  contexts share transaction id, start timestamp, and commit timestamp.
- No Bolt RESET/auth/bookmark recovery/uncertain-commit boundary behavior was added or audited.
- No compatibility executor, adapter, or post-hoc fallback was introduced.

## Concerns

- Full valid-time interval region composition for staged values is intentionally not implemented in
  this task. Explicit transactions with a non-empty overlay now receive a deterministic explicit
  error for interval queries, avoiding silent falsification until the TemporalRow task implements
  correct region composition.

## Reviewer remediation evidence

### Fixed snapshot selector and real logical write prefixes

- `CypherQueryRequest` now carries an optional fixed transaction snapshot. The engine rejects a
  query whose resolved `AT TRANSACTION_TIME AS OF` differs from the BEGIN snapshot, and the Gateway
  rejects procedures in explicit transactions before allocating a fresh procedure snapshot.
- `CompiledQuery::read_prefix_plan` extracts the actual logical input of the first write operator.
  MATCH/WITH aliases and UNWIND therefore execute as typed logical operators rather than through
  query-text slicing. The MATCH/WITH and UNWIND tests were inherited GREEN in this continuation;
  no earlier RED run was observed, so they are not claimed as TDD RED evidence here.
- Replacing the previous fallback initially regressed standalone MERGE against an element created
  earlier: the Gateway integration test returned five Person rows instead of four. A compiler RED
  also showed that the extracted standalone MERGE prefix had no scan and an empty schema. The
  compiler now lowers the parsed MERGE `Pattern` into a structured NodeScan/Expand/Filter read plan
  with the original temporal selector. The focused compiler suite passed 12/12, and the Gateway
  retained its original four-row MERGE assertion.

### Source-free prefix execution

Executing `Argument -> UNWIND` on every shard produced six staged writes for a three-item list.
The optimizer RED expected one coordinator fragment but observed two, and a distributed runtime
RED returned `InvalidCoordinator` for a coordinator-only root without exchanges. Source-free plans
now execute exactly once on the coordinator, while graph-source plans retain all-shard placement.
The optimizer partitioning suite passed 4/4, the distributed coordinator suite passed 8/8, and the
Gateway persisted exactly three UNWIND-created nodes.

### Physical prepublish validation

A dtgproxy RED failed to compile because no prepared-candidate validator existed. The new test uses
a small test limit and proves that validation counts the prepared shard batch's physical mutations,
not the logical input rows. It now passes with the expected
`InvalidMutationCount { max: 2, actual: 3 }` result.

Remote temporal routing and preparation are shared by commit and
`validate_temporal_remote_candidate`. Preparation builds the real per-participant mutation batches,
endpoint guards, routed constraint claims, metadata, and protocol `PrewriteRequest` values. This
applies the existing mutation, constraint-claim, participant, sequence, duplicate-key, transaction,
schema, and topology limits before a statement is published. The Gateway validates the complete
existing-plus-candidate write set outside its mutex, then publishes only if the pending transaction
revision is unchanged.

### Cross-shard committed endpoint hydration

The first physical-fixture assertion was RED: both original MultiEndpoint pairs routed to shard 10,
so they did not prove the reviewer case. A replacement fixture commits two `RemoteEndpoint` nodes
and asserts their catalog routes are shard 10 and shard 20. The first real cross-shard run was also
RED: the staged edge existed, but OUT/IN expansion returned count zero because a shard-local worker
could not resolve the committed endpoint owned by the other shard.

Before each transaction read or write-prefix execution, the Gateway now resolves that query's valid
point and fixed BEGIN snapshot, fetches missing endpoints of visible staged edges from their owning
shards, and adds point-only lookup entries to a cloned execution overlay. The lookup values are
ephemeral and never broaden or replace the staged write's valid interval. Gateway tests verify OUT
and IN count one at valid times 1000 and 2000 for the same staged edge between committed endpoints.

### Statement atomicity and persisted timestamps

A direct `BoltQueryBackend` transaction stages two successful CREATE statements, executes a failing
two-row UNWIND/CREATE statement, reads the prior overlay as count two, and commits. External reads
then observe two `AtomicCommitted` nodes and zero `AtomicFailed` nodes. An interval read of the two
persisted successful rows reports one identical nonempty transaction interval for both rows. The
first direct test run was RED only because `BackendQueryResult` exposed no record accessor; the
semantic behavior was already present after candidate publication was implemented, so no semantic
RED is claimed for this final integration assertion.

### Final verification after remediation

Commands and results:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p temporal-storage -p query-executor -p distributed-query -p cypher-engine -p gateway-node -p dtgproxy
# exit 0: all selected unit, integration, process, and doc tests passed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-compiler --test compile
# exit 0: 12 passed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p query-optimizer --test partitioning
# exit 0: 4 passed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p distributed-query --test coordinator
# exit 0: 8 passed

CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p gateway-node --test service -- --nocapture
# exit 0: 1 passed
```

The implementation remains latest-only. It introduces no versioned query namespace, legacy query adapter,
second executor, Cypher 5 compatibility path, RESET/auth/bookmark recovery, or uncertain-commit
audit.

## Independent re-review

The same read-only reviewer was re-run after remediation. Verdict: **Ready**.

- Critical findings: none.
- Important findings: none.
- Minor finding: the report correctly discloses that semantic RED runs were not observed for the
  inherited MATCH/WITH/UNWIND prefix tests or for the final failed-multi-row integration behavior.
  This is a historical TDD-process gap, not a known functional failure. No claim of complete
  RED-first coverage is made.
