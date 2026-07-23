# Task 4A: Complete current Cypher 25 WITH, UNWIND, and UNION pipelines

## Context

The current-only parser/compiler/runtime already has a minimal `WITH`, `UNWIND`, and one-boundary
`UNION` path. It passes simple read examples and source-free `UNWIND ... CREATE`, but the compiler
explicitly rejects multiple UNION boundaries and the row-pipeline implementation is not yet a
complete Cypher 25 composition surface. This task completes these three clause families before
named procedures and subqueries are handled in Tasks 4B and 4C.

The sole implementation is the top-level `temporal-ir` and `query-executor` path. Never add a
versioned namespace, alternate query representation, compatibility adapter, text-execution
fallback, or backend-native Cypher pass-through.

## Required behavior

1. `WITH` establishes a new lexical scope. Only projected bindings remain visible. Support aliases,
   expressions, `WITH *`, `DISTINCT`, aggregation, and the attached `WHERE`, `ORDER BY`, `SKIP`, and
   `LIMIT` pipeline in the same order required by Cypher. A name removed or shadowed by WITH must
   fail semantic analysis when used later.
2. `UNWIND` executes once per upstream row, supports chained/nested row pipelines, retains upstream
   bindings, emits no rows for an empty list, and follows Cypher null/list type behavior. A write
   after WITH/UNWIND must materialize once per resulting row and publish atomically through the
   existing fixed-snapshot statement candidate path.
3. Support arbitrary top-level UNION and UNION ALL chains, not only one boundary. Each query part
   has an isolated input/scope, all parts must expose the same column names and compatible types,
   and each boundary preserves its own DISTINCT/ALL semantics. Do not flatten a mixed chain in a
   way that changes duplicate elimination.
4. Support UNION chains inside a read subquery using the same logical/physical operators. Task 4C
   will complete the remaining subquery forms; do not introduce a special subquery executor here.
5. Point and interval execution must both preserve row values, multiplicity, deterministic ordering
   where ORDER BY is present, and exact temporal regions/provenance through these operators.
6. PrimaryReplica and Shared-Nothing must return canonical-equivalent results. Source-free branches
   execute once at the coordinator; graph-source branches execute on routed shards and gather before
   global DISTINCT, aggregate, sort, skip, or limit.
7. Every new collection, branch count, row expansion, and intermediate batch is bounded by existing
   parser/IR/executor limits. Failure must be deterministic and must not partially publish writes.
8. Remove the current `DTG-CYPHER-NESTED-UNION-NOT-YET-LOWERED` limitation and any other placeholder
   success or silent fallback for these clause families. Keep current Cypher 25 only.

## Required TDD coverage

Record actual RED and GREEN evidence for at least:

- three UNION boundaries with a mixed `UNION ALL`/`UNION` chain where boundary order changes the
  correct multiplicity;
- a UNION branch schema-name mismatch and a type mismatch;
- `WITH DISTINCT`, grouped aggregation, alias shadowing, `WHERE`, `ORDER BY`, `SKIP`, and `LIMIT`;
- chained `UNWIND` over multiple upstream rows, plus empty-list and null behavior;
- `WITH ... UNWIND ... CREATE` producing all rows in one explicit transaction and leaving no rows
  after a deliberately failing statement;
- an interval UNION/WITH/UNWIND case proving regions are retained and duplicate elimination does
  not merge unequal temporal regions;
- canonical-equivalent results in PrimaryReplica and Shared-Nothing for a graph-source pipeline;
- a UNION chain inside a read subquery.

Run at minimum:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-syntax -p cypher-sema -p cypher-compiler -p temporal-ir \
  -p query-executor -p query-optimizer -p distributed-query -p cypher-engine -p gateway-node
```

Use `rustfmt --edition 2024` and `git diff --check`. Do not commit. Append RED/GREEN evidence,
implementation notes, self-review, and concerns to
`.superpowers/sdd/task-4a-query-pipelines-report.md`.
