# Task 2: Complete distributed MERGE pattern recovery

## Context

The current distributed constraint service routes deterministic keys to stable owner shards and
protects claims with 2PC intents. It persists only one canonical owner and, after lookup, the
gateway restores only that owner's variable. A node-only MERGE works, but a path MERGE cannot
recover all existing nodes/relationships and may retry with a different owner. Relationship-only
MERGE with bound endpoints also derives a key without endpoint identities.

## Required behavior

1. Keep the existing owner-shard/intent protocol and single persisted owner value. Use the current
   query's deterministic probe to reconstruct the full pattern after the stored owner validates the
   probe; no old compatibility representation is needed.
2. Each `MergeConstraint` must retain the exact probe binding names created by that MERGE clause.
   It must not claim bindings created by another MERGE clause or supplied by an earlier MATCH.
3. A successful constraint lookup must restore all of that claim's deterministic node and
   relationship bindings and mark that exact merge key resolved. Conflicting binding values fail
   closed.
4. `WriteContext`/materialization must accept the set of resolved merge keys. A resolved key makes
   that MERGE clause a no-op while retaining all restored bindings, scoped transactions empty, and
   no new constraint claim.
5. `probe_merge_constraints` must seed existing bindings. Reorder gateway preparation so MATCH
   bindings are resolved before probing MERGE constraints.
6. A MERGE pattern is already bound only when all of its named node and relationship variables are
   bound. Bound nodes alone must not cause relationship MERGE to disappear.
7. Constraint hashing for a node variable already bound by MATCH must include its canonical
   `ElementRef` identity. Thus identical relationship type/properties over different endpoint pairs
   route to different constraint keys. Keep hashing canonical and backend-independent.

## Final review fixes

8. `MATCH ... MERGE` must execute once for every upstream row. Prepare every row-scoped write with
   one fixed `start_ts` and one fixed `commit_ts`, then commit the combined participant set as one
   atomic transaction. Explicit Bolt transactions must append every prepared row write to the same
   staged transaction. Zero upstream rows remain a no-op. Row-local bindings must not overwrite
   another row's materialization state.
9. A standalone anonymous first MERGE must not suppress later clauses. For example,
   `MERGE (:A {id: 1}) MERGE (b:B {id: 2})` must create both patterns and return/recover `b`.
   Anonymous cardinality probing is valid even when the first clause contributes no projected name.
10. Do not coalesce same-key/same-owner `MergeConstraint` objects or union their `binding_names`.
    Each claim retains only names introduced by its own MERGE clause. Same-key/different-owner is a
    hard error. Transaction routing may deduplicate the physical protocol constraint write, and
    mutation deduplication by `ElementRef` remains required.
11. Add end-to-end coverage for two upstream MATCH rows creating two bound relationships, repeat as
    a no-op, anonymous-first/named-second MERGE, independent recovery of identical MERGE aliases,
    and post-race cardinality recount for concurrent path MERGE.

Use strict TDD: add each failing regression first, run it and record the expected failure, then
implement the minimum current-only behavior. Do not add compatibility aliases or old query paths.
8. Anonymous pattern elements must still work through the deterministic probe binding names.
9. Preserve node-only MERGE behavior, bounded retries, deterministic IDs, cross-shard routing, and
   current latest-only naming. Do not add a legacy format/adapter.

## TDD coverage

- Cypher engine: a full path probe records exactly its created node/relationship bindings; feeding
  those bindings plus the resolved key back produces a no-op with identical bindings.
- Cypher engine: relationship MERGE over already-bound endpoints creates/claims the relationship;
  changing an endpoint identity changes the constraint key.
- Gateway integration: repeating a full path MERGE returns identical node and relationship IDs and
  does not create extra graph elements; concurrent identical path MERGE converges to one pattern.
- Add a bound-endpoint relationship MERGE integration if the current parser/compiler read-prefix
  path supports it; otherwise implement the missing current-path support rather than weakening the
  requirement.

Run focused tests and then:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-engine -p gateway-node
```

Use `rustfmt --edition 2024` for touched Rust files. Preserve unrelated dirty changes. Do not
commit. Append a report to `.superpowers/sdd/task-2-merge-pattern-report.md`.
