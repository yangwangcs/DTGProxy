# Task 2: Complete distributed MERGE pattern recovery

## Status

Implemented without committing. The focused Cypher engine, two-shard gateway integration, and
required combined package suites pass against the current Gateway API.

## Files changed

- `crates/cypher-engine/src/write.rs`
  - Added resolved MERGE keys and an empty-upstream-row signal to `WriteContext`.
  - Made `MergeConstraint` retain the exact deterministic binding names introduced by its clause.
  - Seeded `probe_merge_constraints` with existing MATCH/staged bindings.
  - Included canonical bound-node `ElementRef` identity in constraint hashing.
  - Derived MERGE-created IDs from canonical structural positions rather than alias names, so
    identical clauses converge on one canonical owner while positions of bound endpoints remain
    part of the structural sequence.
  - Required relationship variables as well as node variables for an already-bound pattern.
  - Reconstructed and validated resolved named/anonymous pattern bindings before returning a
    no-op with no writes or new claim.
- `crates/cypher-engine/tests/write_materializer.rs`
  - Added full-path binding recovery/resolved no-op, bound-endpoint key identity, empty upstream
    MATCH, anonymous deterministic probe-name, and identical multi-clause alias recovery coverage.
- `crates/gateway-node/Cargo.toml` and `Cargo.lock`
  - Added direct AST/syntax dependencies for structured clause and pattern inspection.
- `crates/gateway-node/src/service.rs`
  - Resolves MATCH bindings before MERGE probing.
  - Restores every claim-local deterministic probe binding after validating the persisted owner,
    accumulates resolved keys, and rejects conflicting values.
  - Replaced substring mutation-boundary detection with parsed clause spans and parsed pattern
    variables, excluding bindings introduced by MERGE from the MATCH projection.
  - Uses `RETURN 1` to preserve empty-input semantics for fully anonymous MATCH prefixes.
  - Distinguishes standalone MERGE probe misses from explicit MATCH prefixes with zero rows.
- `crates/gateway-node/tests/service.rs`
  - Added repeated/concurrent full-path MERGE, graph-element cardinality, bound-endpoint
    relationship MERGE, and empty MATCH+MERGE integration coverage.
- `crates/dtgproxy/src/transaction.rs`
  - Preserved bounded concurrent MERGE retries after committed business rejection.
  - A failed prewrite is excluded from cleanup only when it is demonstrably a persisted
    `CommittedRejection` with an explicit write/intent/constraint conflict. Ambiguous and network
    failures remain cleanup-pending.

## TDD evidence

Red phase:

- Full-path engine test failed to compile because `MergeConstraint::binding_names` and
  `WriteContext::with_resolved_merge_keys` did not exist.
- Structured MATCH+MERGE projection returned `None` instead of a prefix projecting only `a, b`.
- Empty-upstream engine test failed to compile because `WriteContext::without_input_row` did not
  exist.
- Concurrent gateway integration repeatedly failed with a durable `IntentConflict` because failed
  committed rejections were incorrectly left in `cleanup_pending`.
- Persisted-rejection classifier test failed to compile before the narrow classifier existed.
- Anonymous MATCH cardinality projection returned `None` instead of `RETURN 1`.
- Identical `MERGE (a:Person {id: 1}) MERGE (b:Person {id: 1})` clauses failed with
  `DTG-CYPHER-MERGE-CONSTRAINT-OWNER-CONFLICT` before structural IDs and claim-name unioning.

Focused green phase:

- `cargo test -p cypher-engine`: all 32 unit/integration/doc tests passed, including 10 write
  materializer tests.
- `cargo test -p gateway-node --lib`: all gateway library tests passed; both structured projection
  regressions pass after the final anonymous fix.
- `cargo test -p gateway-node --test service remote_gateway_commits_and_queries_a_cross_shard_temporal_transaction -- --exact`:
  1 passed, 0 failed. This exercises repeated and concurrent full paths, bound endpoints, and empty
  MATCH semantics on two shards.
- `cargo test -p dtgproxy --lib only_persisted_committed_merge_rejections_are_safe_from_failed_prewrite_cleanup`:
  1 passed, 0 failed.

## Required command

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-engine -p gateway-node
```

Result: passed against the current-only query, transaction, and Gateway surfaces.

## Formatting and diff checks

- `rustfmt --edition 2024 --check` on all Task 2 Rust files: passed.
- `git diff --check` on all Task 2 files and lockfile: passed.
- No commit was created.

## Concerns

- Structured write-prefix recovery intentionally supports the current parsed MATCH/OPTIONAL MATCH
  path. Broader row-producing prefixes such as arbitrary WITH/UNWIND pipelines remain outside the
  current prototype path and were not expanded.

## Final review

An independent reviewer initially found that identical MERGE clauses used alias-derived element
IDs, producing the same logical constraint key with different owners. The fix derives IDs from
pattern structural positions, unions recovery names for same-key/same-owner claims, and deduplicates
identical alias mutations while failing closed on divergent alias materializations. The reviewer
re-ran all 10 materializer tests and reported no remaining actionable findings.

## Final review fixes 8-11

This section supersedes the earlier statement about unioning same-key binding names. The final
required combined suite is green in the current shared worktree, and same-key/same-owner
constraints remain claim-local.

### Changes

- `resolve_existing_write_bindings` now returns one binding map per upstream row. An explicit
  MATCH with zero rows returns one explicit no-input marker so materialization remains a no-op;
  standalone MERGE still receives one normal empty binding row.
- `prepare_cypher_writes` allocates or accepts one `(start_ts, commit_ts)` pair, resolves the read
  prefix at that fixed snapshot, and prepares every row under the same `TransactionContext`.
  Row-specific probe request IDs prevent request replay collisions, while deterministic row seeds
  isolate non-MERGE row-local writes without changing the single-row seed.
- Auto-commit sends the complete prepared vector to `commit_prepared_cypher_writes`, which verifies
  the shared transaction context and merges every participant overlay into one atomic transaction.
- Explicit Bolt transactions append every prepared row to `PendingBoltTransaction::writes`.
  Conflicting row-local aliases are omitted from the cross-statement staged-binding map rather than
  silently overwriting another row; bindings for the same canonical `ElementRef` remain eligible
  for later updates. Conflicts with a different earlier staged identity fail closed.
- Response binding aggregation similarly omits ambiguous aliases while preserving every row's
  overlay values and persistent mutations.
- Structured standalone MERGE extraction now scans later MERGE clauses when an anonymous first
  MERGE contributes no projected names. `MERGE (:A {id: 1}) MERGE (b:B {id: 2})` therefore probes,
  creates, and recovers both clauses.
- `Materializer::finish` no longer unions same-key/same-owner claim names. It sorts claims,
  rejects same-key/different-owner claims, and deduplicates only fully identical
  `MergeConstraint` objects. The transaction coordinator's routed protocol map continues to
  deduplicate identical physical key/value claims.
- The two-shard integration now covers two MATCH rows creating two bound relationships, repeat
  no-op/cardinality, the same operation inside one explicit Bolt transaction, anonymous-first and
  named-second MERGE, independent identical-alias recovery, zero-row no-op, and post-race node/edge
  recount for concurrent path MERGE.

### RED evidence

Claim-local identical aliases:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-engine --test write_materializer \
  identical_merge_clauses_keep_claim_local_aliases_and_recover_independently -- --exact
```

Expected failure observed: assertion expected 2 local constraints but received 1 because `finish`
had coalesced their binding-name sets.

Anonymous-first extraction:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p gateway-node --lib \
  write_match_projection_skips_anonymous_merge_before_named_merge -- --nocapture
```

Expected failure observed: extractor returned `None` instead of
`USE social MATCH (b:B {id: 2}) RETURN b`. The companion engine regression already passed,
isolating the missing behavior to gateway extraction.

Multi-row MATCH fan-out:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p gateway-node --test service \
  remote_gateway_commits_and_queries_a_cross_shard_temporal_transaction -- --exact --nocapture
```

After verifying that the fixture produced four endpoint nodes and two `MULTI_PAIR` rows, the
expected failure was:

```text
Gateway query error: existing-element write matched 2 rows; prototype requires exactly one row
```

The first fixture attempt produced only one pair because two separate CREATE requests reused the
same variable names under the principal-stable write seed; the test was corrected to use distinct
CREATE variable names before accepting the multi-row RED result.

### GREEN verification

- `cargo test -p cypher-engine`: passed, including 11 write materializer regressions.
- `cargo test -p gateway-node --lib`: 5 passed, 0 failed.
- `cargo test -p gateway-node --test service remote_gateway_commits_and_queries_a_cross_shard_temporal_transaction -- --exact`:
  1 passed, 0 failed.
- `cargo test -p dtgproxy --lib`: 2 passed, 0 failed.
- `cargo check -p gateway-node --tests`: passed after compiling all test targets.
- Required `cargo test -p cypher-engine -p gateway-node`: passed completely, including the process
  test, the expanded two-shard service test, and doc tests.
- `rustfmt --edition 2024 --check` on all touched Task 2 Rust files: passed.
- `git diff --check` on Task 2 files and the lockfile: passed.

### Self-review

- Every prepared row carries the exact same `TransactionContext`; commit rejects a mixed-context
  vector before grouping mutations.
- Constraint lookup request IDs include the row index, so two rows cannot replay one lookup under
  different keys.
- Row-local binding maps are cloned from staged state and never extended into another row's map.
- Empty explicit MATCH input still traverses the no-input materialization path and produces no
  transaction or claim.
- Same-key claim recovery iterates each local `MergeConstraint` independently; resolved keys are
  deduplicated only in the `WriteContext` set after every claim-local binding set is restored.
- No compatibility representation, alias API, or old query path was added.

### Remaining concern

The structured write-prefix implementation remains intentionally limited to the current parsed
MATCH/OPTIONAL MATCH path. Arbitrary row-producing WITH/UNWIND write prefixes are still outside the
prototype behavior described by this task.

### Independent final review

The final read-only reviewer reported no actionable findings and approved the fixes. The review
specifically confirmed row preparation and combined commit, explicit Bolt staging, resolved-key
recovery, claim-local aliases, owner-conflict enforcement, and protocol-only constraint deduplication.
The reviewer independently ran the write-materializer suite: 11 passed, 0 failed.
