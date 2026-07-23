# Task 3: Complete fixed-snapshot multi-statement transactions

## Context

An explicit Bolt transaction already captures routing plus `start_ts`/`commit_ts`, and batches
prepared writes for one final distributed commit. Read-your-writes is currently implemented by
executing against the committed snapshot and then calling `CypherQueryResponse::merge_overlay_rows`.
That post-processing path cannot correctly handle updates/deletes of committed elements, mixed
committed/staged expansion, or general aggregates. `PendingBoltTransaction::staged_bindings` also
leaks variable names across Cypher statements even though variables are statement-local.

This task completes the main transaction semantics. Bolt protocol error states, RESET, bookmarks,
authentication, uncertain commit recovery, and transport/resource boundary certification remain
in the later unified boundary audit except for limits required to keep this overlay bounded.

## Required behavior

1. One explicit transaction uses exactly the routing/schema/topology snapshot and `start_ts`
   captured at BEGIN for every committed read used by every statement. No statement obtains a new
   read snapshot. All prepared writes retain the same transaction context and commit once through
   the current single-shard fast path or home-shard 2PC.
2. Implement a bounded transaction-local graph overlay at the query source stage. Node scan,
   relationship scan, and expand must see the committed snapshot after applying every successful
   prior staged put/delete, before Filter, Project, Unwind, Aggregate, Sort, Skip, Limit, joins, or
   coordinator operators run. Remove the aggregate-only arithmetic merge and result-row append
   special cases once the source-stage overlay is authoritative.
3. Overlay replacement is by canonical `ElementRef` and valid-time coverage. At a point valid time,
   a staged update replaces the committed value exactly once, a staged delete suppresses it, and a
   staged create adds it. Preserve validity metadata in the overlay; do not silently apply a write
   outside its valid interval. Interval-region composition belongs to the later TemporalRow task,
   but this task must not discard or falsify intervals.
4. Expansion must support all combinations at the fixed snapshot: committed source with staged
   edge/destination, staged source with committed edge/destination, staged edge between committed
   endpoints, and deletion/replacement of a committed edge or endpoint. Endpoint/type/direction
   checks and deterministic deduplication remain identical to ordinary execution.
5. Write-prefix MATCH resolution must execute against the same source-stage combined graph. A later
   statement can update/delete elements created or modified earlier, and can produce multiple
   upstream rows. Do not carry Cypher variable bindings across statement boundaries; variable names
   are local to each statement and must be rebound by that statement's MATCH/UNWIND/WITH pipeline.
6. Each statement is atomic within the explicit transaction. Prepare and validate against a
   savepoint/candidate overlay, then publish all row writes together. Any compile, read, materialize,
   merge-constraint, size-limit, or staging error leaves all prior successful statements unchanged
   and exposes none of the failed statement. Full ROLLBACK discards the transaction overlay.
7. Enforce the existing mutation/item limits before publishing a statement into the pending
   transaction. No partial append is allowed when a multi-row statement exceeds a limit.
8. Keep current-only APIs and formats. Do not add a legacy adapter, a second executor, or a
   post-hoc compatibility fallback.

## Required TDD coverage

Add real RED tests first and record the expected failure for each behavior:

- an external commit after BEGIN is invisible to later reads in the open transaction;
- a staged create is visible to later MATCH/projection/aggregate, while ROLLBACK removes it;
- staged update/delete of a committed node affects ordinary rows and `count` without duplicates;
- a staged relationship expands with committed endpoints, and staged/committed mixed expansion
  works in both directions;
- a later write statement MATCHes a staged element through graph semantics, not a remembered alias;
- reusing the same variable name in a later statement does not bypass its label/property predicate;
- a failing multi-row statement leaves the prior overlay and final commit unchanged;
- all statement writes commit atomically under one transaction id and timestamps.

Run at minimum:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p temporal-storage -p query-executor -p distributed-query -p cypher-engine -p gateway-node -p dtgproxy
```

Use `rustfmt --edition 2024` and `git diff --check`. Do not commit. Append RED/GREEN evidence,
implementation notes, self-review, and concerns to
`.superpowers/sdd/task-3-fixed-snapshot-transactions-report.md`.
