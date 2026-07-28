# Point History Task 4 Whole-Slice Fix Report

## Status

The two Important and two Minor findings from the independent `49823af..035626f` whole-slice
review are corrected on branch `codex/point-history-budget-correction`.

Correction commits:

- `4fed5ab fix: preserve point history quota semantics`
- `0377d64 test: harden canonical batch contracts`

Supporting approved design and plan commits already on the parent branch:

- `844e33f docs: define point history budget correction`
- `cf60979 docs: plan point history budget correction`

The corrected whole-slice range for re-review is `49823af..0377d64`.

## Root cause and correction

`PointHistoryReader::build_batch` previously multiplied each range's maximum record allowance by
every remaining history record. A legal 1 MiB record allowance therefore reserved almost the full
16 MiB canonical batch for one range, even when each range contained only a small anchor. The
reader also reduced later page bounds to the remaining cumulative value budget, so a production
adapter could reject the first entry before `charge_record` classified it as a history record or
total-byte limit.

The corrected reader assigns each unresolved range one encoded-record inspection allowance plus
its exact history-key overhead per pagination round. It admits as many ranges as fit under the
canonical 16 MiB aggregate cap, keeps the remaining-record count as the item limit, and advances
unfinished ranges in later lockstep rounds. Adapter `ScanByteLimit` on this inspection path maps to
`HistoryRecordByteLimit`; individually fitting records reach the reader's value-only cumulative
accounting and produce `HistoryTotalByteLimit` when appropriate.

The test snapshot now enforces actual item and key-plus-value byte page bounds. New regressions
cover nine distinct one-page histories with a 1 MiB record allowance, an oversized anchor reached
after fifteen deltas, and cumulative exhaustion reached on the anchor page.

The Sidecar append-only field test now parses top-level protobuf field keys and exact field numbers
21 and 23. Neo4j no longer sends or returns the unused `range.max_bytes` result column; byte
enforcement remains in `bounded_canonical_page`.

## TDD evidence

After only the byte-enforcing fixture and regression tests were added, the PointHistory suite
failed exactly three tests:

- nine-range batch call count: actual 9, expected 1;
- cross-page record quota: actual `ScanByteLimit`, expected `HistoryRecordByteLimit`;
- cross-page cumulative quota: actual `ScanByteLimit`, expected `HistoryTotalByteLimit`.

After the reader correction, the suite passed 14/14.

The Neo4j contract assertion was changed before production code. It failed because
`CANONICAL_BATCH_SCAN_CYPHER` still contained `range.max_bytes`; removing the unused column made the
test pass.

## Focused verification

All commands used an isolated worktree and an isolated `CARGO_TARGET_DIR`.

Passed tests:

- `storage-api --test query_primitives`: 13/13;
- `adapter-memory --test canonical_batch_scan`: 1/1;
- `adapter-rocksdb --test canonical_batch_scan`: 1/1;
- `adapter-postgres --test canonical_batch_sql --test sql_contract`: 7/7;
- `adapter-neo4j --lib` plus `--test canonical_batch_protocol`: 24/24;
- `adapter-sidecar --test protocol --test client`: 19/19;
- `temporal-storage --test point_history_reader --test query_adapter_metrics --test record_ref`:
  22/22.

Total focused static and simulated evidence: **87 passed, 0 failed**.

Additional checks:

```text
cargo fmt --all -- --check
git diff --check
git show --check --oneline 4fed5ab
git show --check --oneline 0377d64
```

All exited 0. The correction branch is clean.

## Live backend limitation

The broader PostgreSQL test invocation compiled successfully and passed its non-live tests, but 10
live/mapping tests failed immediately because `DTGPROXY_POSTGRES_URL` is absent. Neo4j live tests
were not invoked because `DTGPROXY_NEO4J_ENDPOINT` and `DTGPROXY_NEO4J_PASSWORD` are absent. This is
the same environment limitation identified by the previous whole-slice reviewer.

The 87 passing tests prove the corrected Rust logic, adapter contracts, protocol framing, query
shape, and simulated pagination. They do not constitute live PostgreSQL or live Neo4j execution
evidence. Those databases remain required for the later three-backend reproducible performance and
deployment certification stages.

## Path scope

The correction since `035626f` contains only:

- `crates/temporal-storage/src/history_reader.rs`
- `crates/temporal-storage/tests/point_history_reader.rs`
- `crates/adapter-sidecar/tests/protocol.rs`
- `crates/adapter-neo4j/src/lib.rs`
- `crates/adapter-neo4j/tests/canonical_batch_protocol.rs`
- `docs/superpowers/specs/2026-07-28-dtgproxy-point-history-task4-boundary-amendment-design.md`
- `docs/superpowers/plans/2026-07-28-point-history-budget-correction.md`

No merge or push has occurred. The shared dirty checkout was not modified by implementation or
verification work.

## Next gate

Request an independent, read-only re-review of `49823af..0377d64`. Task 4 should close only if the
review confirms the batching and quota corrections and finds no remaining Critical or Important
issue.
