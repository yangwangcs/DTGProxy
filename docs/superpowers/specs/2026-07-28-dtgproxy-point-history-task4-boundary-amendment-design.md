# DTGProxy Point History Task 4 Boundary Amendment Design

## Status and authority

This amendment resolves implementation-boundary defects discovered before Point History Task 4.
The user has directed that future design decisions follow the recommended option, so the
recommended design below is treated as approved without another decision prompt.

It supplements, and where necessary overrides, the Task 4 section of:

- `docs/superpowers/specs/2026-07-28-dtgproxy-point-history-snapshot-csr-clean-break-design.md`
- `docs/superpowers/plans/2026-07-28-dtgproxy-point-history-clean-break.md`

Tasks 1 through 3 and the global clean-break constraints remain unchanged.

## Problem

Task 4 must add a multi-range canonical snapshot call and a snapshot-bound point-history reader.
The insertion points for the storage query SPI, backend snapshots, Sidecar protocol, observer, and
store errors currently live in protected uncommitted work. Three required files are also entirely
untracked. A direct Task 4 commit would therefore either capture thousands of unrelated lines or
produce a non-compiling, non-reviewable partial commit.

The original Task 4 text also leaves aggregate batch quotas, duplicate point requests, pagination,
history-key validation, error ownership, and test-file ownership ambiguous.

## Considered approaches

### 1. Directly implement and stage every listed Task 4 file

Rejected. This would silently absorb the protected backend and query work into the Task 4 commit,
making both review and later rollback unreliable.

### 2. Add a parallel optional batch extension trait

Rejected. `PointHistoryReader` accepts `&dyn ReadSnapshot`; an optional side trait would require
downcasting or a fallback path. That conflicts with the clean-break design and would make behavior
depend on runtime type discovery.

### 3. Checkpoint the prerequisite baseline, then split Task 4 into 4A and 4B

Selected. First commit and independently review the already-existing prerequisite query/snapshot
surface without any Task 4 symbols. Then add the batch SPI and implementations as Task 4A, followed
by the point replay engine as Task 4B. This gives each new behavior a compiling parent, a bounded
diff, focused tests, and an independent review gate.

## Architecture

### Prerequisite checkpoint

The checkpoint records only the current prerequisite query/snapshot implementation needed by Task
4. It is not labeled as Task 4 and must contain no `CanonicalBatchScan*`, `HistoryRead*`,
`PointHistory*`, or new history-reader error symbols.

Because the current worktree is intentionally dirty, checkpoint staging is by an explicit path
allow-list and relevant `Cargo.lock` hunks only. The complete cached diff, path list, whitespace
checks, and focused existing snapshot/query tests are reviewed before the checkpoint is accepted.

### Task 4A: bounded multi-range canonical snapshot SPI

`ReadSnapshot` gains `scan_canonical_batch`. Its default returns
`AdapterError::UnsupportedOperation`; it never loops over `scan_canonical`. Memory, RocksDB,
PostgreSQL, Neo4j, Sidecar, and the observed snapshot implement it explicitly. Test-only snapshots
may retain the fail-closed default. A future remote shard snapshot must add one real remote batch
RPC before advertising or using this capability; N single-range calls are never an allowed
implementation.

`CanonicalBatchScanRequest` has these exact invariants:

- range count is `1..=256`;
- ranges are unique by `KeySpan`, independent of page-bound equality;
- `max_total_bytes` is nonzero and no greater than `MAX_QUERY_PAGE_BYTES`;
- the sum of subrequest `max_bytes` is no greater than `max_total_bytes`;
- the sum of subrequest `max_items` is no greater than `MAX_QUERY_PAGE_ITEMS`.

`CanonicalBatchScanPage` preserves request order and has exactly one page per request. Every page
respects its own bounds, every page carries the same exact snapshot `applied_log_index`, and total
retained key-plus-value bytes across all pages do not exceed `max_total_bytes`.

One batch call means one physical adapter or remote protocol request per pagination round:

- Memory and RocksDB execute bounded iterators inside one adapter method call.
- PostgreSQL executes one ordinal-preserving parameterized statement inside the existing
  repeatable-read transaction.
- Neo4j executes one `UNWIND $ranges` statement inside the existing open Query API transaction.
- Sidecar appends new envelope fields, negotiates a new feature bit, and sends one framed request
  containing every range. Existing field numbers are never changed.

### Task 4B: snapshot-bound point replay

`PointHistoryReader` binds every replay to `read.applied_log_index()`. A replay page is rejected if
its applied index differs, its continuation does not advance, its key is not the requested history
key, its key transaction timestamp disagrees with the record, or the chain terminates without an
anchor.

Duplicate point requests are allowed. Requests are grouped by the pair `(element,
transaction_time)`, so one canonical history range is scanned once and its result is fanned out to
all requested valid times while preserving original input order. At most 256 distinct ranges are
sent in one batch call.

Pagination is round-based. Each unresolved distinct range contributes at most one subrequest per
round. Completed ranges are removed; unfinished ranges advance using their validated continuation.
Thus nine one-page distinct requests require one adapter call, while deeper chains require one
call per additional pagination round rather than one RPC per element.

`HistoryReadBudget` uses encoded record value bytes:

- `max_records` is in `1..=MAX_CHAIN_ENTRIES`;
- `max_total_bytes` and `max_record_bytes` are nonzero;
- `max_record_bytes <= max_total_bytes`;
- `max_record_bytes` is checked against `entry.value().len()`;
- `max_total_bytes` is the sum of `entry.value().len()` for charged history records.

Adapter page bounds continue to count retained key-plus-value bytes. The reader includes bounded
history-key overhead when deriving page byte limits so an oversized record can be classified by
the reader rather than hidden behind an adapter page error.

`HistoryReadStats.payloads_decoded` counts only payloads converted into owned public values.
Borrowed structural validation by `HistoryEntryRef` is not counted as owned decoding.
`payload_bytes_copied` counts only bytes copied into owned point deltas or the final result.

The reader adds explicit errors owned by `TemporalStoreError` for invalid budget, per-record byte
limit, total byte limit, applied-index mismatch, nonadvancing continuation, unexpected history key,
key/record timestamp mismatch, missing anchor, and excessive replay depth. No error falls back to
the old reconstruction path.

## Data flow

```text
PointHistoryRequest[]
  -> group by distinct (element, transaction_time)
  -> build <=256 bounded history ranges
  -> one scan_canonical_batch call for the round
  -> validate index, page order, bounds, key order, and continuation
  -> retain only valid-time-matching Put bytes or Delete markers
  -> stop each range at its anchor
  -> replay matching deltas over the anchor's visible payload
  -> project demanded properties
  -> fan outcomes back to original request order
```

## Testing and review gates

The prerequisite checkpoint, Task 4A, and Task 4B each receive a separate implementation commit and
independent task review.

Task 4A contract tests cover:

- zero, too many, duplicate, and aggregate-over-budget requests;
- input-order preservation and exact page cardinality;
- per-page item/byte limits and aggregate retained bytes;
- one applied index across all pages;
- one physical multi-range operation for Memory, RocksDB, PostgreSQL, Neo4j, and Sidecar;
- unchanged Sidecar field numbers and explicit feature negotiation.

Task 4B tests cover:

- depth 0, 1, 8, and 15 replay;
- nonmatching Delta payloads not copied or owned-decoded;
- record, total-byte, and chain-depth limits;
- missing anchor, wrong applied index, malformed/nonadvancing continuation, unexpected key, and
  key/value transaction mismatch;
- duplicate ranges with different valid times scanned once and fanned out in input order;
- nine distinct one-page requests using one adapter batch call;
- owned result equivalence with the current temporal semantics fixtures.

## Clean-break boundary

This amendment adds no compatibility aliases, feature flags, dual reads, dual writes, or fallback
to `ProjectionRecord::decode`, `reconstruct`, or `rewrite_projection`. Unsupported snapshots fail
closed. Production snapshots used by point replay must implement the real bounded batch primitive
before the reader is routed to them.
