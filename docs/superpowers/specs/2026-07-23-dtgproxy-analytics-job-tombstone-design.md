# DTGProxy Analytics Job Tombstone Design

## 1. Goal

Allow the analytics Artifact garbage collector to reclaim a latest pinned generation only when
Meta durably proves that the owning Job reached a terminal state and was pruned. Absence from the
active Job map is never sufficient proof. Unknown and non-terminal Jobs remain fail-closed.

## 2. Tombstone record

Meta stores one `JobTombstone` keyed by `AnalyticsJobId` with:

- final Job revision and terminal `JobState`;
- terminal prune time in Unix milliseconds;
- the final checkpoint and result `(storage_shard_id, generation)` identities, when present;
- `artifacts_reclaimed = false` until a fenced GC owner proves that no Artifact head remains;
- the acknowledging `gc_epoch` and acknowledgement time after reclamation.

The record is checksum-protected by the existing Analytics Ledger snapshot checksum. It is current
format only; no compatibility decoder or legacy command path is added.

## 3. State transitions

1. `PruneTerminal` requires a terminal Job, an exact Job revision and a non-zero prune time. It
   atomically removes the active Job and creates the tombstone. Replaying the same command is
   idempotent; a conflicting revision or time fails closed.
2. A tombstone cannot be created for `QUEUED`, `LEASED` or `RUNNING` Jobs.
3. `AcknowledgeArtifactsReclaimed` requires the tombstone revision, current GC owner Gateway ID,
   current `gc_epoch`, and a non-zero acknowledgement time. Meta validates the active GC lease
   before proposing the Ledger command.
4. Acknowledgement is legal only after the Gateway has observed a complete Shard-wide head scan
   with no generation for the tombstoned Job. It is idempotent for the same or a higher committed
   maintenance retry carrying the same epoch.
5. `CompactTombstones` removes only tombstones with `artifacts_reclaimed = true` whose prune and
   acknowledgement times are both at or before the compaction floor. Unacknowledged tombstones
   survive indefinitely.

## 4. Meta protocol

Meta adds a bounded, canonically ordered `ListAnalyticsJobTombstones` maintenance RPC. The cursor is
an exclusive `job_id`; each row carries the encoded tombstone and CRC32 checksum. Pagination uses
the same strict rules as `ListAnalyticsJobs`: a full page requires a next cursor, a short page must
not carry one, and the cursor must advance.

The existing `ProposeAnalyticsJob` RPC carries the new Ledger commands. For
`AcknowledgeArtifactsReclaimed`, the service verifies that the request Gateway and epoch equal the
currently committed, unexpired Analytics GC lease. Ordinary Job commands keep their existing path.

## 5. Gateway GC behavior

The maintenance owner scans active Jobs, tombstones and Shard Artifact heads under one committed GC
epoch:

- active Job manifests remain protected;
- a tombstone classifies its Job as terminal and permits TTL-eligible latest pinned generations to
  advance the Shard Artifact fence before deletion;
- an absent Job without a tombstone remains protected;
- a non-terminal active Job remains protected even if its latest pinned generation is not its
  current manifest;
- when a complete head scan contains no generation for an unacknowledged tombstone, the Gateway
  submits the fenced reclamation acknowledgement;
- crashes before fence advance, after fence advance, after delete, or before acknowledgement are
  retried idempotently on the next maintenance tick.

## 6. Safety invariants

- No latest pinned Artifact is deleted from Meta absence alone.
- No stale GC owner can acknowledge reclamation or advance a Shard fence after a higher epoch.
- Tombstone compaction never races ahead of Artifact reclamation.
- Active Job and tombstone identities are disjoint.
- Snapshot restore preserves every unacknowledged tombstone and acknowledgement epoch.
- Unknown, corrupt, duplicate or non-canonical tombstone pages fail closed.

## 7. Certification

Acceptance requires Ledger state-machine tests, command/snapshot corruption tests, Meta RPC and
restart tests, Gateway planner tests, and an end-to-end crash matrix covering deletion and
acknowledgement boundaries. The final state has no Artifact generation for the tombstoned Job and
one acknowledged tombstone that becomes compactable only after the configured floor.
