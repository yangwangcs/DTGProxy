# Task 1: Durable committed business rejection

## Context

Concurrent distributed MERGE can commit a command that deterministically loses a constraint race.
`RawNode::ready()` then considers that entry delivered, while `ShardStateMachine::apply_entry`
returns a business error without advancing durable `applied_index`. A later LightReady batch starts
after the rejected entry and fails with a non-contiguous index.

## Required behavior

1. Add and export a current-only `CommittedEntryOutcome` with `Applied(ApplyReceipt)` and
   `Rejected { receipt: ApplyReceipt, message: String }` variants.
2. Add `ShardStateMachine::apply_committed_entry`. Successful commands retain existing behavior.
   Deterministic business errors must atomically persist:
   - the request outcome and command digest,
   - the entry term/digest,
   - the new replica position/applied index,
   and return `CommittedEntryOutcome::Rejected`.
3. Replace the fixed request-digest metadata value with a bounded current request-outcome record.
   Use the existing `DTRQ` magic and current metadata format identifier; no old-format reader or
   compatibility path is permitted. The record must distinguish applied/rejected, checksum all
   fields, validate lengths and UTF-8, and bound persisted rejection text to 512 bytes without
   splitting a UTF-8 code point.
4. `request_replay` must return `Ok(true)` for an applied duplicate, `Ok(false)` for a new request,
   `RequestMismatch` for a digest mismatch, and `CommittedRejection` carrying the persisted message
   for a rejected duplicate.
5. Deterministic command/transaction validation errors may be durably rejected. Adapter I/O,
   corrupt metadata, index/term/replay invariant violations, malformed command bytes, replica
   fault, and receipt mismatch must remain fatal errors and must not be converted to rejection.
6. `DurableRaftReplica::apply_entries` must use committed application for normal commands,
   consider `Rejected` durably applied, continue the batch, skip backend-transition completion for
   a rejected command, and advance Raft apply to the state machine's durable frontier after Ready
   and LightReady processing.
7. Any other path that applies entries already committed by Raft, including the in-process shard
   group, must preserve the same no-gap invariant.
8. Keep `apply_entry` behavior for direct/pre-commit callers unless changing it is necessary for a
   coherent API. Do not change unrelated gateway/query behavior.

## Tests

- Make `committed_business_rejection_advances_and_replays_before_the_next_entry` pass.
- Add reopen coverage proving the rejection outcome and the following accepted entry survive
  reopening the adapter/state machine.
- Add DurableRaftReplica coverage showing one rejected committed entry does not prevent the next
  committed entry from applying.
- Run with:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p shard-runtime
```

Use `rustfmt --edition 2024` for touched Rust files. Do not commit.
