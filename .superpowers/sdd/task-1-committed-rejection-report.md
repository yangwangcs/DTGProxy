# Task 1: Durable committed business rejection report

## Status

Implemented without committing. The required `shard-runtime` test suite passes all 47 tests.

## Files changed

- `crates/shard-runtime/src/lib.rs`
  - Exported current-only `CommittedEntryOutcome::{Applied, Rejected}`.
  - Added `ShardRuntimeError::CommittedRejection`.
  - Added an exhaustive deterministic-business classifier, including an exhaustive `TxnProtocolError` classification.
- `crates/shard-runtime/src/metadata.rs`
  - Replaced the fixed digest-only request value with tagged `RequestOutcomeRecord` values.
  - Retained `DTRQ` and metadata format version 1 with no legacy reader.
  - Added full checksums, explicit message length, strict tag/length/UTF-8 validation, and UTF-8-safe 512-byte rejection-message bounding.
- `crates/shard-runtime/src/state_machine.rs`
  - Added `apply_committed_entry` and shared internal apply logic.
  - Atomically persists rejected request outcome/digest, entry term/digest, and position/applied index.
  - Preserved direct `apply_entry` behavior: new business validation failures remain errors without advancing.
  - Updated request replay to distinguish new/applied/rejected/mismatched requests and carry persisted rejection text.
  - Handles both applied and rejected request duplicates at later committed log indexes without introducing gaps.
- `crates/shard-runtime/src/durable_replica.rs`
  - Uses committed application for normal committed entries.
  - Treats `Rejected` as durably applied, continues the batch, and skips backend-transition completion.
  - Advances Raft apply to the state machine durable frontier after Ready and LightReady.
- `crates/shard-runtime/src/raft_group.rs`
  - Uses committed application in the in-process committed-entry path.
  - Advances Raft apply to the state machine durable frontier.
  - Carries rejected status through applied events so `propose_and_wait` returns persisted `CommittedRejection` rather than success or timeout.
- `crates/shard-runtime/tests/state_machine.rs`
  - Covers rejection advance/replay, following accepted entry, and reopen durability.
- `crates/shard-runtime/tests/distributed_transaction.rs`
  - Covers fatal committed `RequestReplayMismatch` and `MissingIntentLock`, asserting no frontier advance and no request-outcome record.
- `crates/shard-runtime/tests/durable_replica.rs`
  - Covers a rejected normal entry followed by an accepted entry in one Ready drain and verifies backend completion is skipped.
- `crates/shard-runtime/tests/raft_group.rs`
  - Covers in-process rejection reporting and later replica-wide progress.

The workspace was already substantially dirty. Existing unrelated changes, including earlier diagnostic changes in `durable_replica.rs` and unrelated test/runtime edits, were preserved.

## Classification decisions

Durable business rejection includes deterministic state-dependent command errors: stale target epoch transitions, non-monotonic/closed-time command conflicts, intent-at-closed-time, participant proof mismatch, reserved metadata mutation, mutation count overflow, backend lifecycle conflict, and explicitly enumerated transaction semantic/conflict outcomes.

Fatal errors include adapter I/O, command decoding/malformed bytes, invalid log position, shard routing mismatch, non-contiguous index, term regression, divergent entry replay, metadata/index corruption, replica fault, apply receipt mismatch, request-envelope/digest mismatch, prior committed rejection as an ordinary direct-apply error, and invalid backend generation.

Transaction corruption/invariant failures are explicitly fatal: invalid range-length/record-size paths, corrupt/unsupported/non-canonical records, payload encode/decode, missing fields, identifier/digest/length failures, unknown tags, non-canonical delete, length overflow, inspection count mismatch, request replay mismatch, missing expected intent lock, and corrupt participant state. Constraint/intent/write/read-dependency conflicts remain durable business rejections.

## Review findings

The independent reviewer found one P1 issue: `TxnProtocolError::RequestReplayMismatch` and `MissingIntentLock` were initially classified as durable rejection even though they indicate persisted-state/replay invariants. The finding was verified against `ParticipantEngine`, both variants were moved to the fatal arm, and two committed-path regressions were added. Both tests prove the state-machine and adapter frontier remain unchanged and `request_replay` returns `Ok(false)` because no outcome record was persisted.

## TDD and verification

Red phase:

- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p shard-runtime committed_business_rejection_advances_and_replays_before_the_next_entry -- --exact`
  - Failed to compile as expected: missing `CommittedEntryOutcome`, `apply_committed_entry`, and `CommittedRejection`.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p shard-runtime --test distributed_transaction committed_ -- --nocapture`
  - Failed both new invariant tests before the review fix because the calls returned durable rejection instead of fatal errors.

Focused green verification:

- Exact required state-machine regression: 1 passed, 0 failed.
- DurableRaftReplica rejection/following-entry regression: 1 passed, 0 failed.
- In-process rejection reporting regression: 1 passed, 0 failed.
- Committed transaction invariant regressions: 2 passed, 0 failed.

Final required verification:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p shard-runtime
```

Result: 47 passed, 0 failed (including unit, integration, and doc tests).

Formatting/whitespace verification:

- `rustfmt --edition 2024 --check` on all touched Rust files: passed.
- `git diff --check -- crates/shard-runtime`: passed.

## Concerns

An additional non-required `cargo clippy -p shard-runtime --tests -- -D warnings` check fails only on `clippy::result_large_err`: the pre-existing uncommitted `DurableReplicaError::StateMachineApply` diagnostic variant stores `ShardRuntimeError` inline and makes the error enum at least 128 bytes. The task's required tests and formatting checks pass. I did not alter that existing public diagnostic shape because it predates and is separate from the committed-rejection implementation.

## Final review fixes

Three final review findings were addressed after the initial report:

1. Committed request duplicates are now resolved by persisted request outcome before current authority validation. A matching applied or rejected request from epoch N advances a new committed log entry after activation of epoch N+1 and returns the original outcome. Direct application and new committed requests still validate current shard/epoch authority.
2. Durable backend-slot validation now runs only after the state machine returns `Applied`. A state-machine-invalid lifecycle command is therefore durably rejected without executing physical cutover/abort completion, while a state-machine-valid command with a mismatched physical slot still returns fatal `BackendSlotMismatch`.
3. `DurableRaftReplica::propose` decodes the command envelope and rejects an outer/envelope request-ID mismatch through `DurableReplicaError::RequestEnvelopeMismatch` before calling Raft proposal.

Added regressions:

- `committed_applied_duplicate_advances_after_epoch_activation`
- `committed_rejected_duplicate_advances_after_epoch_activation`
- `rejected_committed_entry_does_not_block_the_following_entry_in_a_ready_batch` now uses an idle cutover whose source/target generations also mismatch the physical slot, proving local validation cannot preempt durable business rejection.
- `propose_rejects_request_id_that_differs_from_the_command_envelope`

Final-review red phase:

- The two epoch duplicate tests failed with `StaleEpoch { expected: 10, actual: 9 }` before authority validation was reordered.
- The durable proposal test failed to compile because `DurableReplicaError::RequestEnvelopeMismatch` did not exist.
- The strengthened idle cutover case was blocked by the same compile failure; under the old ordering it would return `BackendSlotMismatch` before state-machine rejection.

Focused green phase:

- State-machine committed regressions: 5 passed, 0 failed.
- Durable invalid-lifecycle/following-entry regression: 1 passed, 0 failed.
- Durable proposal request-ID mismatch regression: 1 passed, 0 failed.

Fresh final verification after all review fixes:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p shard-runtime
```

Result: 47 passed, 0 failed. `rustfmt --edition 2024 --check` and `git diff --check` also passed.
