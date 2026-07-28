use std::collections::BTreeMap;

use dtg_kernel::{Digest32, TransactionId, TransactionTime, ValidInterval, Value, Version};

use crate::{
    AdjacencyDirection, AdjacencyRead, CapabilityManifest, ChangeCursor, CommandId,
    CommittedShardBatch, EdgeId, EdgeRead, EdgeScan, EdgeTombstone, EdgeVersion, LogicalMutation,
    LogicalSnapshotSink, LogicalSnapshotSource, PushdownExecutor, PushdownOperation,
    PushdownOutcome, PushdownRequest, ReadFence, ReplicaBinding, ReplicaMetadata,
    ReplicaStateStore, SUPPORTED_PUSHDOWN_CONTRACT_VERSION, SnapshotRecord, SnapshotReplayRecord,
    SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture, TransactionRecord,
    TransactionState, VertexId, VertexRead, VertexScan, VertexTombstone, VertexVersion,
};

/// Certification-only adapter implemented by provider test harnesses.
///
/// Production store types need not expose fault controls through
/// [`ReplicaStateStore`]; a provider may wrap its store for TCK execution.
pub trait StorageTckStore:
    ReplicaStateStore + LogicalSnapshotSource + LogicalSnapshotSink + PushdownExecutor
{
    /// TCK-only fault control. The next apply must fail after this many
    /// mutations have been staged privately and before any state is published.
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError>;
}

pub trait StorageTckFactory: Send + Sync {
    fn capabilities(&self) -> CapabilityManifest;
    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError>;
    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>>;
}

pub fn run_storage_tck(factory: &dyn StorageTckFactory) -> StoreFuture<'_, ()> {
    Box::pin(async move {
        let capabilities = factory.capabilities();
        let primary_binding = factory.binding("tck-primary", 1)?;
        require(
            primary_binding.capability_digest() == capabilities.digest(),
            "factory binding capability digest differs from its manifest",
        )?;
        let primary = factory.open(primary_binding.clone()).await?;
        require(
            ReplicaStateStore::binding(&*primary) == &primary_binding,
            "opened store returned a different binding",
        )?;
        require(
            PushdownExecutor::binding(&*primary) == &primary_binding,
            "pushdown surface returned a different binding",
        )?;
        require(
            primary.applied_index().await? == 0,
            "new store is not empty",
        )?;

        let initial_fence = ReadFence::new(primary_binding.clone(), 0);
        let before = primary.begin_read_view(initial_fence).await?;
        let first_vertex = sample_vertex(u128::from(u64::MAX) + 101, 1, "first")?;
        let second_vertex = sample_vertex(u128::from(u64::MAX) + 102, 1, "second")?;
        let first_edge = sample_edge(
            u128::from(u64::MAX) + 201,
            first_vertex.id(),
            second_vertex.id(),
            "first-edge",
        )?;
        let latest_first_edge = sample_edge_version(
            u128::from(u64::MAX) + 201,
            first_vertex.id(),
            second_vertex.id(),
            "first-edge-corrected",
            2,
        )?;
        let invisible_newest_first_edge = sample_edge_temporal_version(
            u128::from(u64::MAX) + 201,
            first_vertex.id(),
            second_vertex.id(),
            "first-edge-future",
            3,
            20,
        )?;
        let remote_vertex = VertexId::new(u128::from(u64::MAX) + 103)?;
        let second_edge = sample_edge(
            u128::from(u64::MAX) + 202,
            second_vertex.id(),
            remote_vertex,
            "second-edge",
        )?;
        let transaction = sample_transaction(301)?;
        let metadata = ReplicaMetadata::new(
            "lease-owner",
            Value::String("replica-tck-primary".to_owned()),
        )?;
        let mut expected_snapshot_records = vec![
            SnapshotRecord::Vertex(first_vertex.clone()),
            SnapshotRecord::Vertex(second_vertex.clone()),
            SnapshotRecord::Edge(first_edge.clone()),
            SnapshotRecord::Edge(latest_first_edge.clone()),
            SnapshotRecord::Edge(invisible_newest_first_edge.clone()),
            SnapshotRecord::Edge(second_edge.clone()),
            SnapshotRecord::Transaction(transaction.clone()),
            SnapshotRecord::ReplicaMetadata(metadata.clone()),
        ];
        let first_read = VertexRead::new(
            first_vertex.id(),
            10,
            TransactionTime::new(10).map_err(kernel_error)?,
        );
        require(
            before.get_vertex(first_read.clone()).await?.is_none(),
            "empty read view exposed a vertex",
        )?;

        let command_id = CommandId::new(7001)?;
        let first_batch = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            1,
            command_id,
            vec![
                LogicalMutation::PutVertex(first_vertex.clone()),
                LogicalMutation::PutVertex(second_vertex.clone()),
                LogicalMutation::PutEdge(first_edge.clone()),
                LogicalMutation::PutEdge(latest_first_edge.clone()),
                LogicalMutation::PutEdge(invisible_newest_first_edge.clone()),
                LogicalMutation::PutEdge(second_edge.clone()),
                LogicalMutation::PutTransaction(transaction.clone()),
                LogicalMutation::PutReplicaMetadata(metadata.clone()),
            ],
        )?;
        expected_snapshot_records.extend(snapshot_state_records(&first_batch)?);
        let receipt = primary.apply(first_batch.clone()).await?;
        require(!receipt.replayed(), "first apply was marked as replay")?;
        require(
            receipt.binding() == &primary_binding,
            "receipt lost binding identity",
        )?;
        require(receipt.raft_term() == 4, "receipt lost Raft term")?;
        require(receipt.raft_index() == 1, "receipt lost Raft index")?;
        require(
            receipt.command_id() == command_id,
            "receipt lost command identity",
        )?;
        require(
            receipt.mutation_digest() == first_batch.mutation_digest(),
            "receipt lost mutation digest",
        )?;
        require(
            primary.applied_index().await? == 1,
            "applied index did not advance",
        )?;

        require(
            before.get_vertex(first_read.clone()).await?.is_none(),
            "existing read view changed after apply",
        )?;
        let current_fence = ReadFence::new(primary_binding.clone(), 1);
        let current = primary.begin_read_view(current_fence.clone()).await?;
        require(
            current.get_vertex(first_read.clone()).await? == Some(first_vertex.clone()),
            "first mutation was not visible after atomic apply",
        )?;
        let second_read = VertexRead::new(
            second_vertex.id(),
            10,
            TransactionTime::new(10).map_err(kernel_error)?,
        );
        require(
            current.get_vertex(second_read).await? == Some(second_vertex.clone()),
            "second mutation was not visible after atomic apply",
        )?;
        require(
            current
                .get_edge(EdgeRead::new(
                    first_edge.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                == Some(latest_first_edge.clone()),
            "typed edge mutation was not visible after atomic apply",
        )?;

        let before_failed_apply =
            export_snapshot_records(&*primary, &primary_binding, 1, 8998, 16).await?;
        require_complete_snapshot_categories(&before_failed_apply)?;
        require(
            canonical_snapshot_records(before_failed_apply.clone())
                == canonical_snapshot_records(expected_snapshot_records.clone()),
            "snapshot export did not preserve seeded typed logical records",
        )?;
        let staged_vertex = sample_vertex(u128::from(u64::MAX) + 104, 1, "staged")?;
        let second_staged_vertex = sample_vertex(u128::from(u64::MAX) + 105, 1, "second-staged")?;
        primary.arm_apply_failure_after(1)?;
        let execution_failure = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            2,
            CommandId::new(7002)?,
            vec![
                LogicalMutation::PutVertex(staged_vertex.clone()),
                LogicalMutation::PutVertex(second_staged_vertex.clone()),
            ],
        )?;
        execution_failure.validate()?;
        require_error(
            primary.apply(execution_failure.clone()).await,
            |error| {
                matches!(
                    error,
                    StorageError::InjectedApplyFailure {
                        staged_mutations: 1
                    }
                )
            },
            "execution-stage mutation failure was accepted",
        )?;
        require(
            primary.applied_index().await? == 1,
            "execution-stage failure changed the applied index",
        )?;
        let after_failed_apply =
            export_snapshot_records(&*primary, &primary_binding, 1, 8999, 16).await?;
        require(
            canonical_snapshot_records(after_failed_apply)
                == canonical_snapshot_records(before_failed_apply.clone()),
            "execution-stage failure changed typed business state",
        )?;
        let after_failed_view = primary
            .begin_read_view(ReadFence::new(primary_binding.clone(), 1))
            .await?;
        require(
            after_failed_view
                .get_vertex(VertexRead::new(
                    staged_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "execution-stage failure leaked a staged vertex",
        )?;
        require(
            after_failed_view
                .get_vertex(VertexRead::new(
                    second_staged_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "execution-stage failure leaked a second staged vertex",
        )?;
        let after_failed_scan = after_failed_view
            .scan_vertices(VertexScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                None,
                16,
            )?)
            .await?;
        require(
            after_failed_scan.rows() == [first_vertex.clone(), second_vertex.clone()],
            "execution-stage failure changed logical scan visibility",
        )?;
        let after_failed_changes = after_failed_view
            .changes(crate::ChangesRead::new(None, 2, 32)?)
            .await?;
        require(
            after_failed_changes.rows().len() == first_batch.mutations().len()
                && after_failed_changes
                    .rows()
                    .iter()
                    .all(|change| change.raft_index() == 1),
            "execution-stage failure leaked change-index visibility",
        )?;

        let retry_receipt = primary.apply(execution_failure.clone()).await?;
        require(
            !retry_receipt.replayed(),
            "retry after injected failure was incorrectly marked as replay",
        )?;
        require(
            primary.applied_index().await? == 2,
            "retry after injected failure did not advance the applied index",
        )?;
        let committed_after_retry = primary
            .begin_read_view(ReadFence::new(primary_binding.clone(), 2))
            .await?;
        for vertex in [&staged_vertex, &second_staged_vertex] {
            require(
                committed_after_retry
                    .get_vertex(VertexRead::new(
                        vertex.id(),
                        10,
                        TransactionTime::new(10).map_err(kernel_error)?,
                    ))
                    .await?
                    == Some(vertex.clone()),
                "retry after injected failure did not publish every mutation",
            )?;
        }
        let retry_changes = committed_after_retry
            .changes(crate::ChangesRead::new(
                Some(ChangeCursor::new(1, u64::MAX)),
                2,
                16,
            )?)
            .await?;
        require(
            retry_changes.rows().len() == execution_failure.mutations().len()
                && retry_changes
                    .rows()
                    .iter()
                    .zip(execution_failure.mutations())
                    .all(|(change, mutation)| {
                        change.raft_index() == 2 && change.mutation() == mutation
                    }),
            "retry after injected failure did not publish its complete change set",
        )?;
        expected_snapshot_records.push(SnapshotRecord::Vertex(staged_vertex.clone()));
        expected_snapshot_records.push(SnapshotRecord::Vertex(second_staged_vertex.clone()));
        expected_snapshot_records.extend(snapshot_state_records(&execution_failure)?);
        let retry_replay = primary.apply(execution_failure).await?;
        require(
            retry_replay.replayed(),
            "successful retry was not subsequently idempotent",
        )?;
        require(
            primary.applied_index().await? == 2,
            "idempotent retry replay advanced the applied index",
        )?;
        let after_retry_replay = primary
            .begin_read_view(ReadFence::new(primary_binding.clone(), 2))
            .await?;
        let after_retry_replay_changes = after_retry_replay
            .changes(crate::ChangesRead::new(
                Some(ChangeCursor::new(1, u64::MAX)),
                2,
                16,
            )?)
            .await?;
        require(
            after_retry_replay_changes.rows() == retry_changes.rows(),
            "idempotent retry replay duplicated or reordered observable changes",
        )?;
        let drifted_fence =
            ReadFence::with_capability_digest(primary_binding.clone(), 2, Digest32::new([9; 32]));
        require_error(
            primary.begin_read_view(drifted_fence.clone()).await,
            |error| matches!(error, StorageError::CapabilityDrift),
            "read view accepted capability drift",
        )?;
        require_error(
            primary
                .begin_snapshot(drifted_fence, SnapshotRequest::new(9000, 1)?)
                .await,
            |error| matches!(error, StorageError::CapabilityDrift),
            "snapshot export accepted capability drift",
        )?;

        let replay = primary.apply(first_batch.clone()).await?;
        require(replay.replayed(), "identical replay was not idempotent")?;
        require(
            primary.applied_index().await? == 2,
            "replay advanced the index",
        )?;

        let mismatched_replay = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            1,
            command_id,
            vec![LogicalMutation::PutVertex(sample_vertex(
                103, 1, "mismatch",
            )?)],
        )?;
        require_error(
            primary.apply(mismatched_replay).await,
            |error| matches!(error, StorageError::ReplayMismatch { raft_index: 1 }),
            "mismatched replay was accepted",
        )?;

        let skipped_index = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            4,
            CommandId::new(7003)?,
            vec![LogicalMutation::PutVertex(sample_vertex(106, 1, "skip")?)],
        )?;
        require_error(
            primary.apply(skipped_index).await,
            |error| {
                matches!(
                    error,
                    StorageError::NonMonotonicIndex {
                        applied: 2,
                        proposed: 4
                    }
                )
            },
            "nonmonotonic Raft index was accepted",
        )?;

        let stale_binding = factory.binding("tck-stale", 2)?;
        let stale_batch = CommittedShardBatch::new(
            stale_binding,
            4,
            3,
            CommandId::new(7004)?,
            vec![LogicalMutation::PutVertex(sample_vertex(107, 1, "stale")?)],
        )?;
        require_error(
            primary.apply(stale_batch).await,
            |error| matches!(error, StorageError::StaleBinding { .. }),
            "stale binding was accepted",
        )?;
        require(
            primary.applied_index().await? == 2,
            "rejected apply changed the index",
        )?;
        let after_rejections = primary
            .begin_read_view(ReadFence::new(primary_binding.clone(), 2))
            .await?;
        for rejected_id in [103, 106, 107] {
            require(
                after_rejections
                    .get_vertex(VertexRead::new(
                        VertexId::new(rejected_id)?,
                        10,
                        TransactionTime::new(10).map_err(kernel_error)?,
                    ))
                    .await?
                    .is_none(),
                "rejected batch leaked a partial mutation",
            )?;
        }

        let point = CapabilityManifest::from_names(["point"])?;
        let exact_request = PushdownRequest::new(
            SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
            ReadFence::new(primary_binding.clone(), 2),
            point,
            PushdownOperation::Vertex(first_read),
        )?;
        match primary.execute_pushdown(exact_request).await? {
            PushdownOutcome::Exact(rows) if rows.len() == 1 => {}
            outcome => {
                return Err(StorageError::TckViolation(format!(
                    "supported point pushdown was not exact: {outcome:?}"
                )));
            }
        }

        let residual_request = PushdownRequest::new(
            SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
            ReadFence::new(primary_binding.clone(), 2),
            CapabilityManifest::from_names(["point", "typed-property-predicate"])?,
            PushdownOperation::Vertex(VertexRead::new(
                first_vertex.id(),
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
            )),
        )?;
        match primary.execute_pushdown(residual_request).await? {
            PushdownOutcome::ResidualRequired { rows, guarantees }
                if rows.len() == 1
                    && guarantees.supports("point")
                    && !guarantees.supports("typed-property-predicate") => {}
            outcome => {
                return Err(StorageError::TckViolation(format!(
                    "partial pushdown did not name its guarantees: {outcome:?}"
                )));
            }
        }

        let unsupported_request = PushdownRequest::new(
            SUPPORTED_PUSHDOWN_CONTRACT_VERSION,
            ReadFence::new(primary_binding.clone(), 2),
            CapabilityManifest::from_names(["selected-partial-aggregate"])?,
            PushdownOperation::VertexScan(VertexScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                None,
                16,
            )?),
        )?;
        require(
            matches!(
                primary.execute_pushdown(unsupported_request).await?,
                PushdownOutcome::Unsupported
            ),
            "unsupported capability did not fall back safely",
        )?;

        let first_vertex_page = current
            .scan_vertices(VertexScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                None,
                1,
            )?)
            .await?;
        let vertex_cursor = first_vertex_page.next_after().ok_or_else(|| {
            StorageError::TckViolation("vertex scan omitted its typed continuation".into())
        })?;
        require(
            first_vertex_page.rows().len() == 1
                && first_vertex_page.rows()[0] == first_vertex
                && vertex_cursor == first_vertex_page.rows()[0].id()
                && vertex_cursor.get() > u128::from(u64::MAX),
            "vertex scan cursor did not preserve the last returned 128-bit identity",
        )?;
        let second_vertex_page = current
            .scan_vertices(VertexScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                Some(vertex_cursor),
                1,
            )?)
            .await?;
        require(
            second_vertex_page.rows().len() == 1
                && second_vertex_page.rows()[0] == second_vertex
                && second_vertex_page.rows()[0].id() > vertex_cursor
                && second_vertex_page.next_after().is_none(),
            "vertex scan continuation did not construct the next request losslessly",
        )?;

        let large_edge_page = current
            .scan_edges(EdgeScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                None,
                16,
            )?)
            .await?;
        let mut paged_edges = Vec::new();
        let mut edge_after = None;
        loop {
            let page = current
                .scan_edges(EdgeScan::new(
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                    edge_after,
                    1,
                )?)
                .await?;
            paged_edges.extend_from_slice(page.rows());
            match page.next_after() {
                Some(cursor) => {
                    require(
                        page.rows().last().is_some_and(|edge| edge.id() == cursor)
                            && cursor.get() > u128::from(u64::MAX),
                        "edge scan cursor did not preserve the last returned 128-bit identity",
                    )?;
                    edge_after = Some(cursor);
                }
                None => break,
            }
        }
        require(
            large_edge_page.rows() == [latest_first_edge, second_edge.clone()]
                && paged_edges == large_edge_page.rows(),
            "edge scan pagination skipped, duplicated, or returned multiple versions per edge ID",
        )?;

        let large_change_page = current
            .changes(crate::ChangesRead::new(None, 1, 32)?)
            .await?;
        let mut paged_changes = Vec::new();
        let mut change_after = None;
        loop {
            let page = current
                .changes(crate::ChangesRead::new(change_after, 1, 1)?)
                .await?;
            paged_changes.extend_from_slice(page.rows());
            match page.next_after() {
                Some(cursor) => change_after = Some(cursor),
                None => break,
            }
        }
        require(
            paged_changes == large_change_page.rows(),
            "change pagination lost or duplicated mutations sharing one Raft index",
        )?;

        let deleted_vertex = VertexTombstone::new(
            second_staged_vertex.id(),
            Version::new(2),
            TransactionTime::new(3).map_err(kernel_error)?,
        );
        let deleted_edge = EdgeTombstone::new(
            second_edge.id(),
            Version::new(2),
            TransactionTime::new(3).map_err(kernel_error)?,
        );
        let delete_batch = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            3,
            CommandId::new(7003)?,
            vec![
                LogicalMutation::DeleteVertex(deleted_vertex.clone()),
                LogicalMutation::DeleteEdge(deleted_edge.clone()),
            ],
        )?;
        primary.apply(delete_batch.clone()).await?;
        expected_snapshot_records.push(SnapshotRecord::VertexTombstone(deleted_vertex));
        expected_snapshot_records.push(SnapshotRecord::EdgeTombstone(deleted_edge));
        expected_snapshot_records.extend(snapshot_state_records(&delete_batch)?);

        let mut reader = primary
            .begin_snapshot(
                ReadFence::new(primary_binding.clone(), 3),
                SnapshotRequest::new(9001, 1)?,
            )
            .await?;
        let header = reader.header().clone();
        let mut chunks = Vec::new();
        while let Some(chunk) = reader.next_chunk().await? {
            chunks.push(chunk);
        }
        let manifest = reader.finish().await?;
        require(
            chunks.len() == expected_snapshot_records.len(),
            "snapshot chunk bound was not honored",
        )?;

        let corrupt_binding = factory.binding("tck-corrupt-target", 2)?;
        let corrupt_target = factory.open(corrupt_binding.clone()).await?;
        let mut corrupt_writer = corrupt_target
            .begin_restore(corrupt_binding, header.clone())
            .await?;
        let mut corrupt_chunk = chunks[0].clone();
        corrupt_chunk.digest = Digest32::new([0; 32]);
        require_error(
            corrupt_writer.write_chunk(corrupt_chunk).await,
            |error| matches!(error, StorageError::CorruptSnapshot(_)),
            "corrupt snapshot chunk was accepted",
        )?;
        corrupt_writer.abort().await?;

        let restored_binding = factory.binding("tck-restored", 2)?;
        let restored = factory.open(restored_binding.clone()).await?;
        let dirty_vertex = sample_vertex(999, 1, "dirty-target")?;
        restored
            .apply(CommittedShardBatch::new(
                restored_binding.clone(),
                9,
                1,
                CommandId::new(9901)?,
                vec![LogicalMutation::PutVertex(dirty_vertex.clone())],
            )?)
            .await?;
        let mut writer = restored
            .begin_restore(restored_binding.clone(), header)
            .await?;
        for chunk in chunks {
            writer.write_chunk(chunk).await?;
        }
        let restore_receipt: SnapshotRestoreReceipt = writer.commit(manifest.clone()).await?;
        require(
            restore_receipt.binding() == &restored_binding,
            "restore receipt lost target binding",
        )?;
        require(
            restore_receipt.manifest() == &manifest,
            "restore receipt lost manifest identity",
        )?;
        let restored_export =
            export_snapshot_records(&*restored, &restored_binding, 3, 9002, 2).await?;
        require_complete_snapshot_categories(&restored_export)?;
        require(
            restored_export
                .iter()
                .any(|record| matches!(record, SnapshotRecord::VertexTombstone(_)))
                && restored_export
                    .iter()
                    .any(|record| matches!(record, SnapshotRecord::EdgeTombstone(_))),
            "snapshot omitted vertex or edge deletion history",
        )?;
        require(
            canonical_snapshot_records(restored_export)
                == canonical_snapshot_records(expected_snapshot_records.clone()),
            "logical snapshot did not round-trip all typed record categories",
        )?;
        let restored_view = restored
            .begin_read_view(ReadFence::new(restored_binding.clone(), 3))
            .await?;
        require(
            restored_view
                .get_vertex(VertexRead::new(
                    dirty_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "snapshot restore merged dirty target state instead of replacing it",
        )?;
        require(
            restored_view
                .get_vertex(VertexRead::new(
                    second_staged_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none()
                && restored_view
                    .get_edge(EdgeRead::new(
                        second_edge.id(),
                        10,
                        TransactionTime::new(10).map_err(kernel_error)?,
                    ))
                    .await?
                    .is_none(),
            "snapshot restore resurrected deleted logical records",
        )?;
        let replay_delete = CommittedShardBatch::new(
            restored_binding.clone(),
            delete_batch.raft_term(),
            delete_batch.raft_index(),
            delete_batch.command_id(),
            delete_batch.mutations().to_vec(),
        )?;
        require(
            restored.apply(replay_delete).await?.replayed(),
            "snapshot restore did not preserve replay identity",
        )?;
        let continued_vertex = sample_vertex(1000, 1, "continued")?;
        restored
            .apply(CommittedShardBatch::new(
                restored_binding.clone(),
                5,
                4,
                CommandId::new(7004)?,
                vec![LogicalMutation::PutVertex(continued_vertex.clone())],
            )?)
            .await?;
        let reopened = factory.open(restored_binding.clone()).await?;
        let reopened_view = reopened
            .begin_read_view(ReadFence::new(restored_binding, 4))
            .await?;
        require(
            reopened_view
                .get_vertex(VertexRead::new(
                    continued_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                == Some(continued_vertex),
            "restored state did not survive reopen and continued writes",
        )?;

        let isolated_binding = factory.binding("tck-isolated", 1)?;
        let isolated = factory.open(isolated_binding.clone()).await?;
        let isolated_view = isolated
            .begin_read_view(ReadFence::new(isolated_binding, 0))
            .await?;
        require(
            isolated_view
                .get_vertex(VertexRead::new(
                    first_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "state crossed namespace boundaries",
        )?;
        let isolated_vertex = sample_vertex(201, 1, "isolated")?;
        isolated
            .apply(CommittedShardBatch::new(
                factory.binding("tck-isolated", 1)?,
                5,
                1,
                CommandId::new(8001)?,
                vec![LogicalMutation::PutVertex(isolated_vertex.clone())],
            )?)
            .await?;
        let isolated_after_write = isolated
            .begin_read_view(ReadFence::new(factory.binding("tck-isolated", 1)?, 1))
            .await?;
        require(
            isolated_after_write
                .get_vertex(VertexRead::new(
                    isolated_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                == Some(isolated_vertex.clone()),
            "isolated namespace did not retain its own write",
        )?;
        require(
            after_rejections
                .get_vertex(VertexRead::new(
                    isolated_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "isolated namespace write leaked into the primary namespace",
        )?;

        let adjacency_binding = factory.binding("tck-adjacency", 1)?;
        let adjacency_store = factory.open(adjacency_binding.clone()).await?;
        let old_source = VertexId::new(501)?;
        let old_target = VertexId::new(502)?;
        let new_source = VertexId::new(503)?;
        let new_target = VertexId::new(504)?;
        let old_edge = sample_edge_version(601, old_source, old_target, "moved", 1)?;
        let moved_edge = sample_edge_version(601, new_source, new_target, "moved", 2)?;
        adjacency_store
            .apply(CommittedShardBatch::new(
                adjacency_binding.clone(),
                1,
                1,
                CommandId::new(9601)?,
                vec![
                    LogicalMutation::PutEdge(old_edge),
                    LogicalMutation::PutEdge(moved_edge.clone()),
                ],
            )?)
            .await?;
        let moved_view = adjacency_store
            .begin_read_view(ReadFence::new(adjacency_binding.clone(), 1))
            .await?;
        require(
            moved_view
                .expand(AdjacencyRead::new(
                    old_source,
                    AdjacencyDirection::Outgoing,
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                    10,
                )?)
                .await?
                .is_empty(),
            "adjacency retained an edge under its superseded endpoint",
        )?;
        require(
            moved_view
                .expand(AdjacencyRead::new(
                    new_source,
                    AdjacencyDirection::Outgoing,
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                    10,
                )?)
                .await?
                == [moved_edge.clone()],
            "adjacency did not resolve the latest moved edge version",
        )?;
        adjacency_store
            .apply(CommittedShardBatch::new(
                adjacency_binding.clone(),
                1,
                2,
                CommandId::new(9602)?,
                vec![
                    LogicalMutation::PutEdge(sample_edge_version(
                        601, new_source, new_target, "moved", 3,
                    )?),
                    LogicalMutation::DeleteEdge(EdgeTombstone::new(
                        moved_edge.id(),
                        Version::new(4),
                        TransactionTime::new(4).map_err(kernel_error)?,
                    )),
                ],
            )?)
            .await?;
        let deleted_view = adjacency_store
            .begin_read_view(ReadFence::new(adjacency_binding, 2))
            .await?;
        require(
            deleted_view
                .expand(AdjacencyRead::new(
                    new_source,
                    AdjacencyDirection::Both,
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                    10,
                )?)
                .await?
                .is_empty(),
            "adjacency returned an edge whose latest event is a tombstone",
        )?;

        let wrong_owner = primary_binding.to_builder().backend_generation(2).build()?;
        require_error(
            factory.open(wrong_owner).await,
            |error| matches!(error, StorageError::NamespaceOwnerMismatch { .. }),
            "namespace reopened under a different binding",
        )?;
        Ok(())
    })
}

fn sample_vertex(id: u128, version: u64, name: &str) -> Result<VertexVersion, StorageError> {
    VertexVersion::new(
        VertexId::new(id)?,
        Version::new(version),
        ValidInterval::new(0, 100).map_err(kernel_error)?,
        TransactionTime::new(version as i64).map_err(kernel_error)?,
        BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
    )
}

fn sample_edge(
    id: u128,
    source: VertexId,
    target: VertexId,
    edge_type: &str,
) -> Result<EdgeVersion, StorageError> {
    sample_edge_version(id, source, target, edge_type, 1)
}

fn sample_edge_version(
    id: u128,
    source: VertexId,
    target: VertexId,
    edge_type: &str,
    version: u64,
) -> Result<EdgeVersion, StorageError> {
    sample_edge_temporal_version(id, source, target, edge_type, version, version as i64)
}

fn sample_edge_temporal_version(
    id: u128,
    source: VertexId,
    target: VertexId,
    edge_type: &str,
    version: u64,
    transaction_time: i64,
) -> Result<EdgeVersion, StorageError> {
    EdgeVersion::new(
        EdgeId::new(id)?,
        source,
        target,
        edge_type,
        Version::new(version),
        ValidInterval::new(0, 100).map_err(kernel_error)?,
        TransactionTime::new(transaction_time).map_err(kernel_error)?,
        BTreeMap::from([("weight".to_owned(), Value::Integer(7))]),
    )
}

fn sample_transaction(id: u128) -> Result<TransactionRecord, StorageError> {
    TransactionRecord::new(
        TransactionId::new(id).map_err(kernel_error)?,
        TransactionState::Committed,
        TransactionTime::new(1).map_err(kernel_error)?,
        Digest32::new([7; 32]),
    )
}

fn snapshot_state_records(
    batch: &CommittedShardBatch,
) -> Result<Vec<SnapshotRecord>, StorageError> {
    let mut records = Vec::with_capacity(batch.mutations().len() + 1);
    records.push(SnapshotRecord::Replay(SnapshotReplayRecord::new(
        batch.raft_index(),
        batch.raft_term(),
        batch.command_id(),
        batch.mutation_digest(),
    )?));
    records.extend(
        batch
            .mutations()
            .iter()
            .cloned()
            .enumerate()
            .map(|(ordinal, mutation)| {
                SnapshotRecord::Change(crate::ChangeRecord::new(
                    ChangeCursor::new(batch.raft_index(), ordinal as u64),
                    mutation,
                ))
            }),
    );
    Ok(records)
}

async fn export_snapshot_records(
    store: &dyn StorageTckStore,
    binding: &ReplicaBinding,
    applied_index: u64,
    snapshot_id: u128,
    max_records_per_chunk: u32,
) -> Result<Vec<SnapshotRecord>, StorageError> {
    let mut reader = store
        .begin_snapshot(
            ReadFence::new(binding.clone(), applied_index),
            SnapshotRequest::new(snapshot_id, max_records_per_chunk)?,
        )
        .await?;
    let mut chunks = Vec::new();
    while let Some(chunk) = reader.next_chunk().await? {
        chunks.push(chunk);
    }
    reader.finish().await?;
    let records = chunks
        .iter()
        .flat_map(|chunk| chunk.records().iter().cloned())
        .collect();
    Ok(records)
}

fn require_complete_snapshot_categories(records: &[SnapshotRecord]) -> Result<(), StorageError> {
    let mut vertices = false;
    let mut edges = false;
    let mut transactions = false;
    let mut metadata = false;
    let mut replay = false;
    let mut changes = false;
    for record in records {
        match record {
            SnapshotRecord::Vertex(_) => vertices = true,
            SnapshotRecord::VertexTombstone(_) => {}
            SnapshotRecord::Edge(_) => edges = true,
            SnapshotRecord::EdgeTombstone(_) => {}
            SnapshotRecord::Transaction(_) => transactions = true,
            SnapshotRecord::ReplicaMetadata(_) => metadata = true,
            SnapshotRecord::Replay(_) => replay = true,
            SnapshotRecord::Change(_) => changes = true,
        }
    }
    require(
        vertices && edges && transactions && metadata && replay && changes,
        "snapshot omitted a required typed logical record category",
    )
}

fn canonical_snapshot_records(mut records: Vec<SnapshotRecord>) -> Vec<SnapshotRecord> {
    records.sort_by_key(snapshot_record_key);
    records
}

fn snapshot_record_key(record: &SnapshotRecord) -> (u8, u128, u64, i64, String) {
    match record {
        SnapshotRecord::Vertex(vertex) => (
            1,
            vertex.id().get(),
            vertex.version().get(),
            vertex.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::Edge(edge) => (
            2,
            edge.id().get(),
            edge.version().get(),
            edge.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::VertexTombstone(tombstone) => (
            3,
            tombstone.id().get(),
            tombstone.version().get(),
            tombstone.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::EdgeTombstone(tombstone) => (
            4,
            tombstone.id().get(),
            tombstone.version().get(),
            tombstone.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::Transaction(transaction) => (
            5,
            transaction.id().get(),
            0,
            transaction.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::ReplicaMetadata(metadata) => (6, 0, 0, 0, metadata.name().to_owned()),
        SnapshotRecord::Replay(replay) => (
            7,
            u128::from(replay.raft_index()),
            replay.raft_term(),
            0,
            replay.command_id().get().to_string(),
        ),
        SnapshotRecord::Change(change) => (
            8,
            u128::from(change.raft_index()),
            change.mutation_ordinal(),
            0,
            format!("{:?}", change.mutation()),
        ),
    }
}

fn require(condition: bool, message: &str) -> Result<(), StorageError> {
    if condition {
        Ok(())
    } else {
        Err(StorageError::TckViolation(message.into()))
    }
}

fn require_error<T, F>(
    result: Result<T, StorageError>,
    predicate: F,
    message: &str,
) -> Result<(), StorageError>
where
    F: FnOnce(&StorageError) -> bool,
{
    match result {
        Err(error) if predicate(&error) => Ok(()),
        Err(error) => Err(StorageError::TckViolation(format!(
            "{message}; received {error}"
        ))),
        Ok(_) => Err(StorageError::TckViolation(message.into())),
    }
}

fn kernel_error(error: dtg_kernel::KernelError) -> StorageError {
    StorageError::InvalidMutation(error.to_string())
}
