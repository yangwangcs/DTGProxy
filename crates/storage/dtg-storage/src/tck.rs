use std::collections::BTreeMap;

use dtg_kernel::{Digest32, TransactionId, TransactionTime, ValidInterval, Value, Version};

use crate::{
    CapabilityManifest, CommandId, CommittedShardBatch, EdgeId, EdgeRead, EdgeScan, EdgeVersion,
    LogicalMutation, LogicalSnapshotSink, LogicalSnapshotSource, PushdownExecutor,
    PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence, ReplicaBinding,
    ReplicaMetadata, ReplicaStateStore, SUPPORTED_PUSHDOWN_CONTRACT_VERSION, SnapshotRecord,
    SnapshotRequest, SnapshotRestoreReceipt, StorageError, StoreFuture, TransactionRecord,
    TransactionState, VertexId, VertexRead, VertexScan, VertexVersion,
};

pub trait StorageTckStore:
    ReplicaStateStore + LogicalSnapshotSource + LogicalSnapshotSink + PushdownExecutor
{
}

impl<T> StorageTckStore for T where
    T: ReplicaStateStore + LogicalSnapshotSource + LogicalSnapshotSink + PushdownExecutor
{
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
        let second_edge = sample_edge(
            u128::from(u64::MAX) + 202,
            second_vertex.id(),
            first_vertex.id(),
            "second-edge",
        )?;
        let transaction = sample_transaction(301)?;
        let metadata = ReplicaMetadata::new(
            "lease-owner",
            Value::String("replica-tck-primary".to_owned()),
        )?;
        let expected_snapshot_records = vec![
            SnapshotRecord::Vertex(first_vertex.clone()),
            SnapshotRecord::Vertex(second_vertex.clone()),
            SnapshotRecord::Edge(first_edge.clone()),
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
                LogicalMutation::PutEdge(second_edge.clone()),
                LogicalMutation::PutTransaction(transaction.clone()),
                LogicalMutation::PutReplicaMetadata(metadata.clone()),
            ],
        )?;
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
                == Some(first_edge.clone()),
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
        let staged_vertex = sample_vertex(u128::from(u64::MAX) + 103, 1, "staged")?;
        let missing_vertex = VertexId::new(u128::from(u64::MAX) + 104)?;
        let invalid_edge = sample_edge(
            u128::from(u64::MAX) + 203,
            staged_vertex.id(),
            missing_vertex,
            "invalid-edge",
        )?;
        let execution_failure = CommittedShardBatch::new(
            primary_binding.clone(),
            4,
            2,
            CommandId::new(7002)?,
            vec![
                LogicalMutation::PutVertex(staged_vertex.clone()),
                LogicalMutation::PutEdge(invalid_edge.clone()),
            ],
        )?;
        execution_failure.validate()?;
        require_error(
            primary.apply(execution_failure).await,
            |error| matches!(error, StorageError::ConstraintViolation(_)),
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
                .get_edge(EdgeRead::new(
                    invalid_edge.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                .is_none(),
            "execution-stage failure leaked a staged edge",
        )?;

        let drifted_fence =
            ReadFence::with_capability_digest(primary_binding.clone(), 1, Digest32::new([9; 32]));
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
            primary.applied_index().await? == 1,
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
            3,
            CommandId::new(7003)?,
            vec![LogicalMutation::PutVertex(sample_vertex(104, 1, "skip")?)],
        )?;
        require_error(
            primary.apply(skipped_index).await,
            |error| {
                matches!(
                    error,
                    StorageError::NonMonotonicIndex {
                        applied: 1,
                        proposed: 3
                    }
                )
            },
            "nonmonotonic Raft index was accepted",
        )?;

        let stale_binding = factory.binding("tck-stale", 2)?;
        let stale_batch = CommittedShardBatch::new(
            stale_binding,
            4,
            2,
            CommandId::new(7002)?,
            vec![LogicalMutation::PutVertex(sample_vertex(105, 1, "stale")?)],
        )?;
        require_error(
            primary.apply(stale_batch).await,
            |error| matches!(error, StorageError::StaleBinding { .. }),
            "stale binding was accepted",
        )?;
        require(
            primary.applied_index().await? == 1,
            "rejected apply changed the index",
        )?;
        let after_rejections = primary
            .begin_read_view(ReadFence::new(primary_binding.clone(), 1))
            .await?;
        for rejected_id in [103, 104, 105] {
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
            current_fence.clone(),
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
            current_fence.clone(),
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
            current_fence.clone(),
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

        let first_edge_page = current
            .scan_edges(EdgeScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                None,
                1,
            )?)
            .await?;
        let edge_cursor = first_edge_page.next_after().ok_or_else(|| {
            StorageError::TckViolation("edge scan omitted its typed continuation".into())
        })?;
        require(
            first_edge_page.rows().len() == 1
                && first_edge_page.rows()[0] == first_edge
                && edge_cursor == first_edge_page.rows()[0].id()
                && edge_cursor.get() > u128::from(u64::MAX),
            "edge scan cursor did not preserve the last returned 128-bit identity",
        )?;
        let second_edge_page = current
            .scan_edges(EdgeScan::new(
                10,
                TransactionTime::new(10).map_err(kernel_error)?,
                Some(edge_cursor),
                1,
            )?)
            .await?;
        require(
            second_edge_page.rows().len() == 1
                && second_edge_page.rows()[0] == second_edge
                && second_edge_page.rows()[0].id() > edge_cursor
                && second_edge_page.next_after().is_none(),
            "edge scan continuation did not construct the next request losslessly",
        )?;

        let mut reader = primary
            .begin_snapshot(current_fence, SnapshotRequest::new(9001, 1)?)
            .await?;
        let header = reader.header().clone();
        let mut chunks = Vec::new();
        while let Some(chunk) = reader.next_chunk().await? {
            chunks.push(chunk);
        }
        let manifest = reader.finish().await?;
        require(
            chunks.len() == before_failed_apply.len(),
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
            export_snapshot_records(&*restored, &restored_binding, 1, 9002, 2).await?;
        require_complete_snapshot_categories(&restored_export)?;
        require(
            canonical_snapshot_records(restored_export)
                == canonical_snapshot_records(expected_snapshot_records),
            "logical snapshot did not round-trip all typed record categories",
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
    EdgeVersion::new(
        EdgeId::new(id)?,
        source,
        target,
        edge_type,
        Version::new(1),
        ValidInterval::new(0, 100).map_err(kernel_error)?,
        TransactionTime::new(1).map_err(kernel_error)?,
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
    for record in records {
        match record {
            SnapshotRecord::Vertex(_) => vertices = true,
            SnapshotRecord::Edge(_) => edges = true,
            SnapshotRecord::Transaction(_) => transactions = true,
            SnapshotRecord::ReplicaMetadata(_) => metadata = true,
        }
    }
    require(
        vertices && edges && transactions && metadata,
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
        SnapshotRecord::Transaction(transaction) => (
            3,
            transaction.id().get(),
            0,
            transaction.transaction_time().get(),
            String::new(),
        ),
        SnapshotRecord::ReplicaMetadata(metadata) => (4, 0, 0, 0, metadata.name().to_owned()),
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
