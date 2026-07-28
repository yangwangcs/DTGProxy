use std::collections::BTreeMap;

use dtg_kernel::{Digest32, TransactionTime, ValidInterval, Value, Version};

use crate::{
    CapabilityManifest, CommandId, CommittedShardBatch, LogicalMutation, LogicalSnapshotSink,
    LogicalSnapshotSource, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest,
    ReadFence, ReplicaBinding, ReplicaStateStore, SnapshotRequest, SnapshotRestoreReceipt,
    StorageError, StoreFuture, VertexId, VertexRead, VertexScan, VertexVersion,
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
        let first_vertex = sample_vertex(101, 1, "first")?;
        let second_vertex = sample_vertex(102, 1, "second")?;
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
            1,
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
            1,
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
            1,
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

        let mut reader = primary
            .begin_snapshot(current_fence, SnapshotRequest::new(9001, 1)?)
            .await?;
        let header = reader.header().clone();
        let mut chunks = Vec::new();
        while let Some(chunk) = reader.next_chunk().await? {
            chunks.push(chunk);
        }
        let manifest = reader.finish().await?;
        require(chunks.len() == 2, "snapshot chunk bound was not honored")?;

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
        let restored_view = restored
            .begin_read_view(ReadFence::new(restored_binding.clone(), 1))
            .await?;
        require(
            restored_view
                .get_vertex(VertexRead::new(
                    first_vertex.id(),
                    10,
                    TransactionTime::new(10).map_err(kernel_error)?,
                ))
                .await?
                == Some(first_vertex),
            "logical snapshot did not round-trip typed state",
        )?;

        let isolated_binding = factory.binding("tck-isolated", 1)?;
        let isolated = factory.open(isolated_binding.clone()).await?;
        let isolated_view = isolated
            .begin_read_view(ReadFence::new(isolated_binding, 0))
            .await?;
        require(
            isolated_view
                .get_vertex(VertexRead::new(
                    VertexId::new(101)?,
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
