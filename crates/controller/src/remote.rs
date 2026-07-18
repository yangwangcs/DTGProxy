use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cluster_protocol::CLUSTER_PROTOCOL_VERSION;
use cluster_protocol::proto::meta_service_client::MetaServiceClient;
use cluster_protocol::proto::node_admin_service_client::NodeAdminServiceClient;
use cluster_protocol::proto::shard_service_client::ShardServiceClient;
use cluster_protocol::proto::{
    AcquireControllerLeaseRequest, ActivateReplicaRequest, ChangeMembershipRequest,
    DeleteReplicaRequest, EnsureReplicaRequest, ExecuteRequest, ExportSnapshotRequest,
    GetCatalogRequest, ProposeRequest, ReplicaRole, ReplicaStatusRequest, RequestContext,
    ShardContext,
};
use control_plane::{CatalogCommand, CatalogState, GraphDefinition, MigrationRecord};
use raft_command::{CommandBodyV1, CommandEnvelopeV1};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::Request;

use crate::{CatalogApi, ControllerError, DataPlaneApi, SnapshotFence};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerLease {
    pub owner_term: u64,
    pub expires_unix_ms: u64,
}

#[derive(Clone)]
pub struct RemoteCatalog {
    cluster_id: [u8; 16],
    controller_id: u64,
    meta_seeds: Vec<SocketAddr>,
    request_timeout: Duration,
    request_sequence: Arc<AtomicU64>,
}

impl RemoteCatalog {
    pub fn new(
        cluster_id: [u8; 16],
        controller_id: u64,
        meta_seeds: Vec<SocketAddr>,
        request_timeout: Duration,
    ) -> Result<Self, ControllerError> {
        if cluster_id == [0; 16]
            || controller_id == 0
            || meta_seeds.is_empty()
            || request_timeout.is_zero()
        {
            return Err(ControllerError::Catalog(
                "invalid remote Catalog configuration".into(),
            ));
        }
        Ok(Self {
            cluster_id,
            controller_id,
            meta_seeds,
            request_timeout,
            request_sequence: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn acquire_lease(&self) -> Result<ControllerLease, ControllerError> {
        let request_id = self.next_request_id();
        let request = AcquireControllerLeaseRequest {
            context: Some(self.context(request_id)?),
            controller_id: self.controller_id,
        };
        let response = self
            .with_meta(|mut client| {
                let request = request.clone();
                async move { client.acquire_controller_lease(Request::new(request)).await }
            })
            .await?
            .into_inner();
        if response.owner_term == 0 || response.lease_expires_unix_ms <= now_ms()? {
            return Err(ControllerError::Catalog(
                "Meta returned an invalid Controller lease".into(),
            ));
        }
        Ok(ControllerLease {
            owner_term: response.owner_term,
            expires_unix_ms: response.lease_expires_unix_ms,
        })
    }

    async fn with_meta<F, Fut, T>(&self, call: F) -> Result<T, ControllerError>
    where
        F: Fn(MetaServiceClient<tonic::transport::Channel>) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut errors = Vec::new();
        for endpoint in &self.meta_seeds {
            match MetaServiceClient::connect(format!("http://{endpoint}")).await {
                Ok(client) => match call(client).await {
                    Ok(response) => return Ok(response),
                    Err(error) => errors.push(error.to_string()),
                },
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(ControllerError::Catalog(format!(
            "Meta quorum unavailable: {}",
            errors.join("; ")
        )))
    }

    fn next_request_id(&self) -> u128 {
        (u128::from(self.controller_id) << 64)
            | u128::from(self.request_sequence.fetch_add(1, Ordering::Relaxed))
    }

    fn context(&self, request_id: u128) -> Result<RequestContext, ControllerError> {
        Ok(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: now_ms()?
                .checked_add(self.request_timeout.as_millis() as u64)
                .ok_or_else(|| ControllerError::Catalog("request deadline overflow".into()))?,
        })
    }
}

impl CatalogApi for RemoteCatalog {
    async fn load(&self) -> Result<CatalogState, ControllerError> {
        let request = GetCatalogRequest {
            context: Some(self.context(self.next_request_id())?),
            minimum_revision: 1,
        };
        let snapshot = self
            .with_meta(|mut client| {
                let request = request.clone();
                async move { client.get_catalog(Request::new(request)).await }
            })
            .await?
            .into_inner()
            .snapshot
            .ok_or_else(|| ControllerError::Catalog("Meta omitted Catalog snapshot".into()))?;
        if crc32fast::hash(&snapshot.payload) != snapshot.checksum {
            return Err(ControllerError::Catalog(
                "Catalog snapshot checksum mismatch".into(),
            ));
        }
        let state = CatalogState::decode_snapshot(&snapshot.payload)
            .map_err(|error| ControllerError::Catalog(error.to_string()))?;
        if state.revision() != snapshot.revision {
            return Err(ControllerError::Catalog(
                "Catalog snapshot revision mismatch".into(),
            ));
        }
        Ok(state)
    }

    async fn propose(&self, command: CatalogCommand) -> Result<(), ControllerError> {
        let request = ProposeRequest {
            context: Some(self.context(command.command_id())?),
            command: command
                .encode()
                .map_err(|error| ControllerError::Catalog(error.to_string()))?,
        };
        self.with_meta(|mut client| {
            let request = request.clone();
            async move { client.propose(Request::new(request)).await }
        })
        .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct RemoteDataPlane {
    cluster_id: [u8; 16],
    data_nodes: BTreeMap<u64, SocketAddr>,
    request_timeout: Duration,
}

impl RemoteDataPlane {
    pub fn new(
        cluster_id: [u8; 16],
        controller_id: u64,
        data_nodes: BTreeMap<u64, SocketAddr>,
        request_timeout: Duration,
    ) -> Result<Self, ControllerError> {
        if cluster_id == [0; 16]
            || controller_id == 0
            || data_nodes.is_empty()
            || request_timeout.is_zero()
        {
            return Err(ControllerError::Data(
                "invalid remote Data configuration".into(),
            ));
        }
        Ok(Self {
            cluster_id,
            data_nodes,
            request_timeout,
        })
    }

    async fn shard_client(
        &self,
        node_id: u64,
    ) -> Result<ShardServiceClient<tonic::transport::Channel>, ControllerError> {
        let endpoint = self.endpoint(node_id)?;
        ShardServiceClient::connect(format!("http://{endpoint}"))
            .await
            .map_err(|error| ControllerError::Data(error.to_string()))
    }

    async fn admin_client(
        &self,
        node_id: u64,
    ) -> Result<NodeAdminServiceClient<tonic::transport::Channel>, ControllerError> {
        let endpoint = self.endpoint(node_id)?;
        NodeAdminServiceClient::connect(format!("http://{endpoint}"))
            .await
            .map_err(|error| ControllerError::Data(error.to_string()))
    }

    fn endpoint(&self, node_id: u64) -> Result<SocketAddr, ControllerError> {
        self.data_nodes
            .get(&node_id)
            .copied()
            .ok_or_else(|| ControllerError::Data(format!("Data node {node_id} is not configured")))
    }

    fn common(&self, request_id: u128) -> Result<RequestContext, ControllerError> {
        Ok(RequestContext {
            protocol_version: CLUSTER_PROTOCOL_VERSION,
            cluster_id: self.cluster_id.to_vec(),
            request_id: request_id.to_be_bytes().to_vec(),
            deadline_unix_ms: now_ms()?
                .checked_add(self.request_timeout.as_millis() as u64)
                .ok_or_else(|| ControllerError::Data("request deadline overflow".into()))?,
        })
    }

    fn shard_context(
        &self,
        migration: &MigrationRecord,
        request_id: u128,
    ) -> Result<ShardContext, ControllerError> {
        Ok(ShardContext {
            request: Some(self.common(request_id)?),
            graph_id: migration.graph_id(),
            shard_id: migration.shard_id(),
            placement_epoch: migration.source_epoch(),
        })
    }

    async fn export_stream(
        &self,
        migration: &MigrationRecord,
        request_id: u128,
    ) -> Result<tonic::Streaming<cluster_protocol::proto::SnapshotChunk>, ControllerError> {
        let mut errors = Vec::new();
        for node in migration.source_voters() {
            match self.shard_client(*node).await {
                Ok(mut client) => {
                    let request = ExportSnapshotRequest {
                        context: Some(self.shard_context(migration, request_id)?),
                        migration_id: migration.migration_id().to_be_bytes().to_vec(),
                    };
                    match client.export_snapshot(Request::new(request)).await {
                        Ok(response) => return Ok(response.into_inner()),
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(ControllerError::Data(format!(
            "source Shard leader unavailable: {}",
            errors.join("; ")
        )))
    }

    async fn status_on(
        &self,
        node: u64,
        migration: &MigrationRecord,
        step: u8,
    ) -> Result<cluster_protocol::proto::ReplicaStatusResponse, ControllerError> {
        let request_id = operation_id(migration, step, node);
        let mut client = self.shard_client(node).await?;
        client
            .replica_status(Request::new(ReplicaStatusRequest {
                context: Some(self.shard_context(migration, request_id)?),
            }))
            .await
            .map(|response| response.into_inner())
            .map_err(|error| ControllerError::Data(error.to_string()))
    }

    async fn change_membership_on_leader(
        &self,
        migration: &MigrationRecord,
        step: u8,
        old_voters: Vec<u64>,
        new_voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<u64, ControllerError> {
        let request_id = operation_id(migration, step, 0);
        let mut candidates = old_voters.clone();
        candidates.extend(new_voters.iter().copied());
        candidates.sort_unstable();
        candidates.dedup();
        let mut errors = Vec::new();
        for node in candidates {
            match self.admin_client(node).await {
                Ok(mut client) => {
                    let request = ChangeMembershipRequest {
                        context: Some(self.shard_context(migration, request_id)?),
                        operation_id: request_id.to_be_bytes().to_vec(),
                        old_voters: old_voters.clone(),
                        new_voters: new_voters.clone(),
                        learners: learners.clone(),
                    };
                    match client.change_membership(Request::new(request)).await {
                        Ok(response) => return Ok(response.into_inner().applied_index),
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(ControllerError::Data(format!(
            "membership leader unavailable: {}",
            errors.join("; ")
        )))
    }

    async fn source_leader_status(
        &self,
        migration: &MigrationRecord,
        step: u8,
    ) -> Result<cluster_protocol::proto::ReplicaStatusResponse, ControllerError> {
        let mut errors = Vec::new();
        for node in migration.source_voters() {
            match self.status_on(*node, migration, step).await {
                Ok(status) if status.role == ReplicaRole::Leader as i32 => return Ok(status),
                Ok(_) => errors.push(format!("Data node {node} is not leader")),
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(ControllerError::Data(format!(
            "source leader status unavailable: {}",
            errors.join("; ")
        )))
    }

    async fn delete_nodes(
        &self,
        migration: &MigrationRecord,
        nodes: Vec<u64>,
        step: u8,
    ) -> Result<(), ControllerError> {
        for node in nodes {
            let request_id = operation_id(migration, step, node);
            let mut client = self.admin_client(node).await?;
            client
                .delete_replica(Request::new(DeleteReplicaRequest {
                    context: Some(self.shard_context(migration, request_id)?),
                    operation_id: request_id.to_be_bytes().to_vec(),
                    minimum_safe_index: migration.catchup_index(),
                }))
                .await
                .map_err(|error| ControllerError::Data(error.to_string()))?;
        }
        Ok(())
    }
}

impl DataPlaneApi for RemoteDataPlane {
    async fn ensure_target_learners(
        &self,
        migration: &MigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<(), ControllerError> {
        if graph.backend().provider() != "rocksdb" {
            return Err(ControllerError::Data(format!(
                "Data-node migration runtime does not support provider {}",
                graph.backend().provider()
            )));
        }
        for node in added_nodes(migration) {
            let request_id = operation_id(migration, 10, node);
            let mut client = self.admin_client(node).await?;
            let directory = format!(
                "migration-{:032x}-graph-{}-shard-{}",
                migration.migration_id(),
                migration.graph_id(),
                migration.shard_id()
            );
            client
                .ensure_replica(Request::new(EnsureReplicaRequest {
                    context: Some(self.shard_context(migration, request_id)?),
                    operation_id: request_id.to_be_bytes().to_vec(),
                    local_node_id: node,
                    initial_role: ReplicaRole::Learner.into(),
                    schema_version: graph.schema_version(),
                    backend_generation: graph.backend().generation(),
                    backend_profile: encode_rocks_profile(migration.source_voters(), &directory)?,
                }))
                .await
                .map_err(|error| ControllerError::Data(error.to_string()))?;
        }
        Ok(())
    }

    async fn copy_snapshot(
        &self,
        migration: &MigrationRecord,
    ) -> Result<SnapshotFence, ControllerError> {
        let targets = added_nodes(migration);
        let mut common_fence = None;
        if targets.is_empty() {
            let mut stream = self
                .export_stream(migration, operation_id(migration, 20, 0))
                .await?;
            let mut digest = None;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| ControllerError::Data(error.to_string()))?;
                if chunk.terminal {
                    digest = Some(copy_digest(&chunk.manifest_digest)?);
                }
            }
            let leader = self.source_leader_status(migration, 21).await?;
            return Ok(SnapshotFence {
                index: leader.applied_index,
                checksum: digest.ok_or_else(|| {
                    ControllerError::Data("snapshot stream lacked terminal digest".into())
                })?,
            });
        }
        for target in targets {
            let request_id = operation_id(migration, 20, target);
            let mut source = self.export_stream(migration, request_id).await?;
            let mut destination = self.shard_client(target).await?;
            let (sender, receiver) = mpsc::channel(4);
            let relay = tokio::spawn(async move {
                while let Some(chunk) = source.next().await {
                    let chunk = chunk.map_err(|error| ControllerError::Data(error.to_string()))?;
                    if sender.send(chunk).await.is_err() {
                        return Err(ControllerError::Data(
                            "snapshot destination closed stream".into(),
                        ));
                    }
                }
                Ok::<(), ControllerError>(())
            });
            let installed = destination
                .install_snapshot(Request::new(ReceiverStream::new(receiver)))
                .await
                .map_err(|error| ControllerError::Data(error.to_string()))?
                .into_inner();
            relay
                .await
                .map_err(|error| ControllerError::Data(error.to_string()))??;
            let fence = SnapshotFence {
                index: installed.installed_index,
                checksum: copy_digest(&installed.content_digest)?,
            };
            if common_fence.is_some_and(|expected| expected != fence) {
                return Err(ControllerError::Data(
                    "target replicas installed different snapshot fences".into(),
                ));
            }
            common_fence = Some(fence);
        }
        common_fence.ok_or_else(|| ControllerError::Data("no snapshot target".into()))
    }

    async fn catch_up(&self, migration: &MigrationRecord) -> Result<u64, ControllerError> {
        let targets = added_nodes(migration);
        if targets.is_empty() {
            return Ok(migration.snapshot_index().unwrap_or(0));
        }
        self.change_membership_on_leader(
            migration,
            30,
            migration.source_voters().to_vec(),
            migration.source_voters().to_vec(),
            targets.clone(),
        )
        .await?;
        let source = self.source_leader_status(migration, 31).await?;
        let mut minimum = u64::MAX;
        for target in targets {
            let status = self.status_on(target, migration, 32).await?;
            if !status.ready {
                return Ok(0);
            }
            minimum = minimum.min(status.applied_index);
        }
        if minimum < source.commit_index {
            Ok(0)
        } else {
            Ok(minimum)
        }
    }

    async fn commit_membership(&self, migration: &MigrationRecord) -> Result<u64, ControllerError> {
        self.change_membership_on_leader(
            migration,
            40,
            migration.source_voters().to_vec(),
            migration.target_voters().to_vec(),
            Vec::new(),
        )
        .await?;
        let request_id = operation_id(migration, 41, 0);
        let command = CommandEnvelopeV1::new(
            migration.shard_id(),
            migration.source_epoch(),
            request_id,
            CommandBodyV1::ActivatePlacementEpoch(migration.target_epoch()),
        )
        .encode()
        .map_err(|error| ControllerError::Data(error.to_string()))?;
        let mut errors = Vec::new();
        // The removed source can remain leader briefly after the final membership entry.
        // Fence through the union so the actual leader proposes under the already-committed
        // target voter set; cleanup will then stop the removed leader.
        let mut candidates = migration.source_voters().to_vec();
        candidates.extend_from_slice(migration.target_voters());
        candidates.sort_unstable();
        candidates.dedup();
        for node in candidates {
            match self.shard_client(node).await {
                Ok(mut client) => {
                    let request = ExecuteRequest {
                        context: Some(self.shard_context(migration, request_id)?),
                        command: command.clone(),
                    };
                    match client.execute(Request::new(request)).await {
                        Ok(response) => return Ok(response.into_inner().raft_index),
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(ControllerError::Data(format!(
            "placement epoch fence did not commit: {}",
            errors.join("; ")
        )))
    }

    async fn activate_target_replicas(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        for node in migration.target_voters() {
            let request_id = operation_id(migration, 42, *node);
            let mut client = self.admin_client(*node).await?;
            let response = client
                .activate_replica(Request::new(ActivateReplicaRequest {
                    context: Some(self.shard_context(migration, request_id)?),
                    operation_id: request_id.to_be_bytes().to_vec(),
                    target_placement_epoch: migration.target_epoch(),
                    voters: migration.target_voters().to_vec(),
                }))
                .await
                .map_err(|error| ControllerError::Data(error.to_string()))?
                .into_inner();
            let status = response
                .status
                .ok_or_else(|| ControllerError::Data("activation omitted Replica status".into()))?;
            if !status.ready || status.node_id != *node {
                return Err(ControllerError::Data(
                    "target Replica did not satisfy activation readiness".into(),
                ));
            }
        }
        Ok(())
    }

    async fn cleanup_safe(&self, _migration: &MigrationRecord) -> Result<bool, ControllerError> {
        Ok(true)
    }

    async fn delete_source_replicas(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        self.delete_nodes(migration, removed_nodes(migration), 50)
            .await
    }

    async fn delete_target_learners(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError> {
        self.delete_nodes(migration, added_nodes(migration), 51)
            .await
    }
}

fn now_ms() -> Result<u64, ControllerError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ControllerError::Catalog("system clock is before Unix epoch".into()))?
            .as_millis(),
    )
    .map_err(|_| ControllerError::Catalog("system clock overflow".into()))
}

fn added_nodes(migration: &MigrationRecord) -> Vec<u64> {
    migration
        .target_voters()
        .iter()
        .copied()
        .filter(|node| migration.source_voters().binary_search(node).is_err())
        .collect()
}

fn removed_nodes(migration: &MigrationRecord) -> Vec<u64> {
    migration
        .source_voters()
        .iter()
        .copied()
        .filter(|node| migration.target_voters().binary_search(node).is_err())
        .collect()
}

fn operation_id(migration: &MigrationRecord, step: u8, node: u64) -> u128 {
    let mut input = Vec::with_capacity(41);
    input.extend_from_slice(&migration.migration_id().to_be_bytes());
    input.extend_from_slice(&migration.state_revision().to_be_bytes());
    input.push(step);
    input.extend_from_slice(&node.to_be_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    let value = u128::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}

fn copy_digest(bytes: &[u8]) -> Result<[u8; 32], ControllerError> {
    bytes
        .try_into()
        .map_err(|_| ControllerError::Data("snapshot digest must contain 32 bytes".into()))
}

fn encode_rocks_profile(voters: &[u64], directory: &str) -> Result<Vec<u8>, ControllerError> {
    if voters.is_empty()
        || voters.len() > 64
        || voters[0] == 0
        || voters.windows(2).any(|pair| pair[0] >= pair[1])
        || directory.is_empty()
        || directory.len() > 255
    {
        return Err(ControllerError::Data(
            "invalid RocksDB Replica profile".into(),
        ));
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(b"DTRF");
    encoded.extend_from_slice(&1_u16.to_be_bytes());
    encoded.extend_from_slice(&(voters.len() as u16).to_be_bytes());
    encoded.extend_from_slice(&(directory.len() as u16).to_be_bytes());
    for voter in voters {
        encoded.extend_from_slice(&voter.to_be_bytes());
    }
    encoded.extend_from_slice(directory.as_bytes());
    encoded.extend_from_slice(&crc32fast::hash(&encoded).to_be_bytes());
    Ok(encoded)
}
