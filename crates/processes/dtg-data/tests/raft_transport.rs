use std::pin::Pin;
use std::sync::{Arc, Mutex};

use dtg_data::{RaftTransport, TonicRaftTransport};
use dtg_execution::cluster_protocol::proto::data_service_server::{DataService, DataServiceServer};
use dtg_execution::cluster_protocol::proto::{
    ColumnBatch, ExecutionFragment, LogicalReplicaSnapshot, RaftEnvelope, RetryDisposition,
    SnapshotIngestBatch, SnapshotIngestReceipt, SnapshotIngestReceiptRequest,
    SnapshotIngestReceiptResponse, StatusCode, TransactionRequest, TypedStatus,
};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, ReplicaId, Version,
};
use prost_011::Message as _;
use raft::eraftpb::{Message, MessageType};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
struct RecordingDataService {
    envelopes: Arc<Mutex<Vec<RaftEnvelope>>>,
}

#[tonic::async_trait]
impl DataService for RecordingDataService {
    type ExecuteFragmentStream =
        Pin<Box<dyn Stream<Item = Result<ColumnBatch, Status>> + Send + 'static>>;

    async fn execute_fragment(
        &self,
        _request: Request<ExecutionFragment>,
    ) -> Result<Response<Self::ExecuteFragmentStream>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn apply_transaction(
        &self,
        _request: Request<TransactionRequest>,
    ) -> Result<Response<TypedStatus>, Status> {
        Err(Status::unimplemented("not used"))
    }

    type AcceptSnapshotIngestStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotIngestReceipt, Status>> + Send + 'static>>;

    async fn accept_snapshot_ingest(
        &self,
        _request: Request<SnapshotIngestBatch>,
    ) -> Result<Response<Self::AcceptSnapshotIngestStream>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_snapshot_ingest_receipt(
        &self,
        _request: Request<SnapshotIngestReceiptRequest>,
    ) -> Result<Response<SnapshotIngestReceiptResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn send_raft(
        &self,
        request: Request<RaftEnvelope>,
    ) -> Result<Response<TypedStatus>, Status> {
        let envelope = request.into_inner();
        let context = envelope
            .context
            .as_ref()
            .and_then(|context| context.request.clone());
        self.envelopes.lock().unwrap().push(envelope);
        Ok(Response::new(TypedStatus {
            request: context,
            code: StatusCode::Ok.into(),
            retry: RetryDisposition::Never.into(),
            message: "ok".into(),
            idempotency_key: Vec::new(),
            details: None,
        }))
    }

    async fn install_replica_snapshot(
        &self,
        _request: Request<LogicalReplicaSnapshot>,
    ) -> Result<Response<TypedStatus>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

fn binding(replica_id: u64) -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(7)
        .graph_id(11)
        .shard_id(13)
        .placement_epoch(17)
        .replica_id(replica_id)
        .backend_generation(19)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id(format!("raft-transport-{replica_id}"))
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

#[tokio::test]
async fn tonic_raft_transport_sends_a_fenced_protocol_v2_envelope() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let service = RecordingDataService::default();
    let recorded = service.envelopes.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(DataServiceServer::new(service))
            .serve_with_shutdown(address, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let source = binding(23);
    let transport = TonicRaftTransport::new();
    transport
        .install_route(
            &source,
            ReplicaId::new(29).unwrap(),
            Version::new(31),
            format!("http://{address}"),
        )
        .unwrap();
    let mut message = Message::default();
    message.set_from(23);
    message.set_to(29);
    message.set_term(37);
    message.set_commit(41);
    message.set_msg_type(MessageType::MsgReadIndex);

    transport.send(&source, message.clone()).await.unwrap();

    {
        let envelopes = recorded.lock().unwrap();
        assert_eq!(envelopes.len(), 1);
        let envelope = &envelopes[0];
        let context = envelope.context.as_ref().unwrap();
        assert_eq!(context.catalog_version, 31);
        assert_eq!(context.placement_epoch, 17);
        assert_eq!(context.backend_generation, 19);
        assert_eq!(envelope.from_replica_id, 23);
        assert_eq!(envelope.to_replica_id, 29);
        assert_eq!(envelope.term, 37);
        assert_eq!(envelope.committed_index, 41);
        assert_eq!(
            envelope.kind,
            dtg_execution::cluster_protocol::proto::RaftMessageKind::Control as i32
        );
        assert_eq!(
            Message::decode(envelope.payload.as_ref().unwrap().body.as_slice()).unwrap(),
            message
        );
    }

    shutdown_tx.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn tonic_raft_transport_fails_closed_without_an_exact_route() {
    let source = binding(23);
    let transport = TonicRaftTransport::new();
    let mut message = Message::default();
    message.set_from(23);
    message.set_to(29);
    message.set_term(37);

    let error = transport.send(&source, message).await.unwrap_err();

    assert!(error.to_string().contains("exact Raft route"));
}
