use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayClusterRequest, GatewayExecution, GatewayExecutionError, GatewayExecutionTransport,
    GatewayFuture, GatewayOperation, GatewayResponse,
};
use dtg_gateway::{GatewayConfig, GatewayService};
use support::planning_context;

#[derive(Default)]
struct TransactionTransport {
    next_transaction: Mutex<u128>,
    requests: Mutex<Vec<GatewayClusterRequest>>,
}

impl GatewayExecutionTransport for TransactionTransport {
    fn execute(
        &self,
        request: GatewayClusterRequest,
    ) -> GatewayFuture<'_, Result<GatewayResponse, GatewayExecutionError>> {
        let result = match request.operation() {
            GatewayOperation::BeginTransaction => {
                let mut next = self.next_transaction.lock().unwrap();
                *next += 1;
                Ok(GatewayResponse::Transaction {
                    transaction_id: *next,
                })
            }
            GatewayOperation::Write => panic!("Gateway must reject explicit transaction writes"),
            GatewayOperation::CommitTransaction | GatewayOperation::RollbackTransaction => {
                Ok(GatewayResponse::Acknowledged)
            }
            operation => panic!("unexpected transaction operation: {operation:?}"),
        };
        self.requests.lock().unwrap().push(request);
        Box::pin(async move { result })
    }
}

fn gateway(transport: Arc<TransactionTransport>) -> GatewayService {
    let execution = GatewayExecution::for_process(transport, planning_context());
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    GatewayService::new(config, execution)
}

#[tokio::test]
async fn explicit_transactions_route_boundaries_but_reject_process_writes() {
    let transport = Arc::new(TransactionTransport::default());
    let gateway = gateway(transport.clone());

    let single = gateway.bolt().begin().await.unwrap();
    let write = single
        .query("CREATE (n {id: 1}) VALID FROM 40")
        .execute()
        .await
        .unwrap_err();
    assert_eq!(write.code(), "DTG-EXECUTION-WRITE-TRANSACTION");
    single.rollback().await.unwrap();

    let multi = gateway.bolt().begin().await.unwrap();
    multi.commit().await.unwrap();

    let requests = transport.requests.lock().unwrap();
    assert!(matches!(
        requests.as_slice(),
        [begin_one, rollback_one, begin_two, commit_two]
            if begin_one.operation() == &GatewayOperation::BeginTransaction
                && rollback_one.operation() == &GatewayOperation::RollbackTransaction
                && rollback_one.transaction_id() == Some(1)
                && begin_two.operation() == &GatewayOperation::BeginTransaction
                && commit_two.transaction_id() == Some(2)
    ));
}
mod support;
