use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dtg_execution::{
    GatewayClusterRequest, GatewayExecution, GatewayExecutionError, GatewayExecutionTransport,
    GatewayFuture, GatewayOperation, GatewayResponse, GatewayRetry,
};
use dtg_gateway::{GatewayConfig, GatewayService};

#[derive(Default)]
struct TransactionTransport {
    next_transaction: Mutex<u128>,
    writes: Mutex<BTreeMap<u128, usize>>,
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
            GatewayOperation::Write => match request.transaction_id() {
                Some(transaction_id) => {
                    *self
                        .writes
                        .lock()
                        .unwrap()
                        .entry(transaction_id)
                        .or_default() += 1;
                    Ok(GatewayResponse::Acknowledged)
                }
                None => Err(GatewayExecutionError::new(
                    "DTG-TXN-MISSING-CONTEXT",
                    "write requires an explicit transaction",
                    GatewayRetry::Never,
                )),
            },
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
    let execution = GatewayExecution::for_process(transport);
    let config = GatewayConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7687),
        7,
        Duration::from_secs(5),
    )
    .unwrap();
    GatewayService::new(config, execution)
}

#[tokio::test]
async fn single_and_multi_shard_transactions_share_execution_coordinator_boundaries() {
    let transport = Arc::new(TransactionTransport::default());
    let gateway = gateway(transport.clone());

    let single = gateway.bolt().begin().await.unwrap();
    single
        .query("CREATE (n {id: 1}) VALID FROM 40")
        .execute()
        .await
        .unwrap();
    single.commit().await.unwrap();

    let multi = gateway.bolt().begin().await.unwrap();
    multi
        .query("CREATE (n {id: 2}) VALID FROM 40")
        .execute()
        .await
        .unwrap();
    multi
        .query("CREATE (n {id: 3}) VALID FROM 40")
        .execute()
        .await
        .unwrap();
    multi.commit().await.unwrap();

    let writes = transport.writes.lock().unwrap();
    assert_eq!(writes.values().copied().collect::<Vec<_>>(), vec![1, 2]);
    let requests = transport.requests.lock().unwrap();
    assert!(matches!(
        requests.as_slice(),
        [begin_one, write_one, commit_one, begin_two, write_two_a, write_two_b, commit_two]
            if begin_one.operation() == &GatewayOperation::BeginTransaction
                && write_one.transaction_id() == Some(1)
                && commit_one.transaction_id() == Some(1)
                && begin_two.operation() == &GatewayOperation::BeginTransaction
                && write_two_a.transaction_id() == Some(2)
                && write_two_b.transaction_id() == Some(2)
                && commit_two.transaction_id() == Some(2)
    ));
}
