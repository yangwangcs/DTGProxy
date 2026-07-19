mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use bolt_protocol::ClientMessage;
use bolt_server::{BoltMachine, ConnectionState, ServerMessage};

use support::FakeService;

#[tokio::test]
async fn explicit_transaction_runs_pulls_and_commits() {
    let service = Arc::new(FakeService::default());
    let mut machine = BoltMachine::new(Arc::clone(&service));
    machine.handle(ClientMessage::Hello(BTreeMap::new())).await;

    let begin = machine.handle(ClientMessage::Begin(BTreeMap::new())).await;
    assert!(matches!(begin.as_slice(), [ServerMessage::Success(_)]));
    assert_eq!(machine.state(), ConnectionState::TxReady);

    machine
        .handle(ClientMessage::Run {
            query: "CREATE (n) RETURN n".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert_eq!(machine.state(), ConnectionState::TxStreaming);
    machine
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert_eq!(machine.state(), ConnectionState::TxReady);

    let commit = machine.handle(ClientMessage::Commit).await;
    assert!(
        matches!(commit.as_slice(), [ServerMessage::Success(metadata)] if metadata.contains_key("bookmark"))
    );
    assert_eq!(machine.state(), ConnectionState::Ready);
    assert_eq!(service.committed_transactions(), 1);
}

#[tokio::test]
async fn reset_rolls_back_an_open_transaction() {
    let service = Arc::new(FakeService::default());
    let mut machine = BoltMachine::new(Arc::clone(&service));
    machine.handle(ClientMessage::Hello(BTreeMap::new())).await;
    machine.handle(ClientMessage::Begin(BTreeMap::new())).await;

    machine.handle(ClientMessage::Reset).await;

    assert_eq!(machine.state(), ConnectionState::Ready);
    assert_eq!(service.rolled_back_transactions(), 1);
}
