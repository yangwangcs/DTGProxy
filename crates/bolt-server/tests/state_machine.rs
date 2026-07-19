mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use bolt_protocol::{ClientMessage, Value};
use bolt_server::{BoltMachine, ConnectionState, ServerMessage};

use support::FakeService;

#[tokio::test]
async fn auto_commit_run_pull_returns_to_ready() {
    let mut machine = BoltMachine::new(Arc::new(FakeService::default()));

    let hello = machine.handle(ClientMessage::Hello(BTreeMap::new())).await;
    assert!(matches!(hello.as_slice(), [ServerMessage::Success(_)]));
    assert_eq!(machine.state(), ConnectionState::Ready);

    let run = machine
        .handle(ClientMessage::Run {
            query: "RETURN 1 AS value".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(run.as_slice(), [ServerMessage::Success(_)]));
    assert_eq!(machine.state(), ConnectionState::Streaming);

    let pull = machine
        .handle(ClientMessage::Pull {
            n: 10,
            query_id: None,
        })
        .await;
    assert!(matches!(
        pull.as_slice(),
        [ServerMessage::Record(values), ServerMessage::Success(_)]
            if values == &[Value::Integer(1)]
    ));
    assert_eq!(machine.state(), ConnectionState::Ready);
}

#[tokio::test]
async fn protocol_error_enters_failed_until_reset() {
    let mut machine = BoltMachine::new(Arc::new(FakeService::default()));
    machine.handle(ClientMessage::Hello(BTreeMap::new())).await;

    let failure = machine.handle(ClientMessage::Commit).await;
    assert!(matches!(
        failure.as_slice(),
        [ServerMessage::Failure { .. }]
    ));
    assert_eq!(machine.state(), ConnectionState::Failed);

    let ignored = machine
        .handle(ClientMessage::Run {
            query: "RETURN 1".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        })
        .await;
    assert_eq!(ignored, vec![ServerMessage::Ignored]);

    let reset = machine.handle(ClientMessage::Reset).await;
    assert!(matches!(reset.as_slice(), [ServerMessage::Success(_)]));
    assert_eq!(machine.state(), ConnectionState::Ready);
}
