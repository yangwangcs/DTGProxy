use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bolt_protocol::{ClientMessage, Value};
use bolt_server::{BoltMachine, ServerMessage};
use cypher_engine::{
    BackendFuture, BackendQueryResult, BoltQueryBackend, BoltQueryRequest, CypherBoltService,
};
use query_executor::v2::RuntimeValue;

#[derive(Default)]
struct Backend {
    parameters: Mutex<BTreeMap<String, RuntimeValue>>,
}

impl BoltQueryBackend for Backend {
    fn execute<'a>(&'a self, request: BoltQueryRequest) -> BackendFuture<'a, BackendQueryResult> {
        Box::pin(async move {
            *self.parameters.lock().expect("parameters") = request.parameters().clone();
            Ok(BackendQueryResult::new(
                vec!["seen".into()],
                vec![vec![RuntimeValue::TimestampMicros(1_000_002)]],
                BTreeMap::new(),
            ))
        })
    }
}

#[tokio::test]
async fn bolt_machine_runs_temporal_cypher_with_typed_parameters_and_cursor_pull() {
    let backend = Arc::new(Backend::default());
    let service = Arc::new(CypherBoltService::new(Arc::clone(&backend), 8).expect("service"));
    let mut machine = BoltMachine::new(service);
    machine.handle(ClientMessage::Hello(BTreeMap::new())).await;
    let run = machine
        .handle(ClientMessage::Run {
            query: "AT VALID_TIME AS OF $valid MATCH (n) RETURN n".into(),
            parameters: BTreeMap::from([(
                "valid".into(),
                Value::Structure {
                    signature: 0x49,
                    fields: vec![Value::Integer(1), Value::Integer(2_000), Value::Integer(0)],
                },
            )]),
            extra: BTreeMap::new(),
        })
        .await;
    assert!(matches!(run.as_slice(), [ServerMessage::Success(_)]));
    assert_eq!(
        backend.parameters.lock().expect("parameters").get("valid"),
        Some(&RuntimeValue::TimestampMicros(1_000_002))
    );

    let pulled = machine
        .handle(ClientMessage::Pull {
            n: -1,
            query_id: None,
        })
        .await;
    assert!(matches!(
        pulled.as_slice(),
        [
            ServerMessage::Record(values),
            ServerMessage::Success(summary)
        ] if values == &vec![Value::Structure {
            signature: 0x49,
            fields: vec![
                Value::Integer(1),
                Value::Integer(2_000),
                Value::Integer(0),
            ],
        }] && !summary.contains_key("has_more")
    ));
}
