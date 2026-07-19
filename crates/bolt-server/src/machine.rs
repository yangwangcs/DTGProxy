use std::collections::BTreeMap;
use std::sync::Arc;

use bolt_protocol::{ClientMessage, Value};

use crate::{BoltService, CursorId, RunRequest, ServiceError, TransactionId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Connected,
    Ready,
    Streaming,
    TxReady,
    TxStreaming,
    Failed,
    Interrupted,
    Defunct,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerMessage {
    Success(BTreeMap<String, Value>),
    Record(Vec<Value>),
    Ignored,
    Failure { code: String, message: String },
}

pub struct BoltMachine<S> {
    service: Arc<S>,
    state: ConnectionState,
    cursor: Option<CursorId>,
    transaction: Option<TransactionId>,
}

impl<S> BoltMachine<S>
where
    S: BoltService,
{
    #[must_use]
    pub const fn new(service: Arc<S>) -> Self {
        Self {
            service,
            state: ConnectionState::Connected,
            cursor: None,
            transaction: None,
        }
    }

    #[must_use]
    pub const fn state(&self) -> ConnectionState {
        self.state
    }

    pub async fn handle(&mut self, message: ClientMessage) -> Vec<ServerMessage> {
        if self.state == ConnectionState::Defunct {
            return Vec::new();
        }
        if matches!(&message, ClientMessage::Goodbye) {
            let _ = self.rollback_open_transaction().await;
            self.cursor = None;
            self.state = ConnectionState::Defunct;
            return Vec::new();
        }
        if matches!(&message, ClientMessage::Reset) {
            return self.reset().await;
        }
        if matches!(
            self.state,
            ConnectionState::Failed | ConnectionState::Interrupted
        ) {
            return vec![ServerMessage::Ignored];
        }
        match message {
            ClientMessage::Hello(metadata) if self.state == ConnectionState::Connected => {
                match self.service.hello(metadata).await {
                    Ok(mut metadata) => {
                        metadata
                            .entry("server".into())
                            .or_insert_with(|| Value::String("DTGProxy/1.0".into()));
                        self.state = ConnectionState::Ready;
                        success(metadata)
                    }
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Logon(auth) if self.state == ConnectionState::Ready => {
                match self.service.logon(auth).await {
                    Ok(metadata) => success(metadata),
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Logoff if self.state == ConnectionState::Ready => {
                match self.service.logoff().await {
                    Ok(()) => success(BTreeMap::new()),
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Run {
                query,
                parameters,
                extra,
            } if matches!(
                self.state,
                ConnectionState::Ready | ConnectionState::TxReady
            ) =>
            {
                let tx = self.transaction;
                match self
                    .service
                    .run(RunRequest::new(query, parameters, extra, tx))
                    .await
                {
                    Ok(outcome) => {
                        self.cursor = Some(outcome.cursor());
                        self.state = if tx.is_some() {
                            ConnectionState::TxStreaming
                        } else {
                            ConnectionState::Streaming
                        };
                        success(BTreeMap::from([
                            (
                                "fields".into(),
                                Value::List(
                                    outcome
                                        .fields()
                                        .iter()
                                        .cloned()
                                        .map(Value::String)
                                        .collect(),
                                ),
                            ),
                            (
                                "qid".into(),
                                Value::Integer(
                                    i64::try_from(outcome.cursor().value()).unwrap_or(i64::MAX),
                                ),
                            ),
                        ]))
                    }
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Pull { n, query_id }
                if matches!(
                    self.state,
                    ConnectionState::Streaming | ConnectionState::TxStreaming
                ) =>
            {
                self.pull(n, query_id).await
            }
            ClientMessage::Discard { n, query_id }
                if matches!(
                    self.state,
                    ConnectionState::Streaming | ConnectionState::TxStreaming
                ) =>
            {
                self.discard(n, query_id).await
            }
            ClientMessage::Begin(extra) if self.state == ConnectionState::Ready => {
                match self.service.begin(extra).await {
                    Ok(transaction) => {
                        self.transaction = Some(transaction);
                        self.state = ConnectionState::TxReady;
                        success(BTreeMap::new())
                    }
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Commit if self.state == ConnectionState::TxReady => {
                let transaction = self.transaction.expect("TxReady has a transaction");
                match self.service.commit(transaction).await {
                    Ok(bookmark) => {
                        self.transaction = None;
                        self.state = ConnectionState::Ready;
                        success(BTreeMap::from([(
                            "bookmark".into(),
                            Value::String(bookmark),
                        )]))
                    }
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Rollback if self.state == ConnectionState::TxReady => {
                let transaction = self.transaction.expect("TxReady has a transaction");
                match self.service.rollback(transaction).await {
                    Ok(()) => {
                        self.transaction = None;
                        self.state = ConnectionState::Ready;
                        success(BTreeMap::new())
                    }
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Route {
                routing,
                bookmarks,
                database,
            } if self.state == ConnectionState::Ready => {
                match self.service.route(routing, bookmarks, database).await {
                    Ok(metadata) => success(metadata),
                    Err(error) => self.service_failure(error),
                }
            }
            ClientMessage::Interrupt => {
                self.state = ConnectionState::Interrupted;
                success(BTreeMap::new())
            }
            _ => self.protocol_failure("message is not valid in the current Bolt state"),
        }
    }

    async fn pull(&mut self, n: i64, query_id: Option<i64>) -> Vec<ServerMessage> {
        let Some(cursor) = self.valid_cursor(n, query_id) else {
            return self.protocol_failure("PULL has invalid n or qid metadata");
        };
        match self.service.pull(cursor, n).await {
            Ok(outcome) => {
                let (records, has_more, mut summary) = outcome.into_parts();
                let mut messages = records
                    .into_iter()
                    .map(ServerMessage::Record)
                    .collect::<Vec<_>>();
                if has_more {
                    summary.insert("has_more".into(), Value::Boolean(true));
                } else {
                    self.cursor = None;
                    self.state = if self.transaction.is_some() {
                        ConnectionState::TxReady
                    } else {
                        ConnectionState::Ready
                    };
                }
                messages.push(ServerMessage::Success(summary));
                messages
            }
            Err(error) => self.service_failure(error),
        }
    }

    async fn discard(&mut self, n: i64, query_id: Option<i64>) -> Vec<ServerMessage> {
        let Some(cursor) = self.valid_cursor(n, query_id) else {
            return self.protocol_failure("DISCARD has invalid n or qid metadata");
        };
        match self.service.discard(cursor, n).await {
            Ok(has_more) => {
                if has_more {
                    success(BTreeMap::from([("has_more".into(), Value::Boolean(true))]))
                } else {
                    self.cursor = None;
                    self.state = if self.transaction.is_some() {
                        ConnectionState::TxReady
                    } else {
                        ConnectionState::Ready
                    };
                    success(BTreeMap::new())
                }
            }
            Err(error) => self.service_failure(error),
        }
    }

    fn valid_cursor(&self, n: i64, query_id: Option<i64>) -> Option<CursorId> {
        if n == 0 || n < -1 {
            return None;
        }
        let cursor = self.cursor?;
        if query_id.is_some_and(|query_id| {
            u64::try_from(query_id).map_or(true, |query_id| query_id != cursor.value())
        }) {
            return None;
        }
        Some(cursor)
    }

    async fn reset(&mut self) -> Vec<ServerMessage> {
        if self.rollback_open_transaction().await.is_err() {
            return self.protocol_failure("failed to roll back transaction during RESET");
        }
        self.cursor = None;
        self.state = ConnectionState::Ready;
        success(BTreeMap::new())
    }

    async fn rollback_open_transaction(&mut self) -> Result<(), ServiceError> {
        if let Some(transaction) = self.transaction.take() {
            self.service.rollback(transaction).await?;
        }
        Ok(())
    }

    fn protocol_failure(&mut self, message: impl Into<String>) -> Vec<ServerMessage> {
        self.state = ConnectionState::Failed;
        vec![ServerMessage::Failure {
            code: "Neo.ClientError.Request.Invalid".into(),
            message: message.into(),
        }]
    }

    fn service_failure(&mut self, error: ServiceError) -> Vec<ServerMessage> {
        self.state = ConnectionState::Failed;
        vec![ServerMessage::Failure {
            code: error.code().to_owned(),
            message: error.message().to_owned(),
        }]
    }
}

fn success(metadata: BTreeMap<String, Value>) -> Vec<ServerMessage> {
    vec![ServerMessage::Success(metadata)]
}
