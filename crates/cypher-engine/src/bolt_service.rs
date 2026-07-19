use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bolt_protocol::Value;
use bolt_server::{
    BoltService, CursorId, PullOutcome, RunOutcome, RunRequest, ServiceError, ServiceFuture,
    TransactionId,
};
use query_executor::v2::RuntimeValue;

use crate::{bolt_parameter_to_runtime, runtime_value_to_bolt};

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoltQueryRequest {
    query: String,
    parameters: BTreeMap<String, RuntimeValue>,
    extra: BTreeMap<String, Value>,
    transaction: Option<TransactionId>,
}

impl BoltQueryRequest {
    #[must_use]
    pub const fn new(
        query: String,
        parameters: BTreeMap<String, RuntimeValue>,
        extra: BTreeMap<String, Value>,
        transaction: Option<TransactionId>,
    ) -> Self {
        Self {
            query,
            parameters,
            extra,
            transaction,
        }
    }

    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<String, RuntimeValue> {
        &self.parameters
    }

    #[must_use]
    pub const fn extra(&self) -> &BTreeMap<String, Value> {
        &self.extra
    }

    #[must_use]
    pub const fn transaction(&self) -> Option<TransactionId> {
        self.transaction
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendQueryResult {
    fields: Vec<String>,
    records: Vec<Vec<RuntimeValue>>,
    summary: BTreeMap<String, Value>,
}

impl BackendQueryResult {
    #[must_use]
    pub const fn new(
        fields: Vec<String>,
        records: Vec<Vec<RuntimeValue>>,
        summary: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            fields,
            records,
            summary,
        }
    }
}

pub trait BoltQueryBackend: Send + Sync {
    fn execute<'a>(&'a self, request: BoltQueryRequest) -> BackendFuture<'a, BackendQueryResult>;

    fn begin<'a>(&'a self, _extra: BTreeMap<String, Value>) -> BackendFuture<'a, TransactionId> {
        Box::pin(async {
            Err(ServiceError::new(
                "Neo.ClientError.Transaction.TransactionStartFailed",
                "explicit transactions are not supported by this backend",
            ))
        })
    }

    fn commit<'a>(&'a self, _transaction: TransactionId) -> BackendFuture<'a, String> {
        Box::pin(async {
            Err(ServiceError::new(
                "Neo.ClientError.Transaction.InvalidBookmark",
                "no explicit transaction is active",
            ))
        })
    }

    fn rollback<'a>(&'a self, _transaction: TransactionId) -> BackendFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct Cursor {
    records: Vec<Vec<Value>>,
    offset: usize,
    summary: BTreeMap<String, Value>,
}

pub struct CypherBoltService<B> {
    backend: Arc<B>,
    max_cursors: usize,
    next_cursor: AtomicU64,
    cursors: Mutex<HashMap<CursorId, Cursor>>,
}

impl<B> CypherBoltService<B>
where
    B: BoltQueryBackend,
{
    pub fn new(backend: Arc<B>, max_cursors: usize) -> Result<Self, ServiceError> {
        if max_cursors == 0 {
            return Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "maximum Bolt cursor count must be non-zero",
            ));
        }
        Ok(Self {
            backend,
            max_cursors,
            next_cursor: AtomicU64::new(1),
            cursors: Mutex::new(HashMap::new()),
        })
    }

    fn cursor_error(message: impl Into<String>) -> ServiceError {
        ServiceError::new("Neo.ClientError.Request.Invalid", message)
    }

    fn lock_cursors(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<CursorId, Cursor>>, ServiceError> {
        self.cursors
            .lock()
            .map_err(|_| Self::cursor_error("Bolt cursor state lock is poisoned"))
    }
}

impl<B> BoltService for CypherBoltService<B>
where
    B: BoltQueryBackend + 'static,
{
    fn run<'a>(&'a self, request: RunRequest) -> ServiceFuture<'a, RunOutcome> {
        Box::pin(async move {
            let parameters = request
                .parameters()
                .iter()
                .map(|(name, value)| {
                    bolt_parameter_to_runtime(value)
                        .map(|value| (name.clone(), value))
                        .map_err(|error| {
                            ServiceError::new(
                                "Neo.ClientError.Statement.TypeError",
                                error.to_string(),
                            )
                        })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let backend_request = BoltQueryRequest::new(
                request.query().to_owned(),
                parameters,
                request.extra().clone(),
                request.transaction(),
            );
            let result = self.backend.execute(backend_request).await?;
            if result
                .records
                .iter()
                .any(|record| record.len() != result.fields.len())
            {
                return Err(ServiceError::new(
                    "Neo.DatabaseError.General.UnknownError",
                    "query backend returned a record with the wrong field count",
                ));
            }
            let records = result
                .records
                .iter()
                .map(|record| {
                    record
                        .iter()
                        .map(runtime_value_to_bolt)
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    ServiceError::new("Neo.DatabaseError.General.UnknownError", error.to_string())
                })?;
            let cursor_id = self.next_cursor.fetch_add(1, Ordering::Relaxed);
            if cursor_id == u64::MAX {
                return Err(Self::cursor_error("Bolt cursor identity space exhausted"));
            }
            let cursor = CursorId::new(cursor_id);
            let mut cursors = self.lock_cursors()?;
            if cursors.len() >= self.max_cursors {
                return Err(ServiceError::new(
                    "Neo.TransientError.General.MemoryPoolOutOfMemoryError",
                    "maximum concurrent Bolt cursor count reached",
                ));
            }
            cursors.insert(
                cursor,
                Cursor {
                    records,
                    offset: 0,
                    summary: result.summary,
                },
            );
            Ok(RunOutcome::new(cursor, result.fields))
        })
    }

    fn pull<'a>(&'a self, cursor: CursorId, n: i64) -> ServiceFuture<'a, PullOutcome> {
        Box::pin(async move {
            if n == 0 || n < -1 {
                return Err(Self::cursor_error("PULL n must be -1 or positive"));
            }
            let mut cursors = self.lock_cursors()?;
            let state = cursors
                .get_mut(&cursor)
                .ok_or_else(|| Self::cursor_error("unknown Bolt cursor"))?;
            let remaining = state.records.len().saturating_sub(state.offset);
            let take = if n == -1 {
                remaining
            } else {
                remaining.min(usize::try_from(n).map_err(|_| {
                    Self::cursor_error("PULL record count exceeds platform capacity")
                })?)
            };
            let end = state.offset.saturating_add(take);
            let records = state.records[state.offset..end].to_vec();
            state.offset = end;
            let has_more = end < state.records.len();
            let summary = if has_more {
                BTreeMap::new()
            } else {
                state.summary.clone()
            };
            if !has_more {
                cursors.remove(&cursor);
            }
            Ok(PullOutcome::new(records, has_more, summary))
        })
    }

    fn discard<'a>(&'a self, cursor: CursorId, n: i64) -> ServiceFuture<'a, bool> {
        Box::pin(async move {
            if n == 0 || n < -1 {
                return Err(Self::cursor_error("DISCARD n must be -1 or positive"));
            }
            let mut cursors = self.lock_cursors()?;
            let state = cursors
                .get_mut(&cursor)
                .ok_or_else(|| Self::cursor_error("unknown Bolt cursor"))?;
            let remaining = state.records.len().saturating_sub(state.offset);
            let discard = if n == -1 {
                remaining
            } else {
                remaining.min(usize::try_from(n).map_err(|_| {
                    Self::cursor_error("DISCARD record count exceeds platform capacity")
                })?)
            };
            state.offset = state.offset.saturating_add(discard);
            let has_more = state.offset < state.records.len();
            if !has_more {
                cursors.remove(&cursor);
            }
            Ok(has_more)
        })
    }

    fn begin<'a>(&'a self, extra: BTreeMap<String, Value>) -> ServiceFuture<'a, TransactionId> {
        Box::pin(async move { self.backend.begin(extra).await })
    }

    fn commit<'a>(&'a self, transaction: TransactionId) -> ServiceFuture<'a, String> {
        Box::pin(async move { self.backend.commit(transaction).await })
    }

    fn rollback<'a>(&'a self, transaction: TransactionId) -> ServiceFuture<'a, ()> {
        Box::pin(async move { self.backend.rollback(transaction).await })
    }

    fn route<'a>(
        &'a self,
        _routing: BTreeMap<String, Value>,
        _bookmarks: Vec<Value>,
        _database: Option<String>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move {
            Ok(BTreeMap::from([
                ("ttl".into(), Value::Integer(30)),
                ("servers".into(), Value::List(Vec::new())),
            ]))
        })
    }
}
