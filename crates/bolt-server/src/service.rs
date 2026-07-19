use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use bolt_protocol::Value;

pub type ServiceFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ServiceError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CursorId(u64);

impl CursorId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TransactionId(u64);

impl TransactionId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunRequest {
    query: String,
    parameters: BTreeMap<String, Value>,
    extra: BTreeMap<String, Value>,
    transaction: Option<TransactionId>,
}

impl RunRequest {
    #[must_use]
    pub const fn new(
        query: String,
        parameters: BTreeMap<String, Value>,
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
    pub const fn parameters(&self) -> &BTreeMap<String, Value> {
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
pub struct RunOutcome {
    cursor: CursorId,
    fields: Vec<String>,
}

impl RunOutcome {
    #[must_use]
    pub const fn new(cursor: CursorId, fields: Vec<String>) -> Self {
        Self { cursor, fields }
    }

    #[must_use]
    pub const fn cursor(&self) -> CursorId {
        self.cursor
    }

    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PullOutcome {
    records: Vec<Vec<Value>>,
    has_more: bool,
    summary: BTreeMap<String, Value>,
}

impl PullOutcome {
    #[must_use]
    pub const fn new(
        records: Vec<Vec<Value>>,
        has_more: bool,
        summary: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            records,
            has_more,
            summary,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<Vec<Value>>, bool, BTreeMap<String, Value>) {
        (self.records, self.has_more, self.summary)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceError {
    code: String,
    message: String,
}

impl ServiceError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for ServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for ServiceError {}

pub trait BoltService: Send + Sync {
    fn hello<'a>(
        &'a self,
        _metadata: BTreeMap<String, Value>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move { Ok(BTreeMap::new()) })
    }

    fn logon<'a>(
        &'a self,
        _auth: BTreeMap<String, Value>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move { Ok(BTreeMap::new()) })
    }

    fn logoff<'a>(&'a self) -> ServiceFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn run<'a>(&'a self, request: RunRequest) -> ServiceFuture<'a, RunOutcome>;

    fn pull<'a>(&'a self, cursor: CursorId, n: i64) -> ServiceFuture<'a, PullOutcome>;

    fn discard<'a>(&'a self, cursor: CursorId, n: i64) -> ServiceFuture<'a, bool>;

    fn begin<'a>(&'a self, extra: BTreeMap<String, Value>) -> ServiceFuture<'a, TransactionId>;

    fn commit<'a>(&'a self, transaction: TransactionId) -> ServiceFuture<'a, String>;

    fn rollback<'a>(&'a self, transaction: TransactionId) -> ServiceFuture<'a, ()>;

    fn route<'a>(
        &'a self,
        routing: BTreeMap<String, Value>,
        bookmarks: Vec<Value>,
        database: Option<String>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>>;
}
