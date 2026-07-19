#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use bolt_protocol::Value;
use bolt_server::{
    BoltService, CursorId, PullOutcome, RunOutcome, RunRequest, ServiceError, ServiceFuture,
    TransactionId,
};

#[derive(Default)]
pub struct FakeService {
    committed: AtomicUsize,
    rolled_back: AtomicUsize,
}

impl FakeService {
    pub fn committed_transactions(&self) -> usize {
        self.committed.load(Ordering::SeqCst)
    }

    pub fn rolled_back_transactions(&self) -> usize {
        self.rolled_back.load(Ordering::SeqCst)
    }
}

impl BoltService for FakeService {
    fn run<'a>(&'a self, request: RunRequest) -> ServiceFuture<'a, RunOutcome> {
        Box::pin(async move {
            let _ = request;
            Ok(RunOutcome::new(CursorId::new(7), vec!["value".into()]))
        })
    }

    fn pull<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, PullOutcome> {
        Box::pin(async move {
            Ok(PullOutcome::new(
                vec![vec![Value::Integer(1)]],
                false,
                BTreeMap::new(),
            ))
        })
    }

    fn begin<'a>(&'a self, _extra: BTreeMap<String, Value>) -> ServiceFuture<'a, TransactionId> {
        Box::pin(async move { Ok(TransactionId::new(11)) })
    }

    fn commit<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, String> {
        Box::pin(async move {
            self.committed.fetch_add(1, Ordering::SeqCst);
            Ok("dtg:bookmark:1".into())
        })
    }

    fn rollback<'a>(&'a self, _transaction: TransactionId) -> ServiceFuture<'a, ()> {
        Box::pin(async move {
            self.rolled_back.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn discard<'a>(&'a self, _cursor: CursorId, _n: i64) -> ServiceFuture<'a, bool> {
        Box::pin(async move { Ok(false) })
    }

    fn route<'a>(
        &'a self,
        _routing: BTreeMap<String, Value>,
        _bookmarks: Vec<Value>,
        _database: Option<String>,
    ) -> ServiceFuture<'a, BTreeMap<String, Value>> {
        Box::pin(async move {
            Err(ServiceError::new(
                "Neo.ClientError.Request.Invalid",
                "unused",
            ))
        })
    }
}
