#![forbid(unsafe_code)]

use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use query_executor::LocalExecutor;
use temporal_storage::TemporalStore;

const USAGE: &str = "Usage: dtgproxy [--version] | dtgproxy query --db <path> --text <query>";

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [] => {
            println!("DTGProxy Phase 1C local temporal query slice");
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        [argument] if argument == "--version" || argument == "-V" => {
            println!("DTGProxy {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        [command, db_flag, database, text_flag, query]
            if command == "query" && db_flag == "--db" && text_flag == "--text" =>
        {
            execute_query(database, query)
        }
        [command, ..] if command == "query" => {
            eprintln!("invalid query command");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        [argument, ..] => {
            eprintln!("unknown argument: {argument}");
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn execute_query(database: &str, query: &str) -> ExitCode {
    let plan = match temporal_query::parse(query) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("query parse error: {error}");
            return ExitCode::from(2);
        }
    };
    let adapter = match RocksAdapter::open(database) {
        Ok(adapter) => adapter,
        Err(error) => {
            eprintln!("database open error: {error}");
            return ExitCode::FAILURE;
        }
    };
    let executor = LocalExecutor::new(TemporalStore::new(adapter));
    let result = match block_on(executor.execute(&plan)) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("query execution error: {error}");
            return ExitCode::FAILURE;
        }
    };
    match result.to_canonical_json() {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("query output error: {error}");
            ExitCode::FAILURE
        }
    }
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
