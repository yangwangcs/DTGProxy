use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::run_storage_tck;
use dtg_storage_fjall::FjallStorageTckFactory;

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Fjall storage future unexpectedly yielded"),
    }
}

#[test]
fn fjall_passes_the_shared_storage_tck() {
    let dir = tempfile::tempdir().unwrap();
    let factory = FjallStorageTckFactory::new(dir.path());
    block_on(run_storage_tck(&factory)).unwrap();
}
