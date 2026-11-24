//! A panic is the canonical failure a supervisor exists to handle. Wherever it
//! happens in user code, it must be reported as an abnormal exit so the
//! child's restart policy applies, and it must never take the supervisor down.

use ash_flare::{
    RestartBackoff, RestartPolicy, SupervisorError, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Where a test worker panics, on its first start only.
#[derive(Clone, Copy)]
enum PanicIn {
    Run,
    Initialize,
    Shutdown,
}

struct Panicky {
    starts: Arc<AtomicUsize>,
    first: bool,
    at: PanicIn,
}

impl Worker for Panicky {
    type Error = std::io::Error;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        self.first = self.starts.fetch_add(1, Ordering::SeqCst) == 0;
        if self.first && matches!(self.at, PanicIn::Initialize) {
            panic!("initialize blew up");
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        if self.first && matches!(self.at, PanicIn::Run) {
            panic!("run blew up");
        }
        if self.first && matches!(self.at, PanicIn::Shutdown) {
            // Fail normally so the shutdown hook runs on the way out.
            return Err(std::io::Error::other("first run fails"));
        }
        std::future::pending().await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        if self.first && matches!(self.at, PanicIn::Shutdown) {
            panic!("shutdown blew up");
        }
        Ok(())
    }
}

fn panicky(starts: &Arc<AtomicUsize>, at: PanicIn) -> impl Fn() -> Panicky + Send + Sync + 'static {
    let starts = Arc::clone(starts);
    move || Panicky {
        starts: Arc::clone(&starts),
        first: false,
        at,
    }
}

async fn started_twice_and_still_supervised(at: PanicIn) {
    let starts = Arc::new(AtomicUsize::new(0));
    let spec = SupervisorSpec::new("panics")
        .with_restart_backoff(RestartBackoff::none())
        .with_worker("w", panicky(&starts, at), RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec);

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(starts.load(Ordering::SeqCst), 2, "restarted exactly once");
    let children = handle.which_children().await.expect("supervisor alive");
    assert_eq!(children.len(), 1);
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_panic_in_run_restarts_the_worker() {
    started_twice_and_still_supervised(PanicIn::Run).await;
}

#[tokio::test]
async fn a_panic_in_initialize_restarts_the_worker() {
    started_twice_and_still_supervised(PanicIn::Initialize).await;
}

#[tokio::test]
async fn a_panic_in_the_shutdown_hook_does_not_stop_the_restart() {
    started_twice_and_still_supervised(PanicIn::Shutdown).await;
}

#[tokio::test]
async fn a_transient_worker_that_panics_counts_as_abnormal() {
    let starts = Arc::new(AtomicUsize::new(0));
    let spec = SupervisorSpec::new("transient-panic")
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "w",
            panicky(&starts, PanicIn::Run),
            RestartPolicy::Transient,
        );
    let handle = SupervisorHandle::start(spec);

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "transient restarts only on abnormal exits, and a panic is one"
    );
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_panicking_factory_does_not_take_the_supervisor_down() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let spec = SupervisorSpec::new("factory-panic")
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "w",
            move || {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("factory blew up");
                }
                Panicky {
                    starts: Arc::new(AtomicUsize::new(1)),
                    first: false,
                    at: PanicIn::Run,
                }
            },
            RestartPolicy::Permanent,
        );
    let handle = SupervisorHandle::start(spec);

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the failed start was retried"
    );
    assert_eq!(
        handle
            .which_children()
            .await
            .expect("supervisor alive")
            .len(),
        1
    );
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_linked_start_reports_a_panic_in_initialize() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("linked-panic"));
    let starts = Arc::new(AtomicUsize::new(0));

    let result = handle
        .start_child_linked(
            "w",
            panicky(&starts, PanicIn::Initialize),
            RestartPolicy::Permanent,
            Duration::from_secs(1),
        )
        .await;

    match result {
        Err(SupervisorError::InitializationFailed { reason, .. }) => {
            assert!(reason.contains("initialize blew up"), "got {reason}");
        }
        other => panic!("expected InitializationFailed, got {other:?}"),
    }
    handle.shutdown().await.expect("shutdown");
}
