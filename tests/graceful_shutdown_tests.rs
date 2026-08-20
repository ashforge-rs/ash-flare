//! Regression tests for cooperative worker shutdown.
//!
//! `Worker::shutdown` is documented as running when a worker is being shut
//! down. Before the shutdown protocol existed, termination went straight to
//! `JoinHandle::abort`, so the hook never ran and any cleanup was silently lost.

use ash_flare::{
    RestartPolicy, ShutdownSignal, StatefulSupervisorHandle, StatefulSupervisorSpec,
    SupervisorHandle, SupervisorSpec, Worker, WorkerContext,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Cooperative worker: parks until told to stop, then records its cleanup.
struct Cooperative {
    shutdown: ShutdownSignal,
    cleaned_up: Arc<AtomicBool>,
}

#[async_trait]
impl Worker for Cooperative {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.cleaned_up.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Never observes the signal, but does yield at await points, so the runtime
/// can cancel its `run` future and still give it a cleanup pass.
struct Stubborn {
    cleaned_up: Arc<AtomicBool>,
}

#[async_trait]
impl Worker for Stubborn {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.cleaned_up.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// Observes the signal but takes far longer than the shutdown timeout to finish
/// cleaning up, so it must be abandoned rather than waited on forever.
struct SlowCleanup {
    shutdown: ShutdownSignal,
    finished_cleanup: Arc<AtomicBool>,
}

#[async_trait]
impl Worker for SlowCleanup {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        self.finished_cleanup.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_hook_runs_on_graceful_supervisor_shutdown() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cleaned);

    let spec = SupervisorSpec::new("root").with_worker_signal(
        "w",
        move |signal| Cooperative {
            shutdown: signal,
            cleaned_up: Arc::clone(&flag),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown().await.expect("shutdown request");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "Worker::shutdown must run when the supervisor shuts down gracefully"
    );
}

#[tokio::test]
async fn shutdown_hook_runs_on_terminate_child() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cleaned);

    let spec = SupervisorSpec::new("root").with_worker_signal(
        "w",
        move |signal| Cooperative {
            shutdown: signal,
            cleaned_up: Arc::clone(&flag),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.terminate_child("w").await.expect("terminate");
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "Worker::shutdown must run when a child is terminated"
    );
}

#[tokio::test]
async fn worker_ignoring_signal_is_still_cancelled_and_cleaned_up() {
    // A worker that never checks the signal still has its `run` future dropped
    // at an await point, and its cleanup hook runs. This is strictly better than
    // the old behaviour, where termination aborted the task and skipped cleanup.
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cleaned);

    let spec = SupervisorSpec::new("root")
        .with_shutdown_timeout(Duration::from_millis(500))
        .with_worker(
            "w",
            move || Stubborn {
                cleaned_up: Arc::clone(&flag),
            },
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown().await.expect("shutdown request");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "a cancellable worker gets its shutdown hook even without observing the signal"
    );
}

#[tokio::test]
async fn slow_cleanup_is_bounded_by_shutdown_timeout() {
    // Cleanup that overruns the timeout must be abandoned, so one bad worker
    // cannot stall the whole tree's shutdown.
    let finished = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&finished);

    let spec = SupervisorSpec::new("root")
        .with_shutdown_timeout(Duration::from_millis(150))
        .with_worker_signal(
            "w",
            move |signal| SlowCleanup {
                shutdown: signal,
                finished_cleanup: Arc::clone(&flag),
            },
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    handle.terminate_child("w").await.expect("terminate");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "slow cleanup must not stall termination indefinitely, took {elapsed:?}"
    );
    assert!(
        !finished.load(Ordering::SeqCst),
        "cleanup exceeding the timeout must be abandoned, not awaited to completion"
    );
}

#[tokio::test]
async fn plain_worker_api_still_works_without_signal() {
    // Workers registered through `with_worker` never see the signal, but must
    // still be shut down cleanly.
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);

    struct Quick {
        runs: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Worker for Quick {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        }
    }

    let spec = SupervisorSpec::new("root").with_worker(
        "w",
        move || Quick {
            runs: Arc::clone(&counter),
        },
        RestartPolicy::Temporary,
    );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(200)).await;
    handle.shutdown().await.expect("shutdown");

    assert!(runs.load(Ordering::SeqCst) > 0, "worker should have run");
}

#[tokio::test]
async fn stateful_shutdown_hook_runs_and_context_survives() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cleaned);

    struct StatefulCooperative {
        shutdown: ShutdownSignal,
        context: Arc<WorkerContext>,
        cleaned_up: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Worker for StatefulCooperative {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            self.shutdown.cancelled().await;
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), Self::Error> {
            // Cleanup can still reach shared state, which is the point of
            // having a graceful hook at all.
            self.context.set("drained", serde_json::json!(true));
            self.cleaned_up.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let spec = StatefulSupervisorSpec::new("root").with_worker_signal(
        "w",
        move |ctx, signal| StatefulCooperative {
            shutdown: signal,
            context: ctx,
            cleaned_up: Arc::clone(&flag),
        },
        RestartPolicy::Permanent,
    );
    let context = Arc::clone(spec.context());

    let handle = StatefulSupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown().await.expect("shutdown request");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(
        cleaned.load(Ordering::SeqCst),
        "stateful Worker::shutdown must run on graceful shutdown"
    );
    assert_eq!(
        context.get("drained"),
        Some(serde_json::json!(true)),
        "cleanup must be able to write to the shared context"
    );
}

#[tokio::test]
async fn shutdown_signal_never_does_not_fire() {
    let signal = ShutdownSignal::never();
    assert!(!signal.is_cancelled());

    let fired = tokio::time::timeout(Duration::from_millis(150), signal.cancelled()).await;
    assert!(fired.is_err(), "`never` must not resolve");
}
