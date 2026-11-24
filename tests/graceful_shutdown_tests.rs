//! Regression tests for cooperative worker shutdown.
//!
//! `Worker::shutdown` is documented as running when a worker is being shut
//! down. Before the shutdown protocol existed, termination went straight to
//! `JoinHandle::abort`, so the hook never ran and any cleanup was silently lost.

use ash_flare::{
    RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker, WorkerContext,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Cooperative worker: parks until told to stop, then records its cleanup.
struct Cooperative {
    shutdown: ShutdownSignal,
    cleaned_up: Arc<AtomicBool>,
}
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

    let spec = SupervisorSpec::new("root").with_worker(
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

    let spec = SupervisorSpec::new("root").with_worker(
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
        .with_worker(
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
async fn shutdown_hook_can_write_to_a_shared_context() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&cleaned);

    struct StatefulCooperative {
        shutdown: ShutdownSignal,
        context: WorkerContext,
        cleaned_up: Arc<AtomicBool>,
    }
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

    let context = WorkerContext::new();
    let worker_context = context.clone();
    let spec = SupervisorSpec::new("root").with_worker(
        "w",
        move |signal| StatefulCooperative {
            shutdown: signal,
            context: worker_context.clone(),
            cleaned_up: Arc::clone(&flag),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown().await.expect("shutdown");

    assert!(
        cleaned.load(Ordering::SeqCst),
        "Worker::shutdown must run on graceful shutdown"
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

/// `SupervisorHandle::shutdown` resolves only once the whole tree has stopped,
/// nested supervisors included, so no sleep is needed to observe cleanup.
#[tokio::test]
async fn shutdown_waits_for_the_whole_tree() {
    let cleaned = Arc::new(AtomicUsize::new(0));

    let cooperative = |cleaned: &Arc<AtomicUsize>| {
        let cleaned = Arc::clone(cleaned);
        move |signal: ShutdownSignal| SlowCleanup {
            shutdown: signal,
            cleaned: Arc::clone(&cleaned),
        }
    };

    struct SlowCleanup {
        shutdown: ShutdownSignal,
        cleaned: Arc<AtomicUsize>,
    }
    impl Worker for SlowCleanup {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            self.shutdown.cancelled().await;
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), Self::Error> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let nested = SupervisorSpec::new("nested").with_worker(
        "inner",
        cooperative(&cleaned),
        RestartPolicy::Permanent,
    );
    let spec = SupervisorSpec::new("root")
        .with_worker("outer", cooperative(&cleaned), RestartPolicy::Permanent)
        .with_supervisor(nested);

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.expect("shutdown");

    assert_eq!(
        cleaned.load(Ordering::SeqCst),
        2,
        "both shutdown hooks must have finished when shutdown returns"
    );
    assert!(handle.is_stopped());
    assert!(handle.shutdown().await.is_err(), "already stopped");
}

/// Children are stopped together and share one shutdown timeout, so the total
/// is bounded by the timeout rather than growing with the number of children.
#[tokio::test]
async fn children_share_one_shutdown_timeout() {
    struct OverrunningCleanup;
    impl Worker for OverrunningCleanup {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            std::future::pending().await
        }

        async fn shutdown(&mut self) -> Result<(), Self::Error> {
            // Far longer than the timeout: the worker has to be aborted.
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        }
    }

    let timeout = Duration::from_millis(200);
    let mut spec = SupervisorSpec::new("root").with_shutdown_timeout(timeout);
    for i in 0..3 {
        spec = spec.with_worker(
            format!("w{i}"),
            || OverrunningCleanup,
            RestartPolicy::Permanent,
        );
    }

    let handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let started = std::time::Instant::now();
    handle.shutdown().await.expect("shutdown");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= timeout,
        "workers must get the full timeout, took {elapsed:?}"
    );
    assert!(
        elapsed < timeout * 2,
        "three overrunning workers must share one timeout (sequential would be {:?}), took {elapsed:?}",
        timeout * 3
    );
}

/// A `RestForOne` restart stops the affected siblings together, so restarting
/// them takes about one cleanup rather than one per sibling.
#[tokio::test]
async fn rest_for_one_stops_affected_siblings_together() {
    use ash_flare::{RestartBackoff, RestartStrategy};

    /// Fails 50ms into its first run, then stays up.
    struct CrashOnce {
        starts: Arc<AtomicUsize>,
    }
    impl Worker for CrashOnce {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(std::io::Error::other("first run fails"));
            }
            std::future::pending().await
        }
    }

    /// Counts its starts and needs 100ms to clean up.
    struct SlowSibling {
        starts: Arc<AtomicUsize>,
        shutdown: ShutdownSignal,
    }
    impl Worker for SlowSibling {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.shutdown.cancelled().await;
            Ok(())
        }

        async fn shutdown(&mut self) -> Result<(), Self::Error> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
    }

    let crasher_starts = Arc::new(AtomicUsize::new(0));
    let crasher = Arc::clone(&crasher_starts);
    let mut spec = SupervisorSpec::new("root")
        .with_restart_strategy(RestartStrategy::RestForOne)
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "crasher",
            move || CrashOnce {
                starts: Arc::clone(&crasher),
            },
            RestartPolicy::Permanent,
        );

    let siblings: Vec<Arc<AtomicUsize>> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    for (i, starts) in siblings.iter().enumerate() {
        let starts = Arc::clone(starts);
        spec = spec.with_worker(
            format!("sibling-{i}"),
            move |shutdown| SlowSibling {
                starts: Arc::clone(&starts),
                shutdown,
            },
            RestartPolicy::Permanent,
        );
    }

    let handle = SupervisorHandle::start(spec);
    // Crash at ~50ms. Stopped together, every sibling is back by ~150ms; one
    // after another, the last would only be back at ~450ms.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let restarted: Vec<usize> = siblings.iter().map(|s| s.load(Ordering::SeqCst)).collect();
    assert_eq!(
        crasher_starts.load(Ordering::SeqCst),
        2,
        "crasher restarted once"
    );
    assert_eq!(
        restarted,
        vec![2; 4],
        "every sibling restarted once by 300ms"
    );

    handle.shutdown().await.expect("shutdown");
}
