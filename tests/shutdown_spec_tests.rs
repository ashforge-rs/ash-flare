//! Per-child shutdown settings, the order children are stopped in, and spec
//! validation.

use ash_flare::{
    ChildSpec, RestartPolicy, Shutdown, ShutdownOrder, ShutdownSignal, SupervisorError,
    SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Records, on its way out, that it stopped and whether its hook ran.
struct Recorder {
    id: &'static str,
    shutdown: ShutdownSignal,
    log: Arc<Mutex<Vec<&'static str>>>,
    cleanup: Duration,
}

impl Worker for Recorder {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(self.cleanup).await;
        self.log.lock().unwrap().push(self.id);
        Ok(())
    }
}

fn recorder(
    id: &'static str,
    log: &Arc<Mutex<Vec<&'static str>>>,
    cleanup: Duration,
) -> impl Fn(ShutdownSignal) -> Recorder + Send + Sync + 'static {
    let log = Arc::clone(log);
    move |shutdown| Recorder {
        id,
        shutdown,
        log: Arc::clone(&log),
        cleanup,
    }
}

#[tokio::test]
async fn children_stop_in_reverse_start_order_by_default() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut spec = SupervisorSpec::new("ordered");
    for id in ["first", "second", "third"] {
        // The earliest child cleans up fastest: only stopping one at a time,
        // last first, keeps the log in reverse order.
        let cleanup = match id {
            "first" => Duration::from_millis(1),
            "second" => Duration::from_millis(20),
            _ => Duration::from_millis(40),
        };
        spec = spec.with_worker(id, recorder(id, &log, cleanup), RestartPolicy::Permanent);
    }

    let handle = SupervisorHandle::start(spec).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.expect("shutdown");

    assert_eq!(*log.lock().unwrap(), vec!["third", "second", "first"]);
}

#[tokio::test]
async fn concurrent_order_stops_children_together() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = SupervisorSpec::new("concurrent")
        .with_shutdown_order(ShutdownOrder::Concurrent)
        .with_worker(
            "slow",
            recorder("slow", &log, Duration::from_millis(80)),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "fast",
            recorder("fast", &log, Duration::from_millis(1)),
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    handle.shutdown().await.expect("shutdown");

    assert_eq!(
        *log.lock().unwrap(),
        vec!["fast", "slow"],
        "both were stopping at once"
    );
    assert!(started.elapsed() < Duration::from_millis(160));
}

#[tokio::test]
async fn brutal_kill_skips_the_shutdown_hook() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = SupervisorSpec::new("brutal")
        .with_child(
            ChildSpec::new("killed", recorder("killed", &log, Duration::ZERO))
                .shutdown(Shutdown::BrutalKill),
        )
        .with_worker(
            "graceful",
            recorder("graceful", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.expect("shutdown");

    assert_eq!(*log.lock().unwrap(), vec!["graceful"]);
}

#[tokio::test]
async fn a_child_timeout_overrides_the_supervisor_default() {
    let log = Arc::new(Mutex::new(Vec::new()));
    // The supervisor default would abort this 150ms cleanup; the child's own
    // longer timeout lets it finish.
    let spec = SupervisorSpec::new("override")
        .with_shutdown_timeout(Duration::from_millis(20))
        .with_child(
            ChildSpec::new(
                "patient",
                recorder("patient", &log, Duration::from_millis(150)),
            )
            .shutdown(Shutdown::Timeout(Duration::from_secs(5))),
        )
        .with_child(
            ChildSpec::new(
                "forever",
                recorder("forever", &log, Duration::from_millis(150)),
            )
            .shutdown(Shutdown::Infinity),
        )
        .with_worker(
            "default",
            recorder("default", &log, Duration::from_millis(150)),
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.shutdown().await.expect("shutdown");

    let mut finished = log.lock().unwrap().clone();
    finished.sort_unstable();
    assert_eq!(
        finished,
        vec!["forever", "patient"],
        "the default child was aborted"
    );
}

#[tokio::test]
async fn duplicate_child_ids_are_rejected_at_start() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let spec = SupervisorSpec::new("dupes")
        .with_worker(
            "same",
            recorder("a", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "same",
            recorder("b", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        );

    match SupervisorHandle::start(spec) {
        Err(SupervisorError::DuplicateChildId {
            supervisor,
            child_id,
        }) => {
            assert_eq!((supervisor.as_str(), child_id.as_str()), ("dupes", "same"));
        }
        other => panic!("expected DuplicateChildId, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicates_are_found_in_nested_supervisors_and_across_kinds() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let nested = SupervisorSpec::new("inner")
        .with_worker(
            "x",
            recorder("x", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "x",
            recorder("y", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        );
    let spec = SupervisorSpec::new("outer").with_supervisor(nested);
    assert!(matches!(
        SupervisorHandle::start(spec),
        Err(SupervisorError::DuplicateChildId { supervisor, .. }) if supervisor == "inner"
    ));

    // A worker and a nested supervisor share one id space.
    let spec = SupervisorSpec::new("outer")
        .with_worker(
            "inner",
            recorder("w", &log, Duration::ZERO),
            RestartPolicy::Permanent,
        )
        .with_supervisor(SupervisorSpec::new("inner"));
    assert!(matches!(
        SupervisorHandle::start(spec),
        Err(SupervisorError::DuplicateChildId { child_id, .. }) if child_id == "inner"
    ));
}

#[tokio::test]
async fn start_child_spec_applies_the_childs_shutdown() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let handle = SupervisorHandle::start(SupervisorSpec::new("dynamic")).expect("valid spec");
    handle
        .start_child_spec(
            ChildSpec::new("killed", recorder("killed", &log, Duration::ZERO))
                .shutdown(Shutdown::BrutalKill),
        )
        .await
        .expect("started");
    tokio::time::sleep(Duration::from_millis(20)).await;

    handle.terminate_child("killed").await.expect("terminated");
    assert!(
        log.lock().unwrap().is_empty(),
        "brutal_kill applies to terminate_child too"
    );
    handle.shutdown().await.expect("shutdown");
}
