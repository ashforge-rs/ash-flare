//! Isolated workers run on a thread of their own, so a worker that blocks or
//! hogs the CPU cannot starve the others.

use ash_flare::{
    ChildSpec, RestartBackoff, RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec,
    Worker,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Blocks its thread (no awaits at all) until asked to stop.
struct Blocker(ShutdownSignal);
impl Worker for Blocker {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        while !self.0.is_cancelled() {
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
}

/// Counts ticks of a short async sleep.
struct Ticker(Arc<AtomicUsize>);
impl Worker for Ticker {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// With a single runtime thread, a blocking worker would freeze everything.
/// Isolated, it leaves the shared thread free for the ticker.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn a_blocking_isolated_worker_does_not_starve_the_others() {
    let ticks = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ticks);
    let spec = SupervisorSpec::new("isolation")
        .with_child(ChildSpec::new("blocker", Blocker).isolated(true))
        .with_worker(
            "ticker",
            move || Ticker(Arc::clone(&counted)),
            RestartPolicy::Permanent,
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        ticks.load(Ordering::SeqCst) >= 10,
        "the ticker kept running: {} ticks",
        ticks.load(Ordering::SeqCst)
    );
    handle
        .shutdown()
        .await
        .expect("the blocker sees its shutdown signal and returns");
}

#[tokio::test]
async fn an_isolated_worker_runs_on_its_own_named_thread() {
    struct WhereAmI(Arc<Mutex<Option<String>>>);
    impl Worker for WhereAmI {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            *self.0.lock().unwrap() = std::thread::current().name().map(str::to_owned);
            std::future::pending().await
        }
    }

    let seen = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&seen);
    let handle =
        SupervisorHandle::start(SupervisorSpec::new("named").with_child(
            ChildSpec::new("probe", move || WhereAmI(Arc::clone(&slot))).isolated(true),
        ))
        .expect("valid spec");
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(seen.lock().unwrap().as_deref(), Some("ash-flare:probe"));
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_isolated_worker_is_restarted_after_a_crash() {
    struct CrashOnce(Arc<AtomicUsize>);
    impl Worker for CrashOnce {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                return Err(std::io::Error::other("crash"));
            }
            std::future::pending().await
        }
    }

    let starts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&starts);
    let handle = SupervisorHandle::start(
        SupervisorSpec::new("restart")
            .with_restart_backoff(RestartBackoff::none())
            .with_child(
                ChildSpec::new("w", move || CrashOnce(Arc::clone(&counted))).isolated(true),
            ),
    )
    .expect("valid spec");

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(starts.load(Ordering::SeqCst), 2);
    handle.shutdown().await.unwrap();
}

/// Killing an isolated worker drops it on its own thread, at its next await.
#[tokio::test]
async fn killing_an_isolated_worker_drops_it() {
    struct Waiting(Arc<AtomicBool>);
    impl Worker for Waiting {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            std::future::pending().await
        }
    }
    impl Drop for Waiting {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let handle = SupervisorHandle::start(
        SupervisorSpec::new("kill")
            .with_restart_backoff(RestartBackoff::none())
            .with_child(
                ChildSpec::new("w", move || Waiting(Arc::clone(&flag)))
                    .restart(RestartPolicy::Temporary)
                    .isolated(true),
            ),
    )
    .expect("valid spec");
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert_eq!(handle.kill_child("w").await, Ok(true));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        dropped.load(Ordering::SeqCst),
        "the worker was dropped on its thread"
    );
    handle.shutdown().await.unwrap();
}
