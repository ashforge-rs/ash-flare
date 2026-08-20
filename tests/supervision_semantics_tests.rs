//! Supervision semantics that are easy to get wrong and expensive to get wrong:
//! what a deliberate stop means, what a restart round does to siblings, and what
//! happens when a nested supervisor gives up.

use ash_flare::{
    ChildType, RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy,
    StatefulSupervisorHandle, StatefulSupervisorSpec, SupervisorHandle, SupervisorSpec, Worker,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time::{Duration, Instant, sleep, timeout};

/// Counts every start and fails immediately on the first one only.
struct CrashOnce {
    starts: Arc<AtomicU32>,
}

#[async_trait]
impl Worker for CrashOnce {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(std::io::Error::other("first run always fails"));
        }
        idle().await
    }
}

/// Counts every start and fails immediately, every time.
struct CrashAlways {
    starts: Arc<AtomicU32>,
}

#[async_trait]
impl Worker for CrashAlways {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("boom"))
    }
}

/// Counts every start, then runs until the supervisor stops it.
struct Idle {
    starts: Arc<AtomicU32>,
}

#[async_trait]
impl Worker for Idle {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        idle().await
    }
}

async fn idle() -> Result<(), std::io::Error> {
    loop {
        sleep(Duration::from_secs(3600)).await;
    }
}

fn counter() -> Arc<AtomicU32> {
    Arc::new(AtomicU32::new(0))
}

fn crash_once(starts: &Arc<AtomicU32>) -> impl Fn() -> CrashOnce + Send + Sync + 'static {
    let starts = Arc::clone(starts);
    move || CrashOnce {
        starts: Arc::clone(&starts),
    }
}

fn idle_worker(starts: &Arc<AtomicU32>) -> impl Fn() -> Idle + Send + Sync + 'static {
    let starts = Arc::clone(starts);
    move || Idle {
        starts: Arc::clone(&starts),
    }
}

/// Workers used together in one spec must share a type, so the two shapes above
/// are wrapped in a single enum for the strategy tests.
enum Child {
    CrashOnce(CrashOnce),
    Idle(Idle),
}

#[async_trait]
impl Worker for Child {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        match self {
            Child::CrashOnce(w) => w.run().await,
            Child::Idle(w) => w.run().await,
        }
    }
}

fn crashing_child(starts: &Arc<AtomicU32>) -> impl Fn() -> Child + Send + Sync + 'static {
    let factory = crash_once(starts);
    move || Child::CrashOnce(factory())
}

fn idle_child(starts: &Arc<AtomicU32>) -> impl Fn() -> Child + Send + Sync + 'static {
    let factory = idle_worker(starts);
    move || Child::Idle(factory())
}

/// A `Shutdown` exit is the supervisor's own doing. Feeding it back through the
/// restart policy turned a single failure into an unbounded restart storm that
/// exhausted the restart intensity and killed the supervisor.
#[tokio::test]
async fn one_for_all_restarts_each_child_exactly_once_per_failure() {
    let crasher = counter();
    let sibling = counter();

    let spec = SupervisorSpec::new("one-for-all")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(3, 60))
        .with_worker(
            "crasher",
            crashing_child(&crasher),
            RestartPolicy::Permanent,
        )
        .with_worker("sibling", idle_child(&sibling), RestartPolicy::Permanent);

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(300)).await;

    assert_eq!(
        crasher.load(Ordering::SeqCst),
        2,
        "the failing child starts once and is restarted once"
    );
    assert_eq!(
        sibling.load(Ordering::SeqCst),
        2,
        "its sibling is restarted once, not restarted in a loop by its own stop"
    );

    let children = handle
        .which_children()
        .await
        .expect("supervisor must still be alive, not killed by a self-inflicted storm");
    assert_eq!(children.len(), 2);

    handle.shutdown().await.expect("shutdown");
}

/// A healthy `Transient` sibling stopped as part of a restart round must come
/// back; it was previously dropped from supervision, because its `Shutdown`
/// exit was read as "terminated normally, do not restart".
#[tokio::test]
async fn one_for_all_keeps_a_transient_sibling_under_supervision() {
    let crasher = counter();
    let sibling = counter();

    let spec = SupervisorSpec::new("transient-sibling")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "crasher",
            crashing_child(&crasher),
            RestartPolicy::Permanent,
        )
        .with_worker("sibling", idle_child(&sibling), RestartPolicy::Transient);

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(300)).await;

    let mut ids: Vec<String> = handle
        .which_children()
        .await
        .expect("supervisor alive")
        .into_iter()
        .map(|c| c.id)
        .collect();
    ids.sort();

    assert_eq!(ids, vec!["crasher".to_owned(), "sibling".to_owned()]);
    assert_eq!(
        sibling.load(Ordering::SeqCst),
        2,
        "the transient sibling is restarted with the rest"
    );

    handle.shutdown().await.expect("shutdown");
}

/// A `Temporary` child is never restarted - not even when it was a sibling's
/// failure that stopped it - so it leaves supervision, as OTP does.
#[tokio::test]
async fn one_for_all_drops_a_temporary_sibling() {
    let crasher = counter();
    let sibling = counter();

    let spec = SupervisorSpec::new("temporary-sibling")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "crasher",
            crashing_child(&crasher),
            RestartPolicy::Permanent,
        )
        .with_worker("sibling", idle_child(&sibling), RestartPolicy::Temporary);

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(300)).await;

    let ids: Vec<String> = handle
        .which_children()
        .await
        .expect("supervisor alive")
        .into_iter()
        .map(|c| c.id)
        .collect();

    assert_eq!(ids, vec!["crasher".to_owned()]);
    assert_eq!(
        sibling.load(Ordering::SeqCst),
        1,
        "a temporary child is not restarted"
    );

    handle.shutdown().await.expect("shutdown");
}

/// `RestForOne` restarts the failed child and everything started after it - once.
#[tokio::test]
async fn rest_for_one_restarts_the_tail_exactly_once() {
    let untouched = counter();
    let crasher = counter();
    let after = counter();

    let spec = SupervisorSpec::new("rest-for-one")
        .with_restart_strategy(RestartStrategy::RestForOne)
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(3, 60))
        .with_worker("first", idle_child(&untouched), RestartPolicy::Permanent)
        .with_worker(
            "crasher",
            crashing_child(&crasher),
            RestartPolicy::Permanent,
        )
        .with_worker("last", idle_child(&after), RestartPolicy::Permanent);

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(300)).await;

    assert_eq!(
        untouched.load(Ordering::SeqCst),
        1,
        "children before it stay"
    );
    assert_eq!(crasher.load(Ordering::SeqCst), 2);
    assert_eq!(
        after.load(Ordering::SeqCst),
        2,
        "restarted once, not looping"
    );

    let children = handle.which_children().await.expect("supervisor alive");
    assert_eq!(children.len(), 3);
    let ids: Vec<&str> = children.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["first", "crasher", "last"],
        "rest_for_one must preserve start order"
    );

    handle.shutdown().await.expect("shutdown");
}

/// The restart backoff used to be slept through inline in the command loop, so
/// every other command - shutdown included - stalled for the whole delay.
#[tokio::test]
async fn a_backing_off_child_does_not_stall_the_supervisor() {
    let crasher = counter();

    let spec = SupervisorSpec::new("slow-backoff")
        .with_restart_delay(Duration::from_secs(10))
        .with_worker("crasher", crash_once(&crasher), RestartPolicy::Permanent);

    let handle = SupervisorHandle::start(spec);

    // Let the child fail and the supervisor enter its backoff.
    sleep(Duration::from_millis(100)).await;
    assert_eq!(crasher.load(Ordering::SeqCst), 1, "child has failed once");

    let asked_at = Instant::now();
    let children = timeout(Duration::from_millis(500), handle.which_children())
        .await
        .expect("which_children must not wait out the 10s backoff")
        .expect("supervisor alive");

    assert_eq!(
        children.len(),
        1,
        "the child is still supervised while it backs off"
    );
    assert!(
        asked_at.elapsed() < Duration::from_millis(500),
        "answered in {:?}",
        asked_at.elapsed()
    );

    handle.shutdown().await.expect("shutdown");
}

/// A nested supervisor that exhausts its restart intensity terminates. Its
/// parent has to notice: otherwise the subtree stays dead and listed forever.
#[tokio::test]
async fn a_parent_restarts_a_nested_supervisor_that_escalated() {
    let starts = counter();
    let factory = {
        let starts = Arc::clone(&starts);
        move || CrashAlways {
            starts: Arc::clone(&starts),
        }
    };

    // The nested supervisor gives up after 2 restarts, i.e. 3 worker starts.
    let nested = SupervisorSpec::new("nested")
        .with_restart_intensity(RestartIntensity::new(2, 60))
        .with_restart_backoff(RestartBackoff::none())
        .with_worker("doomed", factory, RestartPolicy::Permanent);

    let parent = SupervisorSpec::new("parent")
        .with_restart_intensity(RestartIntensity::new(100, 60))
        .with_restart_backoff(RestartBackoff::exponential(
            Duration::from_millis(50),
            Duration::from_millis(50),
        ))
        .with_supervisor(nested);

    let handle = SupervisorHandle::start(parent);
    sleep(Duration::from_millis(500)).await;

    assert!(
        starts.load(Ordering::SeqCst) > 3,
        "the parent must restart the escalated subtree; saw {} worker starts, \
         which is one generation of the nested supervisor and no more",
        starts.load(Ordering::SeqCst)
    );

    let children = handle.which_children().await.expect("parent alive");
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].child_type, ChildType::Supervisor);

    handle.shutdown().await.expect("shutdown");
}

/// The stateful runtime carries the same supervision logic and needs the same
/// guarantee: one failure, one restart round.
#[tokio::test]
async fn stateful_one_for_all_restarts_each_child_exactly_once() {
    let crasher = counter();
    let sibling = counter();

    let crasher_factory = {
        let starts = Arc::clone(&crasher);
        move |_ctx| {
            Child::CrashOnce(CrashOnce {
                starts: Arc::clone(&starts),
            })
        }
    };
    let sibling_factory = {
        let starts = Arc::clone(&sibling);
        move |_ctx| {
            Child::Idle(Idle {
                starts: Arc::clone(&starts),
            })
        }
    };

    let spec = StatefulSupervisorSpec::new("stateful-one-for-all")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(3, 60))
        .with_worker("crasher", crasher_factory, RestartPolicy::Permanent)
        .with_worker("sibling", sibling_factory, RestartPolicy::Permanent);

    let handle = StatefulSupervisorHandle::start(spec);
    sleep(Duration::from_millis(300)).await;

    assert_eq!(crasher.load(Ordering::SeqCst), 2);
    assert_eq!(
        sibling.load(Ordering::SeqCst),
        2,
        "sibling restarted once, not looping on its own stop"
    );

    let children = handle
        .which_children()
        .await
        .expect("stateful supervisor must still be alive");
    assert_eq!(children.len(), 2);

    handle.shutdown().await.expect("shutdown");
}
