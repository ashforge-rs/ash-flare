//! Killing children, and links that share a fate between two children.

use ash_flare::{
    ChildExitReason, RestartBackoff, RestartIntensity, RestartPolicy, ShutdownSignal,
    SupervisorError, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker, link,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Counts its starts and runs until stopped; records whether its shutdown
/// hook ran.
struct Counted {
    starts: Arc<AtomicUsize>,
    hook_ran: Arc<AtomicBool>,
    shutdown: ShutdownSignal,
}
impl Worker for Counted {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.shutdown.cancelled().await;
        Ok(())
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.hook_ran.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Probe {
    starts: Arc<AtomicUsize>,
    hook_ran: Arc<AtomicBool>,
}

impl Probe {
    fn factory(&self) -> impl Fn(ShutdownSignal) -> Counted + Send + Sync + 'static {
        let probe = self.clone();
        move |shutdown| Counted {
            starts: Arc::clone(&probe.starts),
            hook_ran: Arc::clone(&probe.hook_ran),
            shutdown,
        }
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }
}

/// No backoff, and room for the restarts these tests cause on purpose.
fn spec(name: &str) -> SupervisorSpec {
    SupervisorSpec::new(name)
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(100, 60))
}

const SETTLE: Duration = Duration::from_millis(80);

#[tokio::test]
async fn kill_child_restarts_a_permanent_child_without_its_hook() {
    let probe = Probe::default();
    let handle = SupervisorHandle::start(spec("kill").with_worker(
        "w",
        probe.factory(),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");
    let mut events = handle.subscribe();
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(handle.kill_child("w").await, Ok(true));
    tokio::time::sleep(SETTLE).await;

    assert_eq!(probe.starts(), 2, "restarted");
    assert!(
        !probe.hook_ran.load(Ordering::SeqCst),
        "a kill skips the shutdown hook"
    );
    assert_eq!(
        events.recv().await.unwrap(),
        SupervisorEvent::ChildTerminated {
            id: "w".into(),
            reason: ChildExitReason::Killed
        }
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_killed_transient_child_is_restarted_and_a_temporary_one_dropped() {
    let transient = Probe::default();
    let temporary = Probe::default();
    let handle = SupervisorHandle::start(
        spec("policies")
            .with_worker("transient", transient.factory(), RestartPolicy::Transient)
            .with_worker("temporary", temporary.factory(), RestartPolicy::Temporary),
    )
    .expect("valid spec");
    tokio::time::sleep(Duration::from_millis(20)).await;

    handle.kill_child("transient").await.unwrap();
    handle.kill_child("temporary").await.unwrap();
    tokio::time::sleep(SETTLE).await;

    assert_eq!(
        transient.starts(),
        2,
        "being killed is abnormal, so transient restarts"
    );
    let ids: Vec<String> = handle
        .which_children()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(ids, vec!["transient"]);
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn kills_count_towards_restart_intensity() {
    let probe = Probe::default();
    let handle = SupervisorHandle::start(
        spec("intensity")
            .with_restart_intensity(RestartIntensity::new(1, 60))
            .with_worker("w", probe.factory(), RestartPolicy::Permanent),
    )
    .expect("valid spec");
    tokio::time::sleep(Duration::from_millis(20)).await;

    handle.kill_child("w").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let _ = handle.kill_child("w").await;
    tokio::time::sleep(SETTLE).await;

    assert!(
        handle.is_stopped(),
        "two kills exceed an intensity of one restart"
    );
}

#[tokio::test]
async fn kill_child_reports_an_unknown_child() {
    let handle = SupervisorHandle::start(spec("unknown")).expect("valid spec");
    assert_eq!(
        handle.kill_child("ghost").await,
        Err(SupervisorError::ChildNotFound("ghost".to_owned()))
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn killing_a_nested_supervisor_restarts_its_subtree() {
    let probe = Probe::default();
    let nested = spec("inner").with_worker("w", probe.factory(), RestartPolicy::Permanent);
    let handle =
        SupervisorHandle::start(spec("outer").with_supervisor(nested)).expect("valid spec");
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(handle.kill_child("inner").await, Ok(true));
    tokio::time::sleep(SETTLE).await;

    assert_eq!(probe.starts(), 2, "the subtree was started again");
    assert_eq!(handle.which_children().await.unwrap().len(), 1);
    handle.shutdown().await.unwrap();
}

/// Fails once, `after` into its first run, then stays up.
struct CrashOnce {
    starts: Arc<AtomicUsize>,
    after: Duration,
}
impl Worker for CrashOnce {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.after).await;
            return Err(std::io::Error::other("crash"));
        }
        std::future::pending().await
    }
}

fn crash_once(
    starts: &Arc<AtomicUsize>,
    after_ms: u64,
) -> impl Fn() -> CrashOnce + Send + Sync + 'static {
    let starts = Arc::clone(starts);
    move || CrashOnce {
        starts: Arc::clone(&starts),
        after: Duration::from_millis(after_ms),
    }
}

#[tokio::test]
async fn a_crash_kills_the_linked_child_in_another_supervisor_once() {
    let crasher = Arc::new(AtomicUsize::new(0));
    let partner = Probe::default();
    let a = SupervisorHandle::start(spec("a").with_worker(
        "crasher",
        crash_once(&crasher, 40),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");
    let b = SupervisorHandle::start(spec("b").with_worker(
        "partner",
        partner.factory(),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");

    let _link = link(&a, "crasher", &b, "partner");
    tokio::time::sleep(Duration::from_millis(250)).await;

    assert_eq!(
        crasher.load(Ordering::SeqCst),
        2,
        "the crasher restarted once"
    );
    assert_eq!(
        partner.starts(),
        2,
        "its partner was killed and restarted once, with no loop"
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_link_works_in_both_directions_and_within_one_supervisor() {
    let left = Probe::default();
    let right = Probe::default();
    let sup = SupervisorHandle::start(
        spec("same")
            .with_worker("left", left.factory(), RestartPolicy::Permanent)
            .with_worker("right", right.factory(), RestartPolicy::Permanent),
    )
    .expect("valid spec");
    let _link = link(&sup, "left", &sup, "right");
    tokio::time::sleep(Duration::from_millis(20)).await;

    sup.kill_child("right").await.unwrap();
    tokio::time::sleep(SETTLE).await;
    assert_eq!((left.starts(), right.starts()), (2, 2));

    sup.kill_child("left").await.unwrap();
    tokio::time::sleep(SETTLE).await;
    assert_eq!((left.starts(), right.starts()), (3, 3));
    sup.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_normal_exit_does_not_travel_along_a_link() {
    struct Finishes;
    impl Worker for Finishes {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(())
        }
    }

    let partner = Probe::default();
    let sup = SupervisorHandle::start(
        spec("normal")
            .with_worker("job", || Finishes, RestartPolicy::Transient)
            .with_worker("partner", partner.factory(), RestartPolicy::Permanent),
    )
    .expect("valid spec");
    let _link = link(&sup, "job", &sup, "partner");

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(partner.starts(), 1);
    sup.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutting_down_one_side_kills_the_other() {
    let gone = Probe::default();
    let partner = Probe::default();
    let a = SupervisorHandle::start(spec("a").with_worker(
        "gone",
        gone.factory(),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");
    let b = SupervisorHandle::start(spec("b").with_worker(
        "partner",
        partner.factory(),
        RestartPolicy::Permanent,
    ))
    .expect("valid spec");
    let _link = link(&a, "gone", &b, "partner");
    tokio::time::sleep(Duration::from_millis(20)).await;

    a.shutdown().await.unwrap();
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        partner.starts(),
        2,
        "shutdown is not a normal exit, so it propagates"
    );
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_the_link_unlinks() {
    let left = Probe::default();
    let right = Probe::default();
    let sup = SupervisorHandle::start(
        spec("unlinked")
            .with_worker("left", left.factory(), RestartPolicy::Permanent)
            .with_worker("right", right.factory(), RestartPolicy::Permanent),
    )
    .expect("valid spec");
    let linked = link(&sup, "left", &sup, "right");
    assert!(linked.is_active());
    drop(linked);
    tokio::time::sleep(Duration::from_millis(20)).await;

    sup.kill_child("left").await.unwrap();
    tokio::time::sleep(SETTLE).await;
    assert_eq!((left.starts(), right.starts()), (2, 1));
    assert!(
        !link(&sup, "left", &sup, "left").is_active(),
        "a self-link is inert"
    );
    sup.shutdown().await.unwrap();
}
