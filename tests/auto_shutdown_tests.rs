//! Significant children and auto shutdown: a supervisor that stops itself once
//! the work it exists for is done.

use ash_flare::{
    AutoShutdown, ChildExitReason, ChildSpec, ChildType, RestartPolicy, ShutdownSignal,
    SupervisorError, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker,
};
use std::time::Duration;

/// Finishes normally after `after`.
struct Job {
    after: Duration,
}
impl Worker for Job {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(self.after).await;
        Ok(())
    }
}

fn job(after_ms: u64) -> impl Fn() -> Job + Send + Sync + 'static {
    move || Job {
        after: Duration::from_millis(after_ms),
    }
}

/// Runs until stopped.
struct Service(ShutdownSignal);
impl Worker for Service {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

fn significant(id: &str, after_ms: u64, policy: RestartPolicy) -> ChildSpec {
    ChildSpec::new(id, job(after_ms))
        .restart(policy)
        .significant(true)
}

#[tokio::test]
async fn any_significant_stops_the_supervisor_when_one_ends() {
    let spec = SupervisorSpec::new("any")
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        .with_child(significant("job", 30, RestartPolicy::Transient))
        .with_worker("service", Service, RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        handle.is_stopped(),
        "the significant job ended, so the supervisor stopped"
    );

    let mut stopped = None;
    while let Ok(event) = events.try_recv() {
        if let SupervisorEvent::Stopped { reason } = event {
            stopped = Some(reason);
        }
    }
    assert_eq!(stopped, Some(ChildExitReason::Normal));
}

#[tokio::test]
async fn all_significant_waits_for_every_significant_child() {
    let spec = SupervisorSpec::new("all")
        .with_auto_shutdown(AutoShutdown::AllSignificant)
        .with_child(significant("quick", 20, RestartPolicy::Temporary))
        .with_child(significant("slow", 120, RestartPolicy::Transient))
        .with_worker("service", Service, RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec).expect("valid spec");

    tokio::time::sleep(Duration::from_millis(70)).await;
    assert!(
        !handle.is_stopped(),
        "one significant child is still running"
    );

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(handle.is_stopped(), "the last significant child ended");
}

#[tokio::test]
async fn a_transient_significant_child_that_fails_is_restarted_instead() {
    struct FailsOnce(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl Worker for FailsOnce {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                return Err(std::io::Error::other("transient failure"));
            }
            std::future::pending().await
        }
    }

    let starts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = std::sync::Arc::clone(&starts);
    let spec = SupervisorSpec::new("restarted")
        .with_restart_backoff(ash_flare::RestartBackoff::none())
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        .with_child(
            ChildSpec::new("job", move || FailsOnce(std::sync::Arc::clone(&counted)))
                .restart(RestartPolicy::Transient)
                .significant(true),
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !handle.is_stopped(),
        "an abnormal exit is restarted, not an ending"
    );
    assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 2);
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminating_a_significant_child_does_not_shut_down() {
    let spec = SupervisorSpec::new("terminated")
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        .with_child(significant("job", 10_000, RestartPolicy::Transient));
    let handle = SupervisorHandle::start(spec).expect("valid spec");

    handle.terminate_child("job").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handle.is_stopped(),
        "only a child ending on its own counts"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_significant_children_are_rejected() {
    let permanent = SupervisorSpec::new("permanent")
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        .with_child(significant("job", 10, RestartPolicy::Permanent));
    assert!(matches!(
        SupervisorHandle::start(permanent),
        Err(SupervisorError::InvalidChildSpec { child_id, .. }) if child_id == "job"
    ));

    let never =
        SupervisorSpec::new("never").with_child(significant("job", 10, RestartPolicy::Transient));
    assert!(matches!(
        SupervisorHandle::start(never),
        Err(SupervisorError::InvalidChildSpec { .. })
    ));
}

/// A subtree that finishes its work stops itself; as a `Transient` child its
/// parent treats that as a normal end, removes it and carries on.
#[tokio::test]
async fn a_nested_supervisor_that_shuts_itself_down_is_not_restarted() {
    let subtree = SupervisorSpec::new("batch")
        .with_restart_policy(RestartPolicy::Transient)
        .with_auto_shutdown(AutoShutdown::AllSignificant)
        .with_child(significant("step", 30, RestartPolicy::Transient));
    let spec = SupervisorSpec::new("root")
        .with_worker("service", Service, RestartPolicy::Permanent)
        .with_supervisor(subtree);
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    tokio::time::sleep(Duration::from_millis(120)).await;
    let mut subtree_exit = None;
    while let Ok(event) = events.try_recv() {
        if let SupervisorEvent::ChildTerminated { id, reason } = event
            && &*id == "batch"
        {
            subtree_exit = Some(reason);
        }
    }
    assert_eq!(
        subtree_exit,
        Some(ChildExitReason::Normal),
        "a subtree that stopped itself ended normally"
    );
    let children = handle.which_children().await.expect("root still running");
    assert_eq!(
        children.len(),
        1,
        "the finished subtree was removed: {children:?}"
    );
    assert_eq!(children[0].child_type, ChildType::Worker);
    handle.shutdown().await.unwrap();
}
