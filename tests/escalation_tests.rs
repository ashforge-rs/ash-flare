//! The top of the tree and the way down it: waiting for a supervisor to stop,
//! what a top-level supervisor does when it gives up, and reaching nested
//! supervisors by handle or registered name.

use ash_flare::{
    ChildExitReason, GiveUp, Registry, RestartBackoff, RestartIntensity, RestartPolicy,
    ShutdownSignal, SupervisorError, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Waits until it is told to stop.
struct Idle(ShutdownSignal);
impl Worker for Idle {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// Fails at once on its first `failures` starts, then idles.
struct FailsAtFirst {
    starts: Arc<AtomicUsize>,
    failures: usize,
    shutdown: ShutdownSignal,
}
impl Worker for FailsAtFirst {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        if self.starts.fetch_add(1, Ordering::SeqCst) < self.failures {
            return Err(std::io::Error::other("failing start"));
        }
        self.shutdown.cancelled().await;
        Ok(())
    }
}

fn fails_at_first(
    failures: usize,
) -> impl Fn(ShutdownSignal) -> FailsAtFirst + Send + Sync + 'static {
    let starts = Arc::new(AtomicUsize::new(0));
    move |shutdown| FailsAtFirst {
        starts: Arc::clone(&starts),
        failures,
        shutdown,
    }
}

/// A supervisor that gives up at the first failure of its always-failing child.
fn doomed(name: &str) -> SupervisorSpec {
    SupervisorSpec::new(name)
        .with_restart_intensity(RestartIntensity::new(0, 5))
        .with_worker(
            "broken",
            fails_at_first(usize::MAX),
            RestartPolicy::Permanent,
        )
}

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("finished in time")
}

// ---------------------------------------------------------------------------
// wait
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wait_reports_a_shutdown() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("app").with_worker(
        "idle",
        Idle,
        RestartPolicy::Permanent,
    ))
    .unwrap();
    let waiter = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.wait().await })
    };

    handle.shutdown().await.unwrap();
    assert_eq!(within(waiter).await.unwrap(), ChildExitReason::Shutdown);
    // Once stopped, waiting returns at once.
    assert_eq!(within(handle.wait()).await, ChildExitReason::Shutdown);
}

#[tokio::test]
async fn wait_reports_giving_up() {
    let handle = SupervisorHandle::start(doomed("doomed")).unwrap();
    assert_eq!(within(handle.wait()).await, ChildExitReason::Abnormal);
    assert!(handle.is_stopped());
}

// ---------------------------------------------------------------------------
// GiveUp
// ---------------------------------------------------------------------------

const EXIT_CHILD: &str = "ASH_FLARE_TEST_EXIT_ON_GIVE_UP";

/// Runs itself as a child process, whose top-level supervisor gives up.
#[test]
fn a_top_level_supervisor_can_exit_the_process_when_it_gives_up() {
    if std::env::var_os(EXIT_CHILD).is_some() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let _app = SupervisorHandle::start(doomed("app").with_give_up(GiveUp::ExitProcess(42)))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        // Reaching this means the process was not exited.
        std::process::exit(3);
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_top_level_supervisor_can_exit_the_process_when_it_gives_up",
        ])
        .env(EXIT_CHILD, "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(42));
}

#[tokio::test]
async fn a_shutdown_does_not_exit_the_process() {
    let handle = SupervisorHandle::start(
        SupervisorSpec::new("app")
            .with_give_up(GiveUp::ExitProcess(42))
            .with_worker("idle", Idle, RestartPolicy::Permanent),
    )
    .unwrap();
    handle.shutdown().await.unwrap();
    // Still running: only giving up exits.
    assert_eq!(handle.wait().await, ChildExitReason::Shutdown);
}

#[tokio::test]
async fn give_up_on_a_nested_supervisor_is_rejected() {
    let spec = SupervisorSpec::new("root")
        .with_supervisor(SupervisorSpec::new("nested").with_give_up(GiveUp::ExitProcess(1)));
    assert!(matches!(
        SupervisorHandle::start(spec),
        Err(SupervisorError::InvalidChildSpec { child_id, .. }) if child_id == "nested"
    ));
}

// ---------------------------------------------------------------------------
// Nested supervisors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn child_supervisor_returns_a_nested_supervisor() {
    let root = SupervisorHandle::start(
        SupervisorSpec::new("root")
            .with_worker("worker", Idle, RestartPolicy::Permanent)
            .with_supervisor(
                SupervisorSpec::new("rooms")
                    .with_restart_policy(RestartPolicy::Transient)
                    .with_worker("lobby", Idle, RestartPolicy::Permanent),
            ),
    )
    .unwrap();

    let policies: Vec<_> = root
        .which_children()
        .await
        .unwrap()
        .into_iter()
        .map(|child| (child.id, child.restart_policy))
        .collect();
    assert_eq!(
        policies,
        [
            ("worker".to_owned(), Some(RestartPolicy::Permanent)),
            ("rooms".to_owned(), Some(RestartPolicy::Transient)),
        ],
        "a nested supervisor reports its own restart policy"
    );

    let rooms = root.child_supervisor("rooms").await.unwrap();
    assert_eq!(rooms.name(), "rooms");
    rooms
        .start_child("room-1", Idle, RestartPolicy::Permanent)
        .await
        .unwrap();
    assert_eq!(rooms.which_children().await.unwrap().len(), 2);

    assert_eq!(
        root.child_supervisor("worker").await.unwrap_err(),
        SupervisorError::NotASupervisor("worker".to_owned())
    );
    assert_eq!(
        root.child_supervisor("missing").await.unwrap_err(),
        SupervisorError::ChildNotFound("missing".to_owned())
    );
    root.shutdown().await.unwrap();
}

/// Records whether the worker could find its own supervisor while starting.
struct FindsItsSupervisor {
    registry: Registry,
    found: Arc<AtomicBool>,
    shutdown: ShutdownSignal,
}
impl Worker for FindsItsSupervisor {
    type Error = std::io::Error;
    async fn initialize(&mut self) -> Result<(), Self::Error> {
        let found = self.registry.whereis::<SupervisorHandle>("jobs").is_some();
        self.found.store(found, Ordering::SeqCst);
        Ok(())
    }
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }
}

#[tokio::test]
async fn a_registered_name_is_set_before_the_children_start() {
    let registry = Registry::new();
    let found = Arc::new(AtomicBool::new(false));
    let (lookup, flag) = (registry.clone(), Arc::clone(&found));
    let root = SupervisorHandle::start(
        SupervisorSpec::new("root").with_supervisor(
            SupervisorSpec::new("jobs")
                .with_registered_name(&registry, "jobs")
                .with_worker(
                    "first",
                    move |shutdown| FindsItsSupervisor {
                        registry: lookup.clone(),
                        found: Arc::clone(&flag),
                        shutdown,
                    },
                    RestartPolicy::Permanent,
                ),
        ),
    )
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        found.load(Ordering::SeqCst),
        "the child found its supervisor"
    );
    root.shutdown().await.unwrap();
    assert!(
        !registry.is_registered("jobs"),
        "the name goes when the supervisor stops"
    );
}

#[tokio::test]
async fn a_registered_name_follows_the_supervisor_across_restarts() {
    let registry = Registry::new();
    let root = SupervisorHandle::start(
        SupervisorSpec::new("root")
            .with_restart_backoff(RestartBackoff::none())
            .with_supervisor(
                // Gives up at its child's first failure; the child fails only
                // on its first start, so the restarted subtree stays up.
                SupervisorSpec::new("jobs")
                    .with_registered_name(&registry, "jobs")
                    .with_restart_intensity(RestartIntensity::new(0, 5))
                    .with_worker("flaky", fails_at_first(1), RestartPolicy::Permanent),
            ),
    )
    .unwrap();
    // Registered while the root started its children.
    let first = registry
        .whereis::<SupervisorHandle>("jobs")
        .expect("registered at start");

    // The first run gives up; the root starts a second one, which registers.
    assert_eq!(within(first.wait()).await, ChildExitReason::Abnormal);
    let current = within(async {
        loop {
            if let Some(jobs) = registry.whereis::<SupervisorHandle>("jobs")
                && !jobs.is_stopped()
            {
                return jobs;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;

    assert!(first.is_stopped());
    assert_eq!(current.which_children().await.unwrap().len(), 1);
    let via_parent = root.child_supervisor("jobs").await.unwrap();
    assert!(!via_parent.is_stopped());
    root.shutdown().await.unwrap();
}
