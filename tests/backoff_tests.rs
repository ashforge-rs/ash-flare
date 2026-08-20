//! Regression tests for restart backoff and supervisor termination on
//! restart-intensity exhaustion.

use ash_flare::{
    RestartBackoff, RestartIntensity, RestartPolicy, SupervisorHandle, SupervisorSpec, Worker,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Worker that fails immediately, counting each attempt.
struct CrashLoop {
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl Worker for CrashLoop {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other("boom"))
    }
}

fn crash_loop_spec(attempts: &Arc<AtomicUsize>) -> SupervisorSpec<CrashLoop> {
    let counter = Arc::clone(attempts);
    SupervisorSpec::new("root").with_worker(
        "crasher",
        move || CrashLoop {
            attempts: Arc::clone(&counter),
        },
        RestartPolicy::Permanent,
    )
}

#[tokio::test]
async fn backoff_prevents_restart_busy_loop() {
    let attempts = Arc::new(AtomicUsize::new(0));

    // Effectively unlimited intensity so the backoff, not the limit, bounds us.
    let spec = crash_loop_spec(&attempts)
        .with_restart_intensity(RestartIntensity::new(1_000_000, 600))
        .with_restart_backoff(RestartBackoff::exponential(
            Duration::from_millis(50),
            Duration::from_secs(5),
        ));

    let _handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = attempts.load(Ordering::SeqCst);
    // With 50ms doubling backoff only a handful of restarts fit in 500ms.
    // Without backoff this was ~35,000.
    assert!(
        count < 20,
        "expected backoff to throttle restarts, but saw {count} attempts in 500ms"
    );
    assert!(count > 0, "worker should have run at least once");
}

#[tokio::test]
async fn default_backoff_is_not_immediate() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let spec =
        crash_loop_spec(&attempts).with_restart_intensity(RestartIntensity::new(1_000_000, 600));

    let _handle = SupervisorHandle::start(spec);
    tokio::time::sleep(Duration::from_millis(400)).await;

    let count = attempts.load(Ordering::SeqCst);
    assert!(
        count < 20,
        "default backoff should throttle restarts, saw {count} in 400ms"
    );
}

#[tokio::test]
async fn supervisor_terminates_when_restart_intensity_exceeded() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let spec = crash_loop_spec(&attempts)
        .with_restart_intensity(RestartIntensity::new(2, 60))
        .with_restart_backoff(RestartBackoff::none());

    let handle = SupervisorHandle::start(spec);

    // Give the supervisor time to blow through its intensity limit.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The supervisor task must have exited, closing its command channel, so
    // every operation now reports ShuttingDown rather than succeeding.
    assert!(
        handle.which_children().await.is_err(),
        "supervisor should be terminated after exceeding restart intensity"
    );
    assert!(
        handle
            .start_child(
                "new",
                || CrashLoop {
                    attempts: Arc::new(AtomicUsize::new(0))
                },
                RestartPolicy::Temporary
            )
            .await
            .is_err(),
        "a terminated supervisor must not accept new children"
    );
    assert!(
        handle.uptime().await.is_err(),
        "a terminated supervisor must not answer queries"
    );
}

#[tokio::test]
async fn backoff_delay_is_exponential_and_capped() {
    let backoff =
        RestartBackoff::exponential(Duration::from_millis(100), Duration::from_millis(500));

    assert_eq!(backoff.delay_for_attempt(0), Duration::from_millis(100));
    assert_eq!(backoff.delay_for_attempt(1), Duration::from_millis(200));
    assert_eq!(backoff.delay_for_attempt(2), Duration::from_millis(400));
    // Capped at max_delay.
    assert_eq!(backoff.delay_for_attempt(3), Duration::from_millis(500));
    assert_eq!(backoff.delay_for_attempt(64), Duration::from_millis(500));
    // Very large attempt counts must saturate, not overflow.
    assert_eq!(
        backoff.delay_for_attempt(u32::MAX),
        Duration::from_millis(500)
    );
}

#[tokio::test]
async fn backoff_none_disables_delay() {
    let backoff = RestartBackoff::none();
    assert_eq!(backoff.delay_for_attempt(0), Duration::ZERO);
    assert_eq!(backoff.delay_for_attempt(10), Duration::ZERO);
}
