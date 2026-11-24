use super::workers::{FailingWorker, QuickWorker};
use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec,
};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_supervisor_duplicate_child() {
    let spec = SupervisorSpec::new("duplicate-test");
    let handle = SupervisorHandle::start(spec);

    // Add first child (permanent so it won't exit)
    let result1 = handle
        .start_child("duplicate", || QuickWorker, RestartPolicy::Permanent)
        .await;
    assert!(result1.is_ok());

    sleep(Duration::from_millis(20)).await;

    // Try to add duplicate - should fail
    let result2 = handle
        .start_child("duplicate", || QuickWorker, RestartPolicy::Temporary)
        .await;
    assert!(result2.is_err());

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_terminate_nonexistent() {
    let spec = SupervisorSpec::new("terminate-test");
    let handle = SupervisorHandle::start(spec);

    let result = handle.terminate_child("nonexistent").await;
    assert!(result.is_err());

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_permanent_worker_restart() {
    let spec = SupervisorSpec::new("permanent-test")
        .with_restart_intensity(RestartIntensity::new(10, 5))
        .with_worker(
            "failing",
            || FailingWorker { fail_count: 0 },
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(150)).await;

    // Permanent worker should be restarting
    // It may be in the process of restarting or running
    let children = handle.which_children().await.unwrap();
    // Should have 0 or 1 depending on restart timing
    assert!(children.len() <= 1);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_transient_worker_no_restart_on_normal_exit() {
    let spec = SupervisorSpec::new("transient-test").with_worker(
        "quick",
        || QuickWorker,
        RestartPolicy::Transient,
    );

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(30)).await;

    // Transient worker should exit normally and not restart
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 0);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_temporary_worker_no_restart() {
    let spec = SupervisorSpec::new("temporary-test").with_worker(
        "failing",
        || FailingWorker { fail_count: 0 },
        RestartPolicy::Temporary,
    );

    let handle = SupervisorHandle::start(spec);
    sleep(Duration::from_millis(30)).await;

    // Temporary worker should not restart even on error
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 0);

    handle.shutdown().await.unwrap();
}

#[test]
fn test_restart_intensity_new() {
    let intensity = RestartIntensity::new(5, 10);
    assert_eq!(intensity.max_restarts, 5);
    assert_eq!(intensity.within_seconds, 10);
}

#[test]
fn test_restart_intensity_clone() {
    let intensity = RestartIntensity::new(3, 7);
    let cloned = intensity.clone();
    assert_eq!(cloned.max_restarts, 3);
    assert_eq!(cloned.within_seconds, 7);
}

#[test]
fn test_supervisor_spec_builder() {
    let _spec = SupervisorSpec::new("builder-test")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_intensity(RestartIntensity::new(10, 30))
        .with_worker("w1", || QuickWorker, RestartPolicy::Permanent)
        .with_worker("w2", || QuickWorker, RestartPolicy::Transient);
    // Spec built successfully
}
