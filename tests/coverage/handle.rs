use super::workers::QuickWorker;
use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_supervisor_name() {
    let spec = SupervisorSpec::new("test-name");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    assert_eq!(handle.name(), "test-name");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_restart_strategy() {
    let spec =
        SupervisorSpec::new("strategy-test").with_restart_strategy(RestartStrategy::OneForAll);
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    let strategy = handle.restart_strategy().await.unwrap();
    assert_eq!(strategy, RestartStrategy::OneForAll);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_uptime() {
    let spec = SupervisorSpec::new("uptime-test");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    sleep(Duration::from_millis(100)).await;

    let uptime = handle.uptime().await.unwrap();
    assert!(uptime >= Duration::from_millis(100));

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_operations_after_shutdown() {
    let spec = SupervisorSpec::new("shutdown-test");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    handle.shutdown().await.unwrap();

    // Give it time to shut down
    sleep(Duration::from_millis(50)).await;

    // Operations after shutdown should fail
    let result = handle.which_children().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_start_child_after_shutdown() {
    let spec = SupervisorSpec::new("start-after-shutdown");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    handle.shutdown().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let result = handle
        .start_child("late-child", || QuickWorker, RestartPolicy::Temporary)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_terminate_child_after_shutdown() {
    let spec = SupervisorSpec::new("terminate-after-shutdown");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    handle.shutdown().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let result = handle.terminate_child("child").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_restart_strategy_after_shutdown() {
    let spec = SupervisorSpec::new("strategy-after-shutdown");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    handle.shutdown().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let result = handle.restart_strategy().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_uptime_after_shutdown() {
    let spec = SupervisorSpec::new("uptime-after-shutdown");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    handle.shutdown().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let result = handle.uptime().await;
    assert!(result.is_err());
}
