use ash_flare::distributed::{
    RemoteCommand, RemoteResponse, RemoteSupervisorHandle, SupervisorAddress, SupervisorServer,
};
use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
use tokio::time::{Duration, sleep};

struct TestWorker;
impl Worker for TestWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[tokio::test]
async fn test_remote_supervisor_tcp() {
    let spec = SupervisorSpec::new("remote-test").with_worker(
        "worker-1",
        || TestWorker,
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec);
    let server = SupervisorServer::new(handle);

    // Start server in background
    tokio::spawn(async move {
        let _ = server.listen_tcp("127.0.0.1:9001").await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Connect to server
    let remote = RemoteSupervisorHandle::tcp("127.0.0.1:9001");

    // Test which_children command
    let children = remote.which_children().await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, "worker-1");

    // Test status command
    let status = remote.status().await.unwrap();
    assert_eq!(status.name, "remote-test");
    assert_eq!(status.children_count, 1);

    // Shutdown
    remote.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg(unix)]
async fn test_remote_supervisor_unix() {
    let socket_path = "/tmp/ash-flare-test.sock";
    let _ = std::fs::remove_file(socket_path);

    let spec = SupervisorSpec::new("unix-test")
        .with_worker("worker-1", || TestWorker, RestartPolicy::Permanent)
        .with_worker("worker-2", || TestWorker, RestartPolicy::Permanent);

    let handle = SupervisorHandle::start(spec);
    let server = SupervisorServer::new(handle);

    // Start server in background
    let socket_path_clone = socket_path.to_string();
    tokio::spawn(async move {
        let _ = server.listen_unix(&socket_path_clone).await;
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Connect to server
    let remote = RemoteSupervisorHandle::unix(socket_path);

    // Test which_children command
    let children = remote.which_children().await.unwrap();
    assert_eq!(children.len(), 2);

    // Test terminate_child command
    remote.terminate_child("worker-1").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let children = remote.which_children().await.unwrap();
    assert_eq!(children.len(), 1);

    // Cleanup
    remote.shutdown().await.unwrap();
    let _ = std::fs::remove_file(socket_path);
}

#[tokio::test]
async fn test_supervisor_address_types() {
    let tcp_addr = SupervisorAddress::Tcp("127.0.0.1:8080".to_string());
    let unix_addr = SupervisorAddress::Unix("/tmp/test.sock".to_string());

    match tcp_addr {
        SupervisorAddress::Tcp(addr) => assert_eq!(addr, "127.0.0.1:8080"),
        _ => panic!("Expected TCP address"),
    }

    match unix_addr {
        SupervisorAddress::Unix(path) => assert_eq!(path, "/tmp/test.sock"),
        _ => panic!("Expected Unix address"),
    }
}

#[tokio::test]
async fn test_remote_commands() {
    let cmd_shutdown = RemoteCommand::Shutdown;
    let cmd_which_children = RemoteCommand::WhichChildren;
    let cmd_terminate = RemoteCommand::TerminateChild {
        id: "test-worker".to_string(),
    };
    let cmd_status = RemoteCommand::Status;

    // Verify enum variants exist
    assert!(matches!(cmd_shutdown, RemoteCommand::Shutdown));
    assert!(matches!(cmd_which_children, RemoteCommand::WhichChildren));
    assert!(matches!(
        cmd_terminate,
        RemoteCommand::TerminateChild { .. }
    ));
    assert!(matches!(cmd_status, RemoteCommand::Status));
}

#[tokio::test]
async fn test_remote_responses() {
    let resp_ok = RemoteResponse::Ok;
    let resp_error = RemoteResponse::Error("test error".to_string());

    assert!(matches!(resp_ok, RemoteResponse::Ok));
    assert!(matches!(resp_error, RemoteResponse::Error(_)));

    if let RemoteResponse::Error(msg) = resp_error {
        assert_eq!(msg, "test error");
    }
}

#[tokio::test]
async fn test_remote_handle_connection() {
    let spec = SupervisorSpec::new("connection-test").with_worker(
        "worker-1",
        || TestWorker,
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec);
    let server = SupervisorServer::new(handle);

    tokio::spawn(async move {
        let _ = server.listen_tcp("127.0.0.1:9002").await;
    });

    sleep(Duration::from_millis(100)).await;

    let remote = RemoteSupervisorHandle::tcp("127.0.0.1:9002");

    // Multiple commands to same connection
    let children1 = remote.which_children().await.unwrap();
    let status = remote.status().await.unwrap();
    let children2 = remote.which_children().await.unwrap();

    assert_eq!(children1.len(), children2.len());
    assert_eq!(status.name, "connection-test");

    remote.shutdown().await.unwrap();
}
