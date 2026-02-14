use ash_flare::distributed::{
    ChildInfo, DistributedError, RemoteCommand, RemoteResponse, RemoteSupervisorHandle,
    SupervisorAddress, SupervisorStatus,
};
use ash_flare::{ChildInfo as SupervisorChildInfo, ChildType, RestartPolicy};

#[test]
fn test_supervisor_address_debug() {
    let tcp = SupervisorAddress::Tcp("127.0.0.1:8080".to_string());
    let debug_str = format!("{:?}", tcp);
    assert!(debug_str.contains("Tcp"));
    assert!(debug_str.contains("127.0.0.1:8080"));

    let unix = SupervisorAddress::Unix("/tmp/supervisor.sock".to_string());
    let debug_str = format!("{:?}", unix);
    assert!(debug_str.contains("Unix"));
}

#[test]
fn test_supervisor_address_clone() {
    let tcp = SupervisorAddress::Tcp("localhost:9000".to_string());
    let cloned = tcp.clone();
    if let SupervisorAddress::Tcp(addr) = cloned {
        assert_eq!(addr, "localhost:9000");
    } else {
        panic!("Expected Tcp variant");
    }
}

#[test]
fn test_remote_command_variants() {
    let shutdown = RemoteCommand::Shutdown;
    assert!(format!("{:?}", shutdown).contains("Shutdown"));

    let which = RemoteCommand::WhichChildren;
    assert!(format!("{:?}", which).contains("WhichChildren"));

    let status = RemoteCommand::Status;
    assert!(format!("{:?}", status).contains("Status"));

    let terminate = RemoteCommand::TerminateChild {
        id: "test-child".to_string(),
    };
    assert!(format!("{:?}", terminate).contains("TerminateChild"));
    assert!(format!("{:?}", terminate).contains("test-child"));
}

#[test]
fn test_remote_response_variants() {
    let ok = RemoteResponse::Ok;
    assert!(format!("{:?}", ok).contains("Ok"));

    let children = RemoteResponse::Children(vec![]);
    assert!(format!("{:?}", children).contains("Children"));

    let status = RemoteResponse::Status(SupervisorStatus {
        name: "test".to_string(),
        children_count: 5,
        restart_strategy: "OneForOne".to_string(),
        uptime_secs: 120,
    });
    assert!(format!("{:?}", status).contains("Status"));

    let error = RemoteResponse::Error("test error".to_string());
    assert!(format!("{:?}", error).contains("Error"));
    assert!(format!("{:?}", error).contains("test error"));
}

#[test]
fn test_child_info_creation() {
    let info = ChildInfo {
        id: "worker-1".to_string(),
        child_type: ChildType::Worker,
        restart_policy: Some(RestartPolicy::Permanent),
    };

    assert_eq!(info.id, "worker-1");
    assert_eq!(info.child_type, ChildType::Worker);
    assert_eq!(info.restart_policy, Some(RestartPolicy::Permanent));
}

#[test]
fn test_child_info_supervisor() {
    let info = ChildInfo {
        id: "supervisor-1".to_string(),
        child_type: ChildType::Supervisor,
        restart_policy: None,
    };

    assert_eq!(info.child_type, ChildType::Supervisor);
    assert!(info.restart_policy.is_none());
}

#[test]
fn test_child_info_debug() {
    let info = ChildInfo {
        id: "test".to_string(),
        child_type: ChildType::Worker,
        restart_policy: Some(RestartPolicy::Transient),
    };

    let debug_str = format!("{:?}", info);
    assert!(debug_str.contains("test"));
    assert!(debug_str.contains("Worker"));
}

#[test]
fn test_supervisor_status_creation() {
    let status = SupervisorStatus {
        name: "main-supervisor".to_string(),
        children_count: 10,
        restart_strategy: "OneForAll".to_string(),
        uptime_secs: 3600,
    };

    assert_eq!(status.name, "main-supervisor");
    assert_eq!(status.children_count, 10);
    assert_eq!(status.restart_strategy, "OneForAll");
    assert_eq!(status.uptime_secs, 3600);
}

#[test]
fn test_supervisor_status_debug() {
    let status = SupervisorStatus {
        name: "test-sup".to_string(),
        children_count: 5,
        restart_strategy: "RestForOne".to_string(),
        uptime_secs: 60,
    };

    let debug_str = format!("{:?}", status);
    assert!(debug_str.contains("test-sup"));
    assert!(debug_str.contains("5"));
}

#[test]
fn test_remote_command_clone() {
    let cmd = RemoteCommand::TerminateChild {
        id: "child-1".to_string(),
    };
    let cloned = cmd.clone();

    if let RemoteCommand::TerminateChild { id } = cloned {
        assert_eq!(id, "child-1");
    } else {
        panic!("Expected TerminateChild variant");
    }
}

#[test]
fn test_remote_response_clone() {
    let resp = RemoteResponse::Error("test".to_string());
    let cloned = resp.clone();

    if let RemoteResponse::Error(msg) = cloned {
        assert_eq!(msg, "test");
    } else {
        panic!("Expected Error variant");
    }
}

#[test]
fn test_child_info_clone() {
    let info = ChildInfo {
        id: "worker".to_string(),
        child_type: ChildType::Worker,
        restart_policy: Some(RestartPolicy::Temporary),
    };
    let cloned = info.clone();

    assert_eq!(cloned.id, "worker");
    assert_eq!(cloned.child_type, ChildType::Worker);
    assert_eq!(cloned.restart_policy, Some(RestartPolicy::Temporary));
}

#[test]
fn test_supervisor_status_clone() {
    let status = SupervisorStatus {
        name: "sup".to_string(),
        children_count: 3,
        restart_strategy: "OneForOne".to_string(),
        uptime_secs: 100,
    };
    let cloned = status.clone();

    assert_eq!(cloned.name, "sup");
    assert_eq!(cloned.children_count, 3);
}

// ============================================================================
// DistributedError Tests
// ============================================================================

#[test]
fn test_distributed_error_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err = DistributedError::Io(io_err);
    let msg = format!("{}", err);
    assert!(msg.contains("IO error"));
    assert!(msg.contains("file not found"));
}

#[test]
fn test_distributed_error_remote() {
    let err = DistributedError::RemoteError("supervisor not found".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("Remote error"));
    assert!(msg.contains("supervisor not found"));
}

#[test]
fn test_distributed_error_unexpected_response() {
    let err = DistributedError::UnexpectedResponse;
    let msg = format!("{}", err);
    assert!(msg.contains("Unexpected response"));
}

#[test]
fn test_distributed_error_message_too_large() {
    let err = DistributedError::MessageTooLarge(10_000_001);
    let msg = format!("{}", err);
    assert!(msg.contains("Message too large"));
    assert!(msg.contains("10000001"));
}

#[test]
fn test_distributed_error_debug() {
    let err = DistributedError::UnexpectedResponse;
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("UnexpectedResponse"));
}

#[test]
fn test_distributed_error_is_error() {
    let err = DistributedError::UnexpectedResponse;
    let _err_ref: &dyn std::error::Error = &err;
}

#[test]
fn test_distributed_error_from_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test");
    let err: DistributedError = io_err.into();
    assert!(matches!(err, DistributedError::Io(_)));
}

// ============================================================================
// ChildInfo From Implementation Tests
// ============================================================================

#[test]
fn test_child_info_from_supervisor_child_info() {
    let supervisor_info = SupervisorChildInfo {
        id: "worker-test".to_string(),
        child_type: ChildType::Worker,
        restart_policy: Some(RestartPolicy::Permanent),
    };

    let child_info: ChildInfo = supervisor_info.into();
    assert_eq!(child_info.id, "worker-test");
    assert_eq!(child_info.child_type, ChildType::Worker);
    assert_eq!(child_info.restart_policy, Some(RestartPolicy::Permanent));
}

#[test]
fn test_child_info_from_supervisor_info_supervisor_type() {
    let supervisor_info = SupervisorChildInfo {
        id: "sup-test".to_string(),
        child_type: ChildType::Supervisor,
        restart_policy: None,
    };

    let child_info: ChildInfo = supervisor_info.into();
    assert_eq!(child_info.id, "sup-test");
    assert_eq!(child_info.child_type, ChildType::Supervisor);
    assert!(child_info.restart_policy.is_none());
}

// ============================================================================
// RemoteSupervisorHandle Tests
// ============================================================================

#[tokio::test]
async fn test_remote_supervisor_handle_new() {
    let addr = SupervisorAddress::Tcp("localhost:8080".to_string());
    let handle = RemoteSupervisorHandle::new(addr);
    // Just verify construction works
    let _ = handle;
}

#[tokio::test]
async fn test_remote_supervisor_handle_connect_tcp() {
    let result = RemoteSupervisorHandle::connect_tcp("localhost:9999").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_remote_supervisor_handle_connect_unix() {
    let result = RemoteSupervisorHandle::connect_unix("/tmp/test.sock").await;
    assert!(result.is_ok());
}

// ============================================================================
// Serialization Tests (require rkyv/serde traits to work)
// ============================================================================

#[test]
fn test_supervisor_address_serialize() {
    let addr = SupervisorAddress::Tcp("test:8080".to_string());
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&addr);
    assert!(encoded.is_ok());

    if let Ok(bytes) = encoded {
        let decoded: Result<SupervisorAddress, _> =
            unsafe { rkyv::from_bytes_unchecked::<SupervisorAddress, rkyv::rancor::Error>(&bytes) };
        assert!(decoded.is_ok());
    }
}

#[test]
fn test_remote_command_serialize() {
    let cmd = RemoteCommand::WhichChildren;
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&cmd);
    assert!(encoded.is_ok());

    if let Ok(bytes) = encoded {
        let decoded: Result<RemoteCommand, _> =
            unsafe { rkyv::from_bytes_unchecked::<RemoteCommand, rkyv::rancor::Error>(&bytes) };
        assert!(decoded.is_ok());
    }
}

#[test]
fn test_remote_response_serialize() {
    let resp = RemoteResponse::Ok;
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&resp);
    assert!(encoded.is_ok());

    if let Ok(bytes) = encoded {
        let decoded: Result<RemoteResponse, _> =
            unsafe { rkyv::from_bytes_unchecked::<RemoteResponse, rkyv::rancor::Error>(&bytes) };
        assert!(decoded.is_ok());
    }
}

#[test]
fn test_child_info_serialize() {
    let info = ChildInfo {
        id: "test".to_string(),
        child_type: ChildType::Worker,
        restart_policy: Some(RestartPolicy::Permanent),
    };
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&info);
    assert!(encoded.is_ok());

    if let Ok(bytes) = encoded {
        let decoded: Result<ChildInfo, _> =
            unsafe { rkyv::from_bytes_unchecked::<ChildInfo, rkyv::rancor::Error>(&bytes) };
        assert!(decoded.is_ok());
    }
}

#[test]
fn test_supervisor_status_serialize() {
    let status = SupervisorStatus {
        name: "test".to_string(),
        children_count: 5,
        restart_strategy: "OneForOne".to_string(),
        uptime_secs: 100,
    };
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&status);
    assert!(encoded.is_ok());

    if let Ok(bytes) = encoded {
        let decoded: Result<SupervisorStatus, _> =
            unsafe { rkyv::from_bytes_unchecked::<SupervisorStatus, rkyv::rancor::Error>(&bytes) };
        assert!(decoded.is_ok());
    }
}
