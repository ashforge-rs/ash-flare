use ash_flare::SupervisorError;

#[test]
fn test_supervisor_error_shutting_down() {
    let err = SupervisorError::ShuttingDown("service-supervisor".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("service-supervisor"));
    assert!(msg.contains("shutting down"));
    assert!(msg.contains("operation not permitted"));
}

#[test]
fn test_supervisor_error_child_already_exists() {
    let err = SupervisorError::ChildAlreadyExists("worker-1".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("worker-1"));
    assert!(msg.contains("already exists"));
    assert!(msg.contains("unique identifier"));
}

#[test]
fn test_supervisor_error_child_not_found() {
    let err = SupervisorError::ChildNotFound("worker-2".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("worker-2"));
    assert!(msg.contains("not found"));
    assert!(msg.contains("may have already terminated"));
}

#[test]
fn test_supervisor_error_debug() {
    let err = SupervisorError::ChildNotFound("test".to_string());
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("ChildNotFound"));
    assert!(debug_str.contains("test"));
}

#[test]
fn test_supervisor_error_is_error() {
    let err = SupervisorError::ShuttingDown("test".to_string());
    // Verify it implements std::error::Error
    let _err_ref: &dyn std::error::Error = &err;
}

#[test]
fn test_all_error_variants() {
    let errors = vec![
        SupervisorError::ShuttingDown("sup3".to_string()),
        SupervisorError::ChildAlreadyExists("child1".to_string()),
        SupervisorError::ChildNotFound("child2".to_string()),
    ];

    for err in errors {
        let msg = format!("{}", err);
        let debug = format!("{:?}", err);
        assert!(!msg.is_empty());
        assert!(!debug.is_empty());
    }
}

#[test]
fn test_error_display_formatting() {
    let err = SupervisorError::ShuttingDown("my-supervisor".to_string());
    let display = err.to_string();
    let formatted = format!("{}", err);
    assert_eq!(display, formatted);
}

#[test]
fn test_error_with_empty_name() {
    let err = SupervisorError::ChildNotFound("".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("''"));
}

#[test]
fn test_error_with_special_characters() {
    let err = SupervisorError::ChildNotFound("worker-123-!@#$".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("worker-123-!@#$"));
}

#[test]
fn test_error_source() {
    let err = SupervisorError::ShuttingDown("test".to_string());
    // std::error::Error::source() should return None for these errors
    assert!(std::error::Error::source(&err).is_none());
}
