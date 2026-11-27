use ash_flare::WorkerContext;

#[tokio::test]
async fn test_worker_context_with_value() {
    let ctx = WorkerContext::new();
    ctx.set("test", serde_json::json!(42));

    let result = ctx.with_value("test", |v| v.and_then(|v| v.as_u64()));
    assert_eq!(result, Some(42));

    let result = ctx.with_value("missing", |v| v.and_then(|v| v.as_u64()));
    assert_eq!(result, None);
}

#[tokio::test]
async fn test_worker_context_update() {
    let ctx = WorkerContext::new();
    ctx.set("counter", serde_json::json!(0));

    ctx.update("counter", |v| {
        let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
        Some(serde_json::json!(count + 1))
    });

    let value = ctx.get("counter").and_then(|v| v.as_u64());
    assert_eq!(value, Some(1));
}

#[tokio::test]
async fn test_worker_context_delete() {
    let ctx = WorkerContext::new();
    ctx.set("temp", serde_json::json!("value"));

    assert!(ctx.contains_key("temp"));

    let deleted = ctx.delete("temp");
    assert!(deleted.is_some());

    assert!(!ctx.contains_key("temp"));
    assert!(ctx.get("temp").is_none());
}

#[tokio::test]
async fn test_worker_context_len_empty() {
    let ctx = WorkerContext::new();
    assert!(ctx.is_empty());
    assert_eq!(ctx.len(), 0);

    ctx.set("a", serde_json::json!(1));
    assert!(!ctx.is_empty());
    assert_eq!(ctx.len(), 1);

    ctx.set("b", serde_json::json!(2));
    assert_eq!(ctx.len(), 2);

    let _removed = ctx.delete("a");
    assert_eq!(ctx.len(), 1);
}

#[tokio::test]
async fn test_worker_context_contains_key() {
    let ctx = WorkerContext::new();

    assert!(!ctx.contains_key("missing"));

    ctx.set("exists", serde_json::json!("value"));
    assert!(ctx.contains_key("exists"));
    assert!(!ctx.contains_key("missing"));
}
