//! Minimal example of workers sharing state through a `WorkerContext`.
//!
//! The context is cheap to clone and every clone sees the same store, so
//! sharing it is just a matter of capturing a clone in each worker's factory.
use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker, WorkerContext};

struct SimpleWorker {
    id: u32,
    ctx: WorkerContext,
}

impl Worker for SimpleWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let key = format!("worker-{}", self.id);
        self.ctx.set(key, serde_json::json!(self.id));
        Ok(())
    }
}

/// Builds a factory that hands each new worker a clone of the shared context.
fn simple_worker(
    id: u32,
    ctx: &WorkerContext,
) -> impl Fn() -> SimpleWorker + Send + Sync + 'static {
    let ctx = ctx.clone();
    move || SimpleWorker {
        id,
        ctx: ctx.clone(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = WorkerContext::new();

    let spec = SupervisorSpec::new("auction-supervisor")
        .with_worker(
            "auction-worker-1",
            simple_worker(1, &store),
            RestartPolicy::Temporary,
        )
        .with_worker(
            "auction-worker-2",
            simple_worker(2, &store),
            RestartPolicy::Temporary,
        );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    // Give workers time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Access shared store
    println!("Worker 1 value: {:?}", store.get("worker-1"));
    println!("Worker 2 value: {:?}", store.get("worker-2"));

    // Demonstrate KV operations
    store.set("bid-count", serde_json::json!(0));
    store.update("bid-count", |v| {
        let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
        Some(serde_json::json!(count + 1))
    });
    println!("Bid count: {:?}", store.get("bid-count"));

    handle.shutdown().await?;
    Ok(())
}
