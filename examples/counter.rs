//! Minimal counter example showing different restart policies

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
struct CounterError(String);

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CounterError {}

struct Counter {
    name: String,
    max_count: Option<u32>,
}

impl Counter {
    fn new(name: impl Into<String>, max_count: Option<u32>) -> Self {
        Self {
            name: name.into(),
            max_count,
        }
    }
}
impl Worker for Counter {
    type Error = CounterError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut count = 0;
        loop {
            count += 1;
            println!("[{}] count: {}", self.name, count);

            if let Some(max) = self.max_count {
                if count >= max {
                    return Err(CounterError(format!("{} reached max", self.name)));
                }
            }

            sleep(Duration::from_millis(800)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[{}] shutdown", self.name);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("=== Counter Example: OneForOne Strategy ===\n");

    let spec = SupervisorSpec::new("counter_supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "stable",
            || Counter::new("stable", None),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "crashes",
            || Counter::new("crashes", Some(5)),
            RestartPolicy::Permanent, // Will restart when it crashes
        )
        .with_worker(
            "temporary",
            || Counter::new("temporary", Some(8)),
            RestartPolicy::Temporary, // Won't restart
        );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    sleep(Duration::from_secs(15)).await;

    println!("\nShutting down...");
    supervisor.shutdown().await.ok();
    sleep(Duration::from_millis(200)).await;
}
