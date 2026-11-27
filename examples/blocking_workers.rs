//! Workers that block their thread, found with stall detection and moved out
//! of the way with isolation.
//!
//! This example demonstrates:
//! - Why blocking matters: async Rust cannot preempt a task, so a worker that
//!   calls a blocking API holds up every other task on its runtime thread
//! - `SupervisorSpec::with_stall_detection`, which reports such a worker with
//!   `SupervisorEvent::ChildStalled` while it is still stuck
//! - `ChildSpec::isolated`, which runs a worker on a thread of its own so it
//!   only ever holds up itself
//!
//! A heartbeat worker ticks every 50ms and records how late its ticks are. Next
//! to it runs a "legacy client" that makes a blocking 300ms call in a loop. The
//! runtime has a single thread, so the effect shows on any machine; with more
//! threads it is the same for whatever shares the blocked thread.
//!
//! Run with: `cargo run --example blocking_workers`

use ash_flare::{ChildSpec, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

const TICK: Duration = Duration::from_millis(50);

/// Ticks every 50ms and records the worst lateness seen, in milliseconds.
struct Heartbeat {
    worst_lateness_ms: Arc<AtomicU64>,
}

impl Worker for Heartbeat {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut next = Instant::now() + TICK;
        loop {
            tokio::time::sleep_until(next.into()).await;
            let late = Instant::now().saturating_duration_since(next);
            self.worst_lateness_ms
                .fetch_max(late.as_millis() as u64, Ordering::Relaxed);
            next += TICK;
        }
    }
}

/// Calls a blocking API in a loop, as a synchronous client library would.
struct LegacyClient;

impl Worker for LegacyClient {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            // Stands in for a synchronous database driver or a heavy computation.
            std::thread::sleep(Duration::from_millis(300));
            sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn run_scenario(isolated: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "--- legacy client {} ---",
        if isolated {
            "isolated on its own thread"
        } else {
            "on the shared runtime"
        }
    );

    let worst = Arc::new(AtomicU64::new(0));
    let heartbeat_worst = Arc::clone(&worst);
    let spec = SupervisorSpec::new("app")
        .with_stall_detection(Duration::from_millis(100))
        .with_child(ChildSpec::new("heartbeat", move || Heartbeat {
            worst_lateness_ms: Arc::clone(&heartbeat_worst),
        }))
        .with_child(ChildSpec::new("legacy-client", || LegacyClient).isolated(isolated));

    let supervisor = SupervisorHandle::start(spec)?;
    let mut events = supervisor.subscribe();
    let stalls = tokio::spawn(async move {
        let mut reported = 0;
        while let Ok(event) = events.recv().await {
            match event {
                SupervisorEvent::ChildStalled { id, stalled_for } => {
                    reported += 1;
                    if reported <= 3 {
                        println!("  stall reported: {id} stuck in one poll for {stalled_for:?}");
                    }
                }
                SupervisorEvent::Stopped { .. } => break,
                _ => {}
            }
        }
        reported
    });

    sleep(Duration::from_millis(1_500)).await;
    supervisor.shutdown().await?;
    let reported = stalls.await?;

    println!("  stall reports in 1.5s: {reported}");
    println!(
        "  worst heartbeat lateness: {}ms\n",
        worst.load(Ordering::Relaxed)
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Blocking workers ===\n");

    // The blocking call holds the only runtime thread: the heartbeat's ticks
    // run up to ~300ms late, and stall detection points at the culprit.
    run_scenario(false).await?;

    // Same worker, own thread: the heartbeat stays on time. Stall detection
    // still notices the blocking poll, but nothing else is waiting on it.
    run_scenario(true).await?;

    Ok(())
}
