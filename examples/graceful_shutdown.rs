//! Per-child shutdown settings and the order children are stopped in.
//!
//! This example demonstrates:
//! - `ChildSpec` with `Shutdown::Infinity`, `Shutdown::Timeout` and
//!   `Shutdown::BrutalKill`
//! - `ChildExitReason::Killed` for a child that was aborted, and whose
//!   `shutdown` hook therefore did not finish (or did not run at all)
//! - `ShutdownOrder::Reverse` (the default: last started, first stopped) and
//!   `ShutdownOrder::Concurrent` (all at once)
//! - `SupervisorHandle::shutdown` waiting until the whole tree has stopped
//!
//! The tree is a database writer that must flush its buffer, an HTTP front end
//! that drains connections slowly, and a metrics scraper whose cleanup does not
//! matter. It is shut down twice, once in each order.
//!
//! Run with: `cargo run --example graceful_shutdown`

use ash_flare::{
    ChildSpec, Shutdown, ShutdownOrder, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// A worker that runs until stopped, then takes `cleanup` to clean up.
struct Service {
    name: &'static str,
    cleanup: Duration,
    started: Instant,
}

impl Worker for Service {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Dropped at this await point when the supervisor asks it to stop.
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!(
            "  {:>5}ms  {:<9} cleanup started ({:?} needed)",
            self.started.elapsed().as_millis(),
            self.name,
            self.cleanup
        );
        sleep(self.cleanup).await;
        println!(
            "  {:>5}ms  {:<9} cleanup finished",
            self.started.elapsed().as_millis(),
            self.name
        );
        Ok(())
    }
}

fn service(
    name: &'static str,
    cleanup: Duration,
    shutdown: Shutdown,
    started: Instant,
) -> ChildSpec {
    ChildSpec::new(name, move || Service {
        name,
        cleanup,
        started,
    })
    .shutdown(shutdown)
}

async fn run_tree(order: ShutdownOrder) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- {order:?} ---");
    let t0 = Instant::now();
    let ms = Duration::from_millis;

    let spec = SupervisorSpec::new("app")
        .with_shutdown_order(order)
        // Started in this order.
        .with_child(service("db-writer", ms(600), Shutdown::Infinity, t0))
        .with_child(service("http", ms(2_000), Shutdown::Timeout(ms(300)), t0))
        .with_child(service("metrics", ms(1_000), Shutdown::BrutalKill, t0));

    let supervisor = SupervisorHandle::start(spec)?;
    let mut events = supervisor.subscribe();
    // Collects why each child ended, to print once the tree is down.
    let watcher = tokio::spawn(async move {
        let mut exits = Vec::new();
        while let Ok(event) = events.recv().await {
            match event {
                SupervisorEvent::ChildTerminated { id, reason } => exits.push((id, reason)),
                SupervisorEvent::Stopped { .. } => break,
                _ => {}
            }
        }
        exits
    });

    sleep(ms(100)).await;
    let asked = t0.elapsed();
    println!("  {:>5}ms  shutdown requested", asked.as_millis());
    // Returns only once every child has stopped.
    supervisor.shutdown().await?;
    println!(
        "  {:>5}ms  shutdown returned, after {}ms",
        t0.elapsed().as_millis(),
        (t0.elapsed() - asked).as_millis()
    );
    for (id, reason) in watcher.await? {
        println!("           {id:<9} exited: {reason:?}");
    }
    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Graceful shutdown ===\n");
    println!("db-writer  Shutdown::Infinity        cleanup 600ms  -> always finishes");
    println!("http       Shutdown::Timeout(300ms)  cleanup 2s     -> aborted after 300ms");
    println!("metrics    Shutdown::BrutalKill      cleanup 1s     -> aborted at once\n");

    // Last started, first stopped: metrics, then http, then db-writer, so the
    // writer outlives everything that might still send it work.
    run_tree(ShutdownOrder::Reverse).await?;

    // All at once: as slow as the slowest child, but no ordering guarantees.
    run_tree(ShutdownOrder::Concurrent).await?;

    Ok(())
}
