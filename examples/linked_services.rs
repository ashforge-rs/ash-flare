//! Linking children of different supervisors, like Erlang's `link/1`.
//!
//! This example demonstrates:
//! - `link`, which makes two children share their fate even when they belong
//!   to different supervisors (what a `OneForAll` strategy cannot do)
//! - `SupervisorHandle::kill_child`, which kills a child as if it crashed
//!   (`ChildExitReason::Killed`), so its restart policy applies
//! - That a link works in both directions, follows children across restarts,
//!   and is removed by dropping the `Link`
//!
//! A `db` supervisor runs a connection pool; a `web` supervisor runs a cache
//! built on that pool and an API that does not depend on it. The cache is
//! linked to the pool. (A normal exit does not travel along a link; any other
//! termination does.)
//!
//! Run with: `cargo run --example linked_services`

use ash_flare::{
    RestartPolicy, ShutdownSignal, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker, link,
};
use std::time::Duration;
use tokio::time::sleep;

/// A long-running service that stops when asked.
struct Service {
    shutdown: ShutdownSignal,
}

impl Worker for Service {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }
}

fn service(shutdown: ShutdownSignal) -> Service {
    Service { shutdown }
}

/// Prints the events of one supervisor, prefixed with its name.
fn watch(supervisor: &SupervisorHandle) -> tokio::task::JoinHandle<()> {
    let name = supervisor.name().to_owned();
    let mut events = supervisor.subscribe();
    tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                SupervisorEvent::ChildTerminated { id, reason } => {
                    println!("  [{name}] {id} terminated ({reason:?})");
                }
                SupervisorEvent::ChildRestarted { id } => println!("  [{name}] {id} restarted"),
                SupervisorEvent::Stopped { .. } => break,
                _ => {}
            }
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Linked services ===\n");

    let restart_delay = Duration::from_millis(100);
    let db = SupervisorHandle::start(
        SupervisorSpec::new("db")
            .with_restart_delay(restart_delay)
            .with_worker("pool", service, RestartPolicy::Permanent),
    )?;
    let web = SupervisorHandle::start(
        SupervisorSpec::new("web")
            .with_restart_delay(restart_delay)
            .with_worker("cache", service, RestartPolicy::Permanent)
            .with_worker("api", service, RestartPolicy::Permanent),
    )?;
    let watchers = [watch(&db), watch(&web)];

    let pool_cache = link(&db, "pool", &web, "cache");
    println!(
        "linked db/pool <-> web/cache (active: {})\n",
        pool_cache.is_active()
    );

    println!("1. kill db/pool: the cache goes down with it, the api does not");
    db.kill_child("pool").await?;
    sleep(Duration::from_millis(500)).await;

    println!("\n2. kill web/cache: the link works both ways, and survived the restarts");
    web.kill_child("cache").await?;
    sleep(Duration::from_millis(500)).await;

    println!("\n3. drop the link, kill db/pool again: only the pool restarts");
    drop(pool_cache);
    db.kill_child("pool").await?;
    sleep(Duration::from_millis(500)).await;

    println!("\nshutting down");
    web.shutdown().await?;
    db.shutdown().await?;
    for watcher in watchers {
        watcher.await?;
    }
    Ok(())
}
