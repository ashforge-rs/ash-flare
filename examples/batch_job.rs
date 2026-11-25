//! Supervisors that stop themselves when their job is done.
//!
//! This example demonstrates:
//! - Significant children (`ChildSpec::significant`) and
//!   `SupervisorSpec::with_auto_shutdown`, as in OTP's `auto_shutdown`
//! - `AutoShutdown::AnySignificant` and `AutoShutdown::AllSignificant`
//! - `SupervisorSpec::with_restart_policy` on a nested supervisor, so its
//!   parent does not start it again once it has finished
//! - `SupervisorHandle::is_stopped`
//! - `SupervisorHandle::start` rejecting an invalid spec
//!
//! Run with: `cargo run --example batch_job`

use ash_flare::{
    AutoShutdown, ChildSpec, RestartPolicy, ShutdownSignal, SupervisorError, SupervisorEvent,
    SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::sleep;

/// Imports `batches` batches, then finishes normally.
struct Importer {
    name: &'static str,
    batches: u32,
    done: Arc<AtomicU32>,
}

impl Worker for Importer {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        for _ in 0..self.batches {
            sleep(Duration::from_millis(150)).await;
            self.done.fetch_add(1, Ordering::Relaxed);
        }
        println!("  [{}] finished", self.name);
        Ok(())
    }
}

/// Reports progress until it is stopped along with its supervisor.
struct Progress {
    done: Arc<AtomicU32>,
    total: u32,
    shutdown: ShutdownSignal,
}

impl Worker for Progress {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                () = sleep(Duration::from_millis(200)) => {
                    let done = self.done.load(Ordering::Relaxed);
                    println!("  [progress] {done}/{} batches", self.total);
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("  [progress] stopped with the job");
        Ok(())
    }
}

/// A long-running service next to the job.
struct Api(ShutdownSignal);

impl Worker for Api {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// A job supervisor nested in an application.
async fn nested_job() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- 1. a nested job with AnySignificant ---");
    let done = Arc::new(AtomicU32::new(0));
    let total = 5;
    let (importer_done, progress_done) = (Arc::clone(&done), Arc::clone(&done));

    let job = SupervisorSpec::new("import-job")
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        // A job that stopped on its own exits normally; Transient means the
        // parent leaves it stopped instead of running it again.
        .with_restart_policy(RestartPolicy::Transient)
        .with_child(
            ChildSpec::new("importer", move || Importer {
                name: "importer",
                batches: total,
                done: Arc::clone(&importer_done),
            })
            .restart(RestartPolicy::Transient)
            .significant(true),
        )
        .with_worker(
            "progress",
            move |shutdown| Progress {
                done: Arc::clone(&progress_done),
                total,
                shutdown,
            },
            RestartPolicy::Permanent,
        );

    let app = SupervisorHandle::start(
        SupervisorSpec::new("app")
            .with_worker("api", Api, RestartPolicy::Permanent)
            .with_supervisor(job),
    )?;
    let mut events = app.subscribe();

    while let Ok(event) = events.recv().await {
        if let SupervisorEvent::ChildTerminated { id, reason } = event {
            println!("  [app] child {id} terminated ({reason:?})");
            if &*id == "import-job" {
                // Stopping itself is a normal end, so a Transient job is
                // left stopped.
                break;
            }
        }
    }

    // Give the parent a moment to decide; a Transient job is not restarted.
    sleep(Duration::from_millis(300)).await;
    let children: Vec<_> = app
        .which_children()
        .await?
        .into_iter()
        .map(|child| child.id)
        .collect();
    println!("  [app] children now: {children:?}");
    app.shutdown().await?;
    Ok(())
}

/// A standalone job that stops once every significant child has finished.
async fn standalone_job() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- 2. a standalone job with AllSignificant ---");
    let done = Arc::new(AtomicU32::new(0));
    let extract = |name: &'static str, batches: u32| {
        let done = Arc::clone(&done);
        ChildSpec::new(name, move || Importer {
            name,
            batches,
            done: Arc::clone(&done),
        })
        .restart(RestartPolicy::Temporary)
        .significant(true)
    };

    let job = SupervisorHandle::start(
        SupervisorSpec::new("extract-job")
            .with_auto_shutdown(AutoShutdown::AllSignificant)
            .with_child(extract("extract-orders", 2))
            .with_child(extract("extract-users", 5)),
    )?;

    while !job.is_stopped() {
        sleep(Duration::from_millis(50)).await;
    }
    println!(
        "  [extract-job] stopped by itself after {} batches; is_stopped() = {}",
        done.load(Ordering::Relaxed),
        job.is_stopped()
    );
    Ok(())
}

/// A significant child must be able to finish, so a Permanent one is refused.
fn invalid_job() {
    println!("\n--- 3. an invalid job ---");
    let spec = SupervisorSpec::new("bad-job")
        .with_auto_shutdown(AutoShutdown::AnySignificant)
        .with_child(
            ChildSpec::new("forever", Api)
                .restart(RestartPolicy::Permanent)
                .significant(true),
        );
    match SupervisorHandle::start(spec) {
        Err(SupervisorError::InvalidChildSpec { reason, .. }) => {
            println!("  start refused the spec: {reason}");
        }
        Err(other) => println!("  start failed: {other}"),
        Ok(_) => println!("  unexpectedly started"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Batch jobs with auto shutdown ===\n");
    nested_job().await?;
    standalone_job().await?;
    invalid_job();
    Ok(())
}
