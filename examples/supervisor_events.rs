//! Watching a supervisor through its event stream.
//!
//! This example demonstrates:
//! - `SupervisorHandle::subscribe` and the `SupervisorEvent`s it delivers:
//!   `ChildStarted`, `ChildTerminated`, `ChildRestarted`,
//!   `RestartIntensityExceeded` and `Stopped`
//! - Panics being supervised like errors: a panicking worker is reported as an
//!   `Abnormal` exit and restarted
//! - A supervisor giving up once its restart intensity is exceeded, and
//!   `SupervisorHandle::is_stopped` afterwards
//!
//! A flaky sensor alternately returns an error and panics. The supervisor
//! allows 3 restarts within 5 seconds; the fourth failure makes it give up.
//!
//! Run with: `cargo run --example supervisor_events`

use ash_flare::{
    RestartIntensity, RestartPolicy, ShutdownSignal, SupervisorEvent, SupervisorHandle,
    SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Fails shortly after every start, alternating between an error and a panic.
struct FlakySensor {
    attempt: u32,
}

impl Worker for FlakySensor {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(150)).await;
        if self.attempt % 2 == 1 {
            println!(
                "           (error: read timeout on attempt {})",
                self.attempt
            );
            Err(format!("read timeout on attempt {}", self.attempt))
        } else {
            panic!("corrupt reading on attempt {}", self.attempt);
        }
    }
}

/// A well-behaved worker, stopped when its supervisor gives up.
struct Logger {
    shutdown: ShutdownSignal,
}

impl Worker for Logger {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }
}

/// The message a panic was raised with.
fn panic_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Supervisor events ===\n");

    // Print panics on one line instead of the default multi-line report.
    std::panic::set_hook(Box::new(|info| {
        println!("           (panic: {})", panic_message(info));
    }));

    let spec = SupervisorSpec::new("sensors")
        .with_restart_intensity(RestartIntensity::new(3, 5))
        .with_restart_delay(Duration::from_millis(100))
        .with_worker(
            "logger",
            |shutdown| Logger { shutdown },
            RestartPolicy::Permanent,
        );
    let supervisor = SupervisorHandle::start(spec)?;

    // Only events after `subscribe` are received, so subscribe before
    // starting the child we want to watch.
    let mut events = supervisor.subscribe();
    let attempts = Arc::new(AtomicU32::new(0));
    supervisor
        .start_child(
            "sensor",
            move || FlakySensor {
                attempt: attempts.fetch_add(1, Ordering::Relaxed) + 1,
            },
            RestartPolicy::Permanent,
        )
        .await?;
    let t0 = Instant::now();

    while let Ok(event) = events.recv().await {
        let at = t0.elapsed().as_millis();
        match event {
            SupervisorEvent::ChildStarted { id } => println!("{at:>5}ms  started     {id}"),
            SupervisorEvent::ChildRestarted { id } => println!("{at:>5}ms  restarted   {id}"),
            SupervisorEvent::ChildTerminated { id, reason } => {
                println!("{at:>5}ms  terminated  {id} ({reason:?})");
            }
            SupervisorEvent::RestartIntensityExceeded => {
                println!("{at:>5}ms  restart intensity exceeded: giving up");
            }
            SupervisorEvent::Stopped { reason } => {
                println!("{at:>5}ms  supervisor stopped ({reason:?})");
                break;
            }
            other => println!("{at:>5}ms  {other:?}"),
        }
    }

    println!("\nis_stopped() = {}", supervisor.is_stopped());
    Ok(())
}
