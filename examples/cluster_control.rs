//! Multi-region cluster control: one client watching several remote
//! supervisors.
//!
//! Each region is a supervisor of devices served on its own Unix socket and
//! protected by a cookie, like an Erlang node; the control client connects to
//! every region with the same cookie and polls their children.
//!
//! Start the regions, each in its own terminal:
//!     cargo run --example cluster_control -- region us-west
//!     cargo run --example cluster_control -- region eu-west
//!
//! Then the control client (`python_examples/cluster_control.py` works too):
//!     cargo run --example cluster_control
//!
//! The cookie is read from `ASH_FLARE_COOKIE`, with a fixed fallback for the
//! demo. Note: This example requires Unix sockets.

#[cfg(unix)]
use ash_flare::distributed::{RemoteSupervisorHandle, SupervisorServer};
#[cfg(unix)]
use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker};
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use tokio::time::sleep;

#[cfg(unix)]
const REGIONS: [&str; 2] = ["us-west", "eu-west"];

#[cfg(unix)]
fn cookie() -> String {
    std::env::var("ASH_FLARE_COOKIE").unwrap_or_else(|_| "ash-flare-demo-cookie".to_owned())
}

#[cfg(unix)]
fn socket_path(region: &str) -> String {
    format!("/tmp/supervisor-{region}.sock")
}

/// A device that stays online until stopped.
#[cfg(unix)]
struct Device(ShutdownSignal);

#[cfg(unix)]
impl Worker for Device {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// Serves one region's supervisor until it is shut down remotely.
#[cfg(unix)]
async fn run_region(region: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut spec = SupervisorSpec::new(region);
    for device in ["gateway", "thermostat-1", "thermostat-2", "camera-1"] {
        let policy = if device == "gateway" {
            RestartPolicy::Permanent
        } else {
            RestartPolicy::Transient
        };
        spec = spec.with_worker(format!("{region}-{device}"), Device, policy);
    }
    let supervisor = SupervisorHandle::start(spec)?;

    let path = socket_path(region);
    let server = SupervisorServer::new(supervisor.clone()).with_cookie(cookie());
    println!("[{region}] serving on {path}");
    tokio::select! {
        result = server.listen_unix(&path) => result?,
        () = async {
            while !supervisor.is_stopped() {
                sleep(Duration::from_millis(200)).await;
            }
        } => println!("[{region}] shut down remotely"),
    }
    Ok(())
}

/// Polls every region and prints its devices.
#[cfg(unix)]
async fn run_control() {
    println!("=== IoT Fleet Management - Cluster Control ===\n");

    let regions: Vec<_> = REGIONS
        .iter()
        .map(|region| {
            let handle = RemoteSupervisorHandle::unix(socket_path(region)).with_cookie(cookie());
            (*region, handle)
        })
        .collect();

    loop {
        println!("--- Fleet Status Report ---");

        for (region, handle) in &regions {
            match handle.which_children().await {
                Ok(children) => {
                    println!("\n[{region}] - {} devices online:", children.len());
                    for child in children {
                        println!(
                            "  • {} - {:?} (restart: {:?})",
                            child.id, child.child_type, child.restart_policy
                        );
                    }
                }
                Err(e) => {
                    println!("\n[{region}] unreachable: {e}");
                    println!(
                        "  start it with: cargo run --example cluster_control -- region {region}"
                    );
                }
            }
        }

        println!("\n---------------------------\n");
        sleep(Duration::from_secs(15)).await;
    }
}

#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, region] if mode == "region" => run_region(region).await,
        [] => {
            run_control().await;
            Ok(())
        }
        _ => {
            eprintln!("usage: cluster_control [region <name>]");
            std::process::exit(2);
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("This example requires Unix sockets and is not supported on Windows.");
    std::process::exit(1);
}
