//! A remote supervisor that only accepts clients knowing a shared cookie.
//!
//! This example demonstrates:
//! - `SupervisorServer::with_cookie` and `RemoteSupervisorHandle::with_cookie`,
//!   like an Erlang distribution cookie: both ends prove they know it by
//!   signing a random challenge, so the cookie never crosses the wire
//! - The `RemoteSupervisorHandle::tcp` constructor and `status`
//! - Clients without the cookie, or with the wrong one, being refused during
//!   the handshake, before any command runs
//!
//! Traffic after the handshake is not encrypted; keep the listener on a network
//! you trust.
//!
//! Run the self-contained demo with:
//!     cargo run --example secure_remote
//!
//! Or only serve, for other clients such as `python_examples/remote_cookie.py`,
//! until a client shuts the supervisor down:
//!     cargo run --example secure_remote -- serve

use ash_flare::distributed::{DistributedError, RemoteSupervisorHandle, SupervisorServer};
use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;
use tokio::time::sleep;

const ADDRESS: &str = "127.0.0.1:47311";
const COOKIE: &str = "correct-horse-battery-staple";

struct Service(ShutdownSignal);

impl Worker for Service {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// Starts a supervisor and serves it on `ADDRESS`, protected by `COOKIE`.
fn serve() -> Result<SupervisorHandle, Box<dyn std::error::Error>> {
    let supervisor = SupervisorHandle::start(
        SupervisorSpec::new("billing")
            .with_worker("invoices", Service, RestartPolicy::Permanent)
            .with_worker("payments", Service, RestartPolicy::Permanent)
            .with_worker("reports", Service, RestartPolicy::Transient),
    )?;
    let server = SupervisorServer::new(supervisor.clone()).with_cookie(COOKIE);
    tokio::spawn(async move {
        if let Err(error) = server.listen_tcp(ADDRESS).await {
            eprintln!("server stopped: {error}");
        }
    });
    Ok(supervisor)
}

/// Waits until something accepts connections on `ADDRESS`.
async fn wait_until_listening() {
    while tokio::net::TcpStream::connect(ADDRESS).await.is_err() {
        sleep(Duration::from_millis(20)).await;
    }
}

fn describe(result: Result<impl std::fmt::Debug, DistributedError>) -> String {
    match result {
        Ok(value) => format!("ok: {value:?}"),
        Err(DistributedError::AuthenticationRequired) => "refused: no cookie".to_owned(),
        Err(DistributedError::AuthenticationFailed(why)) => format!("refused: {why}"),
        Err(other) => format!("error: {other}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = serve()?;
    wait_until_listening().await;

    if std::env::args().nth(1).as_deref() == Some("serve") {
        println!("serving \"billing\" on {ADDRESS} with cookie {COOKIE:?}");
        println!("waiting for a client to shut it down...");
        while !supervisor.is_stopped() {
            sleep(Duration::from_millis(100)).await;
        }
        println!("shut down remotely");
        return Ok(());
    }

    println!("=== Cookie-authenticated remote supervisor on {ADDRESS} ===\n");

    let anonymous = RemoteSupervisorHandle::tcp(ADDRESS);
    println!("no cookie:      {}", describe(anonymous.status().await));

    let impostor = RemoteSupervisorHandle::tcp(ADDRESS).with_cookie("guess");
    println!(
        "wrong cookie:   {}",
        describe(impostor.which_children().await)
    );

    let operator = RemoteSupervisorHandle::tcp(ADDRESS).with_cookie(COOKIE);
    let status = operator.status().await?;
    println!(
        "right cookie:   status: {} with {} children, {:?}, up {}s",
        status.name, status.children_count, status.restart_strategy, status.uptime_secs
    );

    // The handle reuses its authenticated connection for later commands.
    operator.terminate_child("reports").await?;
    let children: Vec<_> = operator
        .which_children()
        .await?
        .into_iter()
        .map(|child| child.id)
        .collect();
    println!("                terminated reports, children now {children:?}");

    operator.shutdown().await?;
    while !supervisor.is_stopped() {
        sleep(Duration::from_millis(20)).await;
    }
    println!("                shut the supervisor down remotely");
    Ok(())
}
