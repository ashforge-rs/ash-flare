//! Process monitor example - supervising external system processes

use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{interval, sleep};

#[derive(Debug)]
struct ProcessError(String);

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ProcessError {}

struct ProcessMonitor {
    process_name: String,
    command: String,
    args: Vec<String>,
    check_interval_secs: u64,
    child_process: Option<Child>,
}

impl ProcessMonitor {
    fn new(
        process_name: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
        check_interval_secs: u64,
    ) -> Self {
        Self {
            process_name: process_name.into(),
            command: command.into(),
            args,
            check_interval_secs,
            child_process: None,
        }
    }

    async fn start_process(&mut self) -> Result<(), ProcessError> {
        println!(
            "[{}] Starting process: {} {}",
            self.process_name,
            self.command,
            self.args.join(" ")
        );

        let child = Command::new(&self.command)
            .args(&self.args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null())
            .spawn()
            .map_err(|e| ProcessError(format!("Failed to spawn process: {}", e)))?;

        let pid = child.id().unwrap_or(0);
        println!("[{}] Process started with PID: {}", self.process_name, pid);

        self.child_process = Some(child);
        Ok(())
    }

    async fn check_process_alive(&mut self) -> Result<bool, ProcessError> {
        if let Some(ref mut child) = self.child_process {
            match child.try_wait() {
                Ok(Some(status)) => {
                    println!(
                        "[{}] Process exited with status: {}",
                        self.process_name, status
                    );
                    Ok(false)
                }
                Ok(None) => Ok(true),
                Err(e) => {
                    println!("[{}] Error checking process: {}", self.process_name, e);
                    Ok(false)
                }
            }
        } else {
            Ok(false)
        }
    }

    async fn stop_process(&mut self) -> Result<(), ProcessError> {
        if let Some(mut child) = self.child_process.take() {
            println!("[{}] Stopping process...", self.process_name);

            let _ = child.kill().await;

            match child.wait().await {
                Ok(status) => {
                    println!(
                        "[{}] Process terminated with status: {}",
                        self.process_name, status
                    );
                }
                Err(e) => {
                    println!("[{}] Error waiting for process: {}", self.process_name, e);
                }
            }
        }
        Ok(())
    }
}
impl Worker for ProcessMonitor {
    type Error = ProcessError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Initializing process monitor", self.process_name);
        self.start_process().await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut check_timer = interval(Duration::from_secs(self.check_interval_secs));

        loop {
            check_timer.tick().await;

            if !self.check_process_alive().await? {
                println!(
                    "[{}] ⚠️  Process not running! Worker will terminate and supervisor will restart it.",
                    self.process_name
                );
                return Err(ProcessError(format!(
                    "Process {} is not running",
                    self.process_name
                )));
            }

            println!("[{}] ✓ Process is healthy", self.process_name);
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Shutting down monitor", self.process_name);
        self.stop_process().await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Process Monitor Example ===\n");
    println!("This example demonstrates supervising external system processes.");
    println!("The supervisor will automatically restart processes when they fail.\n");

    let spec = SupervisorSpec::new("process-supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        // The workers here fail on purpose, more often than the default of 3
        // restarts in 5 seconds allows; past that the supervisor would give up.
        .with_restart_intensity(RestartIntensity::new(10, 5))
        .with_worker(
            "web-server-monitor",
            || ProcessMonitor::new("web-server", "sleep", vec!["3".to_string()], 2),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "api-service-monitor",
            || ProcessMonitor::new("api-service", "sleep", vec!["5".to_string()], 2),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "background-job-monitor",
            || ProcessMonitor::new("background-job", "sleep", vec!["7".to_string()], 3),
            RestartPolicy::Permanent,
        );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    println!("Started supervisor with 3 process monitors\n");
    println!("Watch as monitors detect failures and supervisor restarts them...\n");

    sleep(Duration::from_secs(18)).await;

    println!("\n=== Shutting down supervisor ===");
    supervisor.shutdown().await?;

    println!("\n=== Example completed ===");
    Ok(())
}
