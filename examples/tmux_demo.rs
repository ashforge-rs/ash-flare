//! Tmux Split Panel Demo
//!
//! This demo creates two tmux panes:
//! - Left pane: Real-time supervisor tree visualization
//! - Right pane: Live worker logs and events

use ash_flare::{
    ChildType, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};

// ============================================================================
// Shared Event Log
// ============================================================================

#[derive(Debug, Clone)]
enum LogEvent {
    WorkerStarted(String),
    WorkerRunning(String, u32),
    WorkerFailed(String, u32),
    WorkerRestarting(String),
    Heartbeat(String),
}

#[derive(Clone)]
struct EventLog {
    events: Arc<RwLock<Vec<(std::time::Instant, LogEvent)>>>,
    max_events: usize,
}

impl EventLog {
    fn new(max_events: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            max_events,
        }
    }

    async fn log(&self, event: LogEvent) {
        let mut events = self.events.write().await;
        events.push((std::time::Instant::now(), event));

        // Keep only the last N events
        let len = events.len();
        if len > self.max_events {
            events.drain(0..(len - self.max_events));
        }
    }

    async fn get_recent(&self, count: usize) -> Vec<(std::time::Instant, LogEvent)> {
        let events = self.events.read().await;
        events
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

// ============================================================================
// Worker Implementation
// ============================================================================

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

struct DemoWorker {
    name: String,
    fail_after: u32,
    counter: Arc<RwLock<u32>>,
    stats: Arc<RwLock<WorkerStats>>,
    event_log: EventLog,
}

impl DemoWorker {
    fn new(
        name: impl Into<String>,
        fail_after: u32,
        stats: Arc<RwLock<WorkerStats>>,
        event_log: EventLog,
    ) -> Self {
        let name_str = name.into();
        Self {
            name: name_str,
            fail_after,
            counter: Arc::new(RwLock::new(0)),
            stats,
            event_log,
        }
    }
}
impl Worker for DemoWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Mark as running
        {
            let mut stats = self.stats.write().await;
            stats.mark_running(&self.name);
        }

        self.event_log
            .log(LogEvent::WorkerStarted(self.name.clone()))
            .await;

        loop {
            let mut counter = self.counter.write().await;
            *counter += 1;
            let current = *counter;

            // Update heartbeat
            {
                let mut stats = self.stats.write().await;
                stats.heartbeat(&self.name);
            }

            // Log every few ticks
            if current % 5 == 0 {
                self.event_log
                    .log(LogEvent::WorkerRunning(self.name.clone(), current))
                    .await;
            } else {
                self.event_log
                    .log(LogEvent::Heartbeat(self.name.clone()))
                    .await;
            }

            if current >= self.fail_after {
                // Mark as failed before exiting
                {
                    let mut stats = self.stats.write().await;
                    stats.mark_failed(&self.name);
                }

                self.event_log
                    .log(LogEvent::WorkerFailed(self.name.clone(), current))
                    .await;
                self.event_log
                    .log(LogEvent::WorkerRestarting(self.name.clone()))
                    .await;

                return Err(WorkerError(format!(
                    "{} crashed after {} ticks",
                    self.name, current
                )));
            }

            drop(counter);
            sleep(Duration::from_millis(500)).await;
        }
    }
}

// ============================================================================
// Statistics Tracking
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum WorkerStatus {
    Starting,
    Running,
    Failed,
    Restarting,
}

#[derive(Debug, Clone)]
struct WorkerState {
    status: WorkerStatus,
    restarts: u32,
    last_heartbeat: std::time::Instant,
}

#[derive(Debug, Clone)]
struct WorkerStats {
    states: HashMap<String, WorkerState>,
    total_restarts: u32,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            states: HashMap::new(),
            total_restarts: 0,
        }
    }

    fn mark_running(&mut self, name: &str) {
        let state = self.states.entry(name.to_string()).or_insert(WorkerState {
            status: WorkerStatus::Starting,
            restarts: 0,
            last_heartbeat: std::time::Instant::now(),
        });

        if state.status == WorkerStatus::Failed || state.status == WorkerStatus::Restarting {
            state.restarts += 1;
            self.total_restarts += 1;
        }

        state.status = WorkerStatus::Running;
        state.last_heartbeat = std::time::Instant::now();
    }

    fn mark_failed(&mut self, name: &str) {
        if let Some(state) = self.states.get_mut(name) {
            state.status = WorkerStatus::Restarting;
        }
    }

    fn heartbeat(&mut self, name: &str) {
        if let Some(state) = self.states.get_mut(name) {
            state.last_heartbeat = std::time::Instant::now();
        }
    }

    fn get_status(&self, name: &str) -> Option<&WorkerState> {
        self.states.get(name)
    }
}

// ============================================================================
// Tree Rendering (Left Pane)
// ============================================================================

async fn render_tree_pane(
    root: &SupervisorHandle,
    stats: &Arc<RwLock<WorkerStats>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clear screen
    print!("\x1B[2J\x1B[1;1H");

    // Colorful header
    println!(
        "\x1B[1;36m╔═══════════════════════════════════════════════════════════════════════════════╗\x1B[0m"
    );
    println!(
        "\x1B[1;36m║\x1B[0m\x1B[1;35m                    🌳 SUPERVISOR TREE STATE 🌳\x1B[0m                            \x1B[1;36m║\x1B[0m"
    );
    println!(
        "\x1B[1;36m╚═══════════════════════════════════════════════════════════════════════════════╝\x1B[0m"
    );

    // Legend with colors
    println!("\n\x1B[1;33m📊 LEGEND:\x1B[0m");
    println!("  \x1B[1;34m📁 Supervisor\x1B[0m  │  \x1B[1;32m⚙️  Worker\x1B[0m");
    println!(
        "  \x1B[1;35m♻️  Permanent\x1B[0m  │  \x1B[1;33m⏱️  Temporary\x1B[0m  │  \x1B[1;36m🔄 Transient\x1B[0m"
    );
    println!(
        "  \x1B[1;32m✅ Running\x1B[0m    │  \x1B[1;31m❌ Failed\x1B[0m     │  \x1B[1;33m🔄 Restarting\x1B[0m"
    );

    // Statistics
    let stats_guard = stats.read().await;
    let running_count = stats_guard
        .states
        .values()
        .filter(|s| s.status == WorkerStatus::Running)
        .count();

    println!(
        "\n\x1B[1;33m📈 STATS:\x1B[0m Workers: \x1B[1;32m{}\x1B[0m  │  Total Restarts: \x1B[1;35m{}\x1B[0m",
        running_count, stats_guard.total_restarts
    );

    println!("\n\x1B[1;36m🌳 TREE:\x1B[0m");
    println!(
        "\x1B[2;36m─────────────────────────────────────────────────────────────────────────────────\x1B[0m"
    );

    // Render root
    println!(
        "\x1B[1;35m📦 {}\x1B[0m \x1B[2;37m(root)\x1B[0m",
        root.name()
    );

    // Render children recursively
    if let Ok(children) = root.which_children().await {
        render_children(&children, "", &stats_guard).await?;
    }

    println!(
        "\n\x1B[2;36m─────────────────────────────────────────────────────────────────────────────────\x1B[0m"
    );
    println!("\x1B[2;37mPress Ctrl+C to exit  │  Refresh: 2s\x1B[0m");

    Ok(())
}

async fn render_children(
    children: &[ash_flare::ChildInfo],
    prefix: &str,
    stats: &WorkerStats,
) -> Result<(), Box<dyn std::error::Error>> {
    for (idx, child) in children.iter().enumerate() {
        let is_last = idx == children.len() - 1;
        let connector = if is_last { "└──" } else { "├──" };
        let continuation = if is_last { "    " } else { "│   " };

        let (type_icon, type_color) = match child.child_type {
            ChildType::Supervisor => ("📁", "\x1B[1;34m"),
            ChildType::Worker => ("⚙️", "\x1B[1;32m"),
        };

        let policy_str = match child.restart_policy {
            Some(RestartPolicy::Permanent) => "\x1B[1;35m♻️\x1B[0m ",
            Some(RestartPolicy::Temporary) => "\x1B[1;33m⏱️\x1B[0m ",
            Some(RestartPolicy::Transient) => "\x1B[1;36m🔄\x1B[0m",
            None => "  ",
        };

        let status_str = if child.child_type == ChildType::Worker {
            if let Some(state) = stats.get_status(&child.id) {
                let age = state.last_heartbeat.elapsed().as_secs();
                match state.status {
                    WorkerStatus::Running if age < 3 => "\x1B[1;32m✅\x1B[0m",
                    WorkerStatus::Running => "\x1B[1;33m⚠️\x1B[0m ",
                    WorkerStatus::Failed => "\x1B[1;31m❌\x1B[0m",
                    WorkerStatus::Restarting => "\x1B[1;33m🔄\x1B[0m",
                    WorkerStatus::Starting => "\x1B[1;34m🔵\x1B[0m",
                }
            } else {
                "\x1B[2;37m⏸️\x1B[0m "
            }
        } else {
            "  "
        };

        let restart_info = if child.child_type == ChildType::Worker {
            if let Some(state) = stats.get_status(&child.id) {
                if state.restarts > 0 {
                    format!(" \x1B[1;35m(↻{})\x1B[0m", state.restarts)
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        println!(
            "\x1B[2;36m{}{}\x1B[0m {} {}{}\x1B[0m {} {}{}",
            prefix,
            connector,
            type_icon,
            type_color,
            child.id,
            policy_str,
            status_str,
            restart_info
        );

        if child.child_type == ChildType::Supervisor {
            let new_prefix = format!("{}{}", prefix, continuation);
            println!(
                "\x1B[2;36m{}\x1B[0m    \x1B[2;33m↳ (nested)\x1B[0m",
                new_prefix
            );
        }
    }

    Ok(())
}

// ============================================================================
// Log Rendering (Right Pane)
// ============================================================================

async fn render_log_pane(event_log: &EventLog) {
    // Clear screen
    print!("\x1B[2J\x1B[1;1H");

    println!(
        "\x1B[1;36m╔═══════════════════════════════════════════════════════════════════════════════╗\x1B[0m"
    );
    println!(
        "\x1B[1;36m║\x1B[0m\x1B[1;33m                      📝 WORKER EVENT LOG 📝\x1B[0m                              \x1B[1;36m║\x1B[0m"
    );
    println!(
        "\x1B[1;36m╚═══════════════════════════════════════════════════════════════════════════════╝\x1B[0m\n"
    );

    let events = event_log.get_recent(100).await;

    if events.is_empty() {
        println!("  \x1B[2;37mNo events yet...\x1B[0m\n");
    } else {
        for (timestamp, event) in events.iter().rev().take(35) {
            let elapsed = timestamp.elapsed();
            let time_str = format!(
                "\x1B[2;36m{:>3}.{:03}s\x1B[0m",
                elapsed.as_secs(),
                elapsed.subsec_millis()
            );

            match event {
                LogEvent::WorkerStarted(name) => {
                    println!(
                        "  {} \x1B[2;36m│\x1B[0m \x1B[1;32m🚀\x1B[0m \x1B[1;37m{}\x1B[0m \x1B[32mstarted\x1B[0m",
                        time_str, name
                    );
                }
                LogEvent::WorkerRunning(name, tick) => {
                    println!(
                        "  {} \x1B[2;36m│\x1B[0m \x1B[1;33m⚡\x1B[0m \x1B[1;37m{}\x1B[0m tick \x1B[1;33m#{}\x1B[0m",
                        time_str, name, tick
                    );
                }
                LogEvent::WorkerFailed(name, tick) => {
                    println!(
                        "  {} \x1B[2;36m│\x1B[0m \x1B[1;31m❌\x1B[0m \x1B[1;37m{}\x1B[0m \x1B[1;31mFAILED\x1B[0m at tick \x1B[31m#{}\x1B[0m",
                        time_str, name, tick
                    );
                }
                LogEvent::WorkerRestarting(name) => {
                    println!(
                        "  {} \x1B[2;36m│\x1B[0m \x1B[1;33m🔄\x1B[0m \x1B[1;37m{}\x1B[0m \x1B[33mrestarting...\x1B[0m",
                        time_str, name
                    );
                }
                LogEvent::Heartbeat(name) => {
                    println!(
                        "  {} \x1B[2;36m│\x1B[0m \x1B[35m💓\x1B[0m \x1B[2;37m{}\x1B[0m \x1B[2;37mheartbeat\x1B[0m",
                        time_str, name
                    );
                }
            }
        }
    }

    println!(
        "\n\x1B[2;36m─────────────────────────────────────────────────────────────────────────────────\x1B[0m"
    );
    println!("\x1B[2;37mShowing last 35 events  │  Refresh: 1s\x1B[0m");
}

// ============================================================================
// Demo Tree Setup
// ============================================================================

fn create_demo_tree(stats: Arc<RwLock<WorkerStats>>, event_log: EventLog) -> SupervisorSpec {
    // Database layer
    let db_cluster = SupervisorSpec::new("database")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "db-primary",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("db-primary", 20, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "db-replica-1",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("db-replica-1", 18, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "db-replica-2",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("db-replica-2", 22, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        );

    // API layer
    let api_cluster = SupervisorSpec::new("api-servers")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "api-1",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("api-1", 12, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "api-2",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("api-2", 14, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "api-3",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("api-3", 16, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "load-balancer",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("load-balancer", 30, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        );

    // Background jobs
    let jobs = SupervisorSpec::new("background-jobs")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "email-worker",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("email-worker", 10, stats.clone(), log.clone())
            },
            RestartPolicy::Transient,
        )
        .with_worker(
            "notification",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("notification", 11, stats.clone(), log.clone())
            },
            RestartPolicy::Transient,
        )
        .with_worker(
            "cleanup",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("cleanup", 25, stats.clone(), log.clone())
            },
            RestartPolicy::Temporary,
        );

    // Root
    SupervisorSpec::new("production")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(db_cluster)
        .with_supervisor(api_cluster)
        .with_supervisor(jobs)
        .with_worker(
            "metrics",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("metrics", 40, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "health-check",
            {
                let stats = stats.clone();
                let log = event_log.clone();
                move || DemoWorker::new("health-check", 35, stats.clone(), log.clone())
            },
            RestartPolicy::Permanent,
        )
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <tree|logs>", args[0]);
        eprintln!("This binary is meant to be launched by the run_tmux_demo.sh script");
        std::process::exit(1);
    }

    let mode = &args[1];

    // Shared state
    let stats = Arc::new(RwLock::new(WorkerStats::new()));
    let event_log = EventLog::new(500);

    // Start supervisor tree
    let tree_spec = create_demo_tree(stats.clone(), event_log.clone());
    let root = SupervisorHandle::start(tree_spec);

    sleep(Duration::from_millis(1000)).await;

    match mode.as_str() {
        "tree" => {
            // Render tree view
            let mut render_interval = interval(Duration::from_secs(2));
            loop {
                render_interval.tick().await;
                if let Err(e) = render_tree_pane(&root, &stats).await {
                    eprintln!("Error rendering tree: {}", e);
                    break;
                }
            }
        }
        "logs" => {
            // Render log view
            let mut render_interval = interval(Duration::from_secs(1));
            loop {
                render_interval.tick().await;
                render_log_pane(&event_log).await;
            }
        }
        _ => {
            eprintln!("Invalid mode: {}", mode);
            std::process::exit(1);
        }
    }
}
