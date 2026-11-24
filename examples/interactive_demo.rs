//! Interactive Supervisor Tree Demo
//!
//! This demo provides a comprehensive visualization of the supervisor tree with:
//! - Real-time rendering of the entire hierarchy
//! - Status indicators (running/stopped/restarting)
//! - Dynamic spawning of new workers and supervisors
//! - Interactive controls
//! - Statistics and health monitoring

use ash_flare::{
    ChildType, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{interval, sleep};

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
}

impl DemoWorker {
    fn new(name: impl Into<String>, fail_after: u32, stats: Arc<RwLock<WorkerStats>>) -> Self {
        let name_str = name.into();
        Self {
            name: name_str,
            fail_after,
            counter: Arc::new(RwLock::new(0)),
            stats,
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

        loop {
            let mut counter = self.counter.write().await;
            *counter += 1;

            // Update heartbeat
            {
                let mut stats = self.stats.write().await;
                stats.heartbeat(&self.name);
            }

            if *counter >= self.fail_after {
                // Mark as failed before exiting
                {
                    let mut stats = self.stats.write().await;
                    stats.mark_failed(&self.name);
                }

                return Err(WorkerError(format!(
                    "{} crashed after {} ticks",
                    self.name, *counter
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
// Tree Rendering
// ============================================================================

async fn render_tree(
    root: &SupervisorHandle,
    stats: &Arc<RwLock<WorkerStats>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clear screen and move cursor to top
    print!("\x1B[2J\x1B[1;1H");

    // Header
    println!("╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    INTERACTIVE SUPERVISOR TREE DEMO                           ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝");

    // Legend
    println!("\n📊 LEGEND:");
    println!("  Types:   📁 Supervisor  │  ⚙️  Worker");
    println!("  Policy:  ♻️  Permanent  │  ⏱️  Temporary  │  🔄 Transient");
    println!("  Status:  ✅ Running    │  ❌ Failed     │  🔄 Restarting  │  ⏸️  Stopped");

    // Statistics
    let stats_guard = stats.read().await;
    let running_count = stats_guard
        .states
        .values()
        .filter(|s| s.status == WorkerStatus::Running)
        .count();
    let _failed_count = stats_guard
        .states
        .values()
        .filter(|s| s.status == WorkerStatus::Failed || s.status == WorkerStatus::Restarting)
        .count();

    println!("\n📈 STATISTICS:");
    println!(
        "  Workers Running: {}  │  Total Restarts: {}",
        running_count, stats_guard.total_restarts
    );

    println!("\n🌳 TREE STRUCTURE:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Render root
    println!("📦 {} (root supervisor)", root.name());

    // Render children recursively
    if let Ok(children) = root.which_children().await {
        render_children_recursive(&children, "", true, &stats_guard, root, 0).await?;
    }

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("\n⌨️  CONTROLS:");
    println!("  Press Ctrl+C to exit  │  Refresh every 2 seconds");
    println!("  Workers automatically fail and restart to demonstrate supervision");

    Ok(())
}

async fn render_children_recursive(
    children: &[ash_flare::ChildInfo],
    prefix: &str,
    _is_root: bool,
    stats: &WorkerStats,
    _parent_handle: &SupervisorHandle,
    depth: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_DEPTH: usize = 10; // Prevent infinite recursion

    if depth > MAX_DEPTH {
        return Ok(());
    }

    for (idx, child) in children.iter().enumerate() {
        let is_last = idx == children.len() - 1;
        let connector = if is_last { "└──" } else { "├──" };
        let continuation = if is_last { "    " } else { "│   " };

        // Icon based on type
        let type_icon = match child.child_type {
            ChildType::Supervisor => "📁",
            ChildType::Worker => "⚙️",
        };

        // Policy indicator
        let policy_str = match child.restart_policy {
            Some(RestartPolicy::Permanent) => " ♻️ ",
            Some(RestartPolicy::Temporary) => " ⏱️ ",
            Some(RestartPolicy::Transient) => " 🔄",
            None => "   ",
        };

        // Status indicator for workers
        let status_str = if child.child_type == ChildType::Worker {
            if let Some(state) = stats.get_status(&child.id) {
                let age = state.last_heartbeat.elapsed().as_secs();
                match state.status {
                    WorkerStatus::Running if age < 3 => " ✅",
                    WorkerStatus::Running => " ⚠️ ", // Stale
                    WorkerStatus::Failed => " ❌",
                    WorkerStatus::Restarting => " 🔄",
                    WorkerStatus::Starting => " 🔵",
                }
            } else {
                " ⏸️ " // Unknown/stopped
            }
        } else {
            "   "
        };

        // Restart count
        let restart_info = if child.child_type == ChildType::Worker {
            if let Some(state) = stats.get_status(&child.id) {
                if state.restarts > 0 {
                    format!(" (↻ {})", state.restarts)
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
            "{}{} {} {}{} {}{}",
            prefix, connector, type_icon, child.id, policy_str, status_str, restart_info
        );

        // If it's a supervisor, we could recursively query its children
        // But since we don't have nested supervisor handles, we show a placeholder
        if child.child_type == ChildType::Supervisor {
            let new_prefix = format!("{}{}", prefix, continuation);
            println!("{}    ↳ (nested children...)", new_prefix);
        }
    }

    Ok(())
}

// ============================================================================
// Demo Scenarios
// ============================================================================

fn create_demo_tree(stats: Arc<RwLock<WorkerStats>>) -> SupervisorSpec {
    // Create a complex, multi-level tree structure

    // Level 3: Database cluster
    let db_primary = SupervisorSpec::new("db-primary")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "db-writer",
            {
                let stats = stats.clone();
                move || DemoWorker::new("db-writer", 15, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "db-health-check",
            {
                let stats = stats.clone();
                move || DemoWorker::new("db-health-check", 20, stats.clone())
            },
            RestartPolicy::Permanent,
        );

    let db_replica = SupervisorSpec::new("db-replica")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "db-reader-1",
            {
                let stats = stats.clone();
                move || DemoWorker::new("db-reader-1", 18, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "db-reader-2",
            {
                let stats = stats.clone();
                move || DemoWorker::new("db-reader-2", 22, stats.clone())
            },
            RestartPolicy::Permanent,
        );

    // Level 2: Data layer
    let data_layer = SupervisorSpec::new("data-layer")
        .with_restart_strategy(RestartStrategy::RestForOne)
        .with_supervisor(db_primary)
        .with_supervisor(db_replica)
        .with_worker(
            "cache-manager",
            {
                let stats = stats.clone();
                move || DemoWorker::new("cache-manager", 25, stats.clone())
            },
            RestartPolicy::Permanent,
        );

    // Level 3: API workers
    let api_cluster = SupervisorSpec::new("api-cluster")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "api-server-1",
            {
                let stats = stats.clone();
                move || DemoWorker::new("api-server-1", 12, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "api-server-2",
            {
                let stats = stats.clone();
                move || DemoWorker::new("api-server-2", 14, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "api-server-3",
            {
                let stats = stats.clone();
                move || DemoWorker::new("api-server-3", 16, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "load-balancer",
            {
                let stats = stats.clone();
                move || DemoWorker::new("load-balancer", 30, stats.clone())
            },
            RestartPolicy::Permanent,
        );

    // Level 2: Application layer
    let app_layer = SupervisorSpec::new("application-layer")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(api_cluster)
        .with_worker(
            "auth-service",
            {
                let stats = stats.clone();
                move || DemoWorker::new("auth-service", 28, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "session-manager",
            {
                let stats = stats.clone();
                move || DemoWorker::new("session-manager", 24, stats.clone())
            },
            RestartPolicy::Permanent,
        );

    // Level 3: Background jobs
    let job_workers = SupervisorSpec::new("job-workers")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "email-worker",
            {
                let stats = stats.clone();
                move || DemoWorker::new("email-worker", 10, stats.clone())
            },
            RestartPolicy::Transient,
        )
        .with_worker(
            "notification-worker",
            {
                let stats = stats.clone();
                move || DemoWorker::new("notification-worker", 11, stats.clone())
            },
            RestartPolicy::Transient,
        )
        .with_worker(
            "cleanup-worker",
            {
                let stats = stats.clone();
                move || DemoWorker::new("cleanup-worker", 35, stats.clone())
            },
            RestartPolicy::Temporary,
        );

    // Level 1: Root supervisor
    SupervisorSpec::new("production-system")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(data_layer)
        .with_supervisor(app_layer)
        .with_supervisor(job_workers)
        .with_worker(
            "metrics-collector",
            {
                let stats = stats.clone();
                move || DemoWorker::new("metrics-collector", 40, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "health-monitor",
            {
                let stats = stats.clone();
                move || DemoWorker::new("health-monitor", 45, stats.clone())
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "audit-logger",
            {
                let stats = stats.clone();
                move || DemoWorker::new("audit-logger", 50, stats.clone())
            },
            RestartPolicy::Permanent,
        )
}

// ============================================================================
// Interactive Commands (placeholder for future expansion)
// ============================================================================

// ============================================================================
// Main Demo
// ============================================================================

#[tokio::main]
async fn main() {
    println!("🚀 Starting Interactive Supervisor Tree Demo...\n");

    // Create shared statistics
    let stats = Arc::new(RwLock::new(WorkerStats::new()));

    // Build and start the supervisor tree
    let tree_spec = create_demo_tree(stats.clone());
    let root = SupervisorHandle::start(tree_spec);

    println!("📦 Supervisor tree started!");
    println!("⏳ Initializing workers...\n");

    // Give the tree time to initialize
    sleep(Duration::from_millis(1000)).await;

    // Render loop
    let mut render_interval = interval(Duration::from_secs(2));

    loop {
        render_interval.tick().await;

        match render_tree(&root, &stats).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error rendering tree: {}", e);
                break;
            }
        }
    }
}
