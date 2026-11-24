//! Tree visualization example - renders supervisor tree structure in terminal

use ash_flare::{
    ChildType, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::time::Duration;
use tokio::time::{interval, sleep};

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

struct SimpleWorker {
    name: String,
    fail_after: u32,
    counter: u32,
}

impl SimpleWorker {
    fn new(name: impl Into<String>, fail_after: u32) -> Self {
        Self {
            name: name.into(),
            fail_after,
            counter: 0,
        }
    }
}
impl Worker for SimpleWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            self.counter += 1;

            if self.counter >= self.fail_after {
                return Err(WorkerError(format!(
                    "{} failed after {} ticks",
                    self.name, self.counter
                )));
            }

            sleep(Duration::from_millis(800)).await;
        }
    }
}

async fn render_supervisor_tree(root: &SupervisorHandle) {
    // Clear screen
    print!("\x1B[2J\x1B[1;1H");

    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║              SUPERVISOR TREE VISUALIZATION                        ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝\n");

    println!("Legend: 📁 Supervisor  ⚙️  Worker  │  ♻️  Permanent  ⏱️  Temporary  🔄 Transient\n");

    // Render root with tree structure
    println!("🌳 {} (root)", root.name());

    // Get root's children
    if let Ok(children) = root.which_children().await {
        render_children(&children, "", true).await;
    }

    println!("\n{}", "═".repeat(70));
    println!("Press Ctrl+C to exit");
}

async fn render_children(children: &[ash_flare::ChildInfo], prefix: &str, _is_root: bool) {
    for (idx, child) in children.iter().enumerate() {
        let is_last = idx == children.len() - 1;
        let connector = if is_last { "└──" } else { "├──" };
        let icon = match child.child_type {
            ChildType::Supervisor => "📁",
            ChildType::Worker => "⚙️",
        };
        let policy_str = match child.restart_policy {
            Some(RestartPolicy::Permanent) => " ♻️",
            Some(RestartPolicy::Temporary) => " ⏱️",
            Some(RestartPolicy::Transient) => " 🔄",
            None => "",
        };

        println!(
            "{}{} {} {}{}",
            prefix, connector, icon, child.id, policy_str
        );

        // Note: We can't recursively query supervisor children because we don't have handles
        // This would require API changes to get nested supervisor handles
    }
}

#[tokio::main]
async fn main() {
    // Level 5: Deep leaf workers
    let deep_leaf_1 = SupervisorSpec::new("storage-cluster-a")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "disk-monitor-1",
            || SimpleWorker::new("disk-monitor-1", 25),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "disk-monitor-2",
            || SimpleWorker::new("disk-monitor-2", 28),
            RestartPolicy::Permanent,
        );

    let deep_leaf_2 = SupervisorSpec::new("storage-cluster-b")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "disk-monitor-3",
            || SimpleWorker::new("disk-monitor-3", 30),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "cache-worker",
            || SimpleWorker::new("cache-worker", 12),
            RestartPolicy::Transient,
        );

    // Level 4: Storage layer
    let storage_supervisor = SupervisorSpec::new("storage-layer")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(deep_leaf_1)
        .with_supervisor(deep_leaf_2)
        .with_worker(
            "storage-coordinator",
            || SimpleWorker::new("storage-coordinator", 35),
            RestartPolicy::Permanent,
        );

    // Level 3: Sensor clusters
    let sensor_cluster_east = SupervisorSpec::new("sensors-east")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "temp-sensor-1",
            || SimpleWorker::new("temp-sensor-1", 15),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "humidity-sensor-1",
            || SimpleWorker::new("humidity-sensor-1", 20),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "pressure-sensor-1",
            || SimpleWorker::new("pressure-sensor-1", 18),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "test-sensor-1",
            || SimpleWorker::new("test-sensor-1", 8),
            RestartPolicy::Temporary,
        );

    let sensor_cluster_west = SupervisorSpec::new("sensors-west")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "temp-sensor-2",
            || SimpleWorker::new("temp-sensor-2", 22),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "humidity-sensor-2",
            || SimpleWorker::new("humidity-sensor-2", 24),
            RestartPolicy::Transient,
        )
        .with_worker(
            "air-quality-sensor",
            || SimpleWorker::new("air-quality-sensor", 16),
            RestartPolicy::Permanent,
        );

    // Level 2: Regional supervisors
    let region_north = SupervisorSpec::new("region-north")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(sensor_cluster_east)
        .with_supervisor(sensor_cluster_west)
        .with_worker(
            "gateway-north",
            || SimpleWorker::new("gateway-north", 40),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "aggregator-north",
            || SimpleWorker::new("aggregator-north", 45),
            RestartPolicy::Permanent,
        );

    let region_south = SupervisorSpec::new("region-south")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(storage_supervisor)
        .with_worker(
            "gateway-south",
            || SimpleWorker::new("gateway-south", 38),
            RestartPolicy::Permanent,
        );

    // Level 1: Root supervisor
    let root_spec = SupervisorSpec::new("datacenter-prime")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(region_north)
        .with_supervisor(region_south)
        .with_worker(
            "global-monitor",
            || SimpleWorker::new("global-monitor", 60),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "logger",
            || SimpleWorker::new("logger", 55),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "health-checker",
            || SimpleWorker::new("health-checker", 50),
            RestartPolicy::Permanent,
        );

    let root = SupervisorHandle::start(root_spec);

    // Give supervisors time to start
    sleep(Duration::from_millis(500)).await;

    // Render tree every 8 seconds
    let mut render_interval = interval(Duration::from_secs(8));

    println!("Starting tree visualization (refreshes every 8 seconds)...\n");
    sleep(Duration::from_secs(1)).await;

    loop {
        render_interval.tick().await;
        render_supervisor_tree(&root).await;
    }
}
