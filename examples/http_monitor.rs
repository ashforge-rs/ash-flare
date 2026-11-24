//! Realistic example: HTTP service health monitoring system
//!
//! This demonstrates a practical supervision tree for monitoring multiple web services.
//! The supervisor manages independent monitor workers that check service health periodically.
//!
//! Architecture:
//! - Each monitor worker checks one service endpoint
//! - Workers restart automatically if they crash (Permanent policy)
//! - OneForOne strategy: only failed monitors restart, others continue
//! - Monitors can be added/removed dynamically as services come and go

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, thiserror::Error)]
#[error("Monitor error: {0}")]
struct MonitorError(String);

/// Simulates health check status
#[derive(Debug, Clone, Copy)]
enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// A worker that monitors a single service endpoint
struct ServiceMonitor {
    service_name: String,
    endpoint: String,
    check_interval: Duration,
    failure_count: u32,
    max_failures: u32,
}

impl ServiceMonitor {
    fn new(service_name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            endpoint: endpoint.into(),
            check_interval: Duration::from_secs(3),
            failure_count: 0,
            max_failures: 3,
        }
    }

    async fn check_health(&self) -> HealthStatus {
        // Simulate health check with varying results
        // In real implementation, this would make an HTTP request
        let hash = self.service_name.len() + self.endpoint.len();
        match hash % 10 {
            0..=6 => HealthStatus::Healthy,
            7..=8 => HealthStatus::Degraded,
            _ => HealthStatus::Unhealthy,
        }
    }
}
impl Worker for ServiceMonitor {
    type Error = MonitorError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!(
            "📡 [{}] Starting health monitor for {}",
            self.service_name, self.endpoint
        );
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            let status = self.check_health().await;

            match status {
                HealthStatus::Healthy => {
                    self.failure_count = 0;
                    println!("✅ [{}] Service healthy", self.service_name);
                }
                HealthStatus::Degraded => {
                    println!("⚠️  [{}] Service degraded", self.service_name);
                }
                HealthStatus::Unhealthy => {
                    self.failure_count += 1;
                    println!(
                        "❌ [{}] Service unhealthy (failures: {}/{})",
                        self.service_name, self.failure_count, self.max_failures
                    );

                    if self.failure_count >= self.max_failures {
                        return Err(MonitorError(format!(
                            "Service {} exceeded failure threshold",
                            self.service_name
                        )));
                    }
                }
            }

            sleep(self.check_interval).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🛑 [{}] Stopping health monitor", self.service_name);
        Ok(())
    }
}

/// A worker that aggregates statistics from all monitors
struct MetricsAggregator {
    report_interval: Duration,
}

impl MetricsAggregator {
    fn new() -> Self {
        Self {
            report_interval: Duration::from_secs(10),
        }
    }
}
impl Worker for MetricsAggregator {
    type Error = MonitorError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📊 [Metrics] Starting metrics aggregator");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(self.report_interval).await;
            println!("\n📈 [Metrics] System health report:");
            println!("   - All monitors operational");
            println!("   - Automatic recovery enabled\n");
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📊 [Metrics] Stopping metrics aggregator");
        Ok(())
    }
}

/// Alert worker that would send notifications (simulated)
struct AlertManager {
    check_interval: Duration,
}

impl AlertManager {
    fn new() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
        }
    }
}
impl Worker for AlertManager {
    type Error = MonitorError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🔔 [Alerts] Starting alert manager");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(self.check_interval).await;
            // In reality, this would check for alerts and send notifications
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔔 [Alerts] Stopping alert manager");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("🚀 Starting HTTP Service Monitoring System\n");

    // Create supervisor with OneForOne strategy
    // If one monitor fails, only that monitor restarts
    let spec = SupervisorSpec::new("health_monitor_supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "api_gateway",
            || ServiceMonitor::new("API Gateway", "https://api.example.com/health"),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "auth_service",
            || ServiceMonitor::new("Auth Service", "https://auth.example.com/health"),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "payment_service",
            || ServiceMonitor::new("Payment Service", "https://pay.example.com/health"),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "metrics",
            || MetricsAggregator::new(),
            RestartPolicy::Permanent,
        )
        .with_worker("alerts", || AlertManager::new(), RestartPolicy::Permanent);

    let supervisor = SupervisorHandle::start(spec);

    // Let the system run
    println!("✨ Monitoring system is running...\n");
    sleep(Duration::from_secs(15)).await;

    // Demonstrate dynamic child management
    println!("\n🔧 Adding new service monitor dynamically...");
    let _ = supervisor
        .start_child(
            "cdn_service",
            || ServiceMonitor::new("CDN Service", "https://cdn.example.com/health"),
            RestartPolicy::Permanent,
        )
        .await;

    sleep(Duration::from_secs(10)).await;

    // Check active monitors
    if let Ok(children) = supervisor.which_children().await {
        println!("\n📋 Active monitors: {}", children.len());
        for child in children {
            println!("   - {}", child.id);
        }
    }

    sleep(Duration::from_secs(5)).await;

    // Remove a monitor
    println!("\n🔧 Removing payment service monitor...");
    let _ = supervisor.terminate_child("payment_service").await;

    sleep(Duration::from_secs(5)).await;

    // Final status
    if let Ok(children) = supervisor.which_children().await {
        println!("\n📋 Final active monitors: {}", children.len());
        for child in children {
            println!("   - {}", child.id);
        }
    }

    println!("\n🛑 Shutting down monitoring system...");
    supervisor.shutdown().await.ok();

    sleep(Duration::from_millis(500)).await;
    println!("✅ System shutdown complete");
}
