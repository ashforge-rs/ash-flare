//! Large supervision tree example with 50+ workers
//!
//! This example demonstrates a complex, multi-layered supervision tree
//! simulating a microservices architecture with:
//! - API Gateway layer (3 workers)
//! - Service layer (4 supervisors, each with 3-5 workers)
//! - Data layer (3 supervisors for different databases)
//! - Background job processors (2 supervisors with worker pools)
//! - Monitoring & metrics collectors
//!
//! Total: 55 workers

use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;

// Global request counter for demo purposes
static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// Worker Error Type
// ============================================================================

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

// ============================================================================
// API Gateway Layer Workers
// ============================================================================

#[derive(Debug)]
struct HttpGateway {
    id: u32,
}
impl Worker for HttpGateway {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🌐 [HTTP-Gateway-{}] Starting up...", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            let count = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
            if count % 100 == 0 {
                println!("🌐 [HTTP-Gateway-{}] Processed {} requests", self.id, count);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🌐 [HTTP-Gateway-{}] Shutting down", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct WebSocketGateway {
    connections: u32,
}
impl Worker for WebSocketGateway {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!(
            "🔌 [WebSocket-Gateway] Managing {} connections",
            self.connections
        );
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔌 [WebSocket-Gateway] Closing all connections");
        Ok(())
    }
}

#[derive(Debug)]
struct GraphQLGateway;
impl Worker for GraphQLGateway {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🔺 [GraphQL-Gateway] Schema loaded");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(75)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔺 [GraphQL-Gateway] Shutting down");
        Ok(())
    }
}

// ============================================================================
// User Service Workers
// ============================================================================

#[derive(Debug)]
struct AuthService {
    id: u32,
}
impl Worker for AuthService {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🔐 [Auth-{}] JWT validator ready", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(60)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔐 [Auth-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct ProfileService {
    id: u32,
}
impl Worker for ProfileService {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("👤 [Profile-{}] Cache warmed", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(80)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("👤 [Profile-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct NotificationService {
    id: u32,
}
impl Worker for NotificationService {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📧 [Notification-{}] Email/SMS provider connected", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(120)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📧 [Notification-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Content Service Workers
// ============================================================================

#[derive(Debug)]
struct ContentIndexer {
    id: u32,
}
impl Worker for ContentIndexer {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📑 [Indexer-{}] Search engine connected", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(90)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📑 [Indexer-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct MediaProcessor {
    id: u32,
}
impl Worker for MediaProcessor {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🎬 [Media-{}] FFmpeg pipeline ready", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(150)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🎬 [Media-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct CdnSync {
    id: u32,
}
impl Worker for CdnSync {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🌍 [CDN-{}] Edge nodes synchronized", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🌍 [CDN-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Payment Service Workers
// ============================================================================

#[derive(Debug)]
struct PaymentProcessor {
    id: u32,
    provider: &'static str,
}
impl Worker for PaymentProcessor {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!(
            "💳 [Payment-{}-{}] Connected to provider",
            self.id, self.provider
        );
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("💳 [Payment-{}-{}] Shutdown", self.id, self.provider);
        Ok(())
    }
}

#[derive(Debug)]
struct FraudDetector {
    id: u32,
}
impl Worker for FraudDetector {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🚨 [Fraud-{}] ML model loaded", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(80)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🚨 [Fraud-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Analytics Service Workers
// ============================================================================

#[derive(Debug)]
struct EventCollector {
    id: u32,
}
impl Worker for EventCollector {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📊 [Events-{}] Kafka consumer ready", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(40)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📊 [Events-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct MetricsAggregator {
    id: u32,
}
impl Worker for MetricsAggregator {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📈 [Metrics-{}] Time-series DB connected", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📈 [Metrics-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Database Workers
// ============================================================================

#[derive(Debug)]
struct PostgresConnector {
    id: u32,
    pool_size: u32,
}
impl Worker for PostgresConnector {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!(
            "🐘 [Postgres-{}] Connection pool ({} conns) ready",
            self.id, self.pool_size
        );
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(70)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🐘 [Postgres-{}] Closing connections", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct RedisConnector {
    id: u32,
}
impl Worker for RedisConnector {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🔴 [Redis-{}] Cache cluster connected", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(30)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔴 [Redis-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct MongoConnector {
    id: u32,
}
impl Worker for MongoConnector {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🍃 [MongoDB-{}] Replica set connected", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(60)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🍃 [MongoDB-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Background Job Workers
// ============================================================================

#[derive(Debug)]
struct EmailWorker {
    id: u32,
}
impl Worker for EmailWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📮 [Email-Worker-{}] SMTP ready", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(150)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📮 [Email-Worker-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct ReportGenerator {
    id: u32,
}
impl Worker for ReportGenerator {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📋 [Report-{}] Template engine loaded", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(300)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📋 [Report-{}] Shutdown", self.id);
        Ok(())
    }
}

#[derive(Debug)]
struct DataSyncWorker {
    id: u32,
}
impl Worker for DataSyncWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🔄 [Sync-{}] CDC stream active", self.id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(180)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🔄 [Sync-{}] Shutdown", self.id);
        Ok(())
    }
}

// ============================================================================
// Monitoring Workers
// ============================================================================

#[derive(Debug)]
struct HealthChecker;
impl Worker for HealthChecker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("💚 [HealthCheck] Starting health monitoring");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_secs(5)).await;
            println!("💚 [HealthCheck] All systems operational");
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("💚 [HealthCheck] Shutdown");
        Ok(())
    }
}

#[derive(Debug)]
struct LogAggregator;
impl Worker for LogAggregator {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("📝 [LogAggregator] Connected to log streams");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("📝 [LogAggregator] Shutdown");
        Ok(())
    }
}

#[derive(Debug)]
struct AlertManager;
impl Worker for AlertManager {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("🚨 [AlertManager] PagerDuty/Slack webhooks ready");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("🚨 [AlertManager] Shutdown");
        Ok(())
    }
}

// ============================================================================
// Tree Visualization Helper
// ============================================================================

/// Recursively prints the supervision tree structure with visual formatting
async fn print_tree(handle: &SupervisorHandle, name: &str, prefix: &str, is_last: bool) {
    // Print current node
    let connector = if prefix.is_empty() {
        ""
    } else if is_last {
        "└── "
    } else {
        "├── "
    };

    println!("{}{}{}", prefix, connector, name);

    // Get children
    if let Ok(children) = handle.which_children().await {
        let child_count = children.len();

        for (idx, child) in children.iter().enumerate() {
            let is_last_child = idx == child_count - 1;

            // Calculate new prefix for children
            let new_prefix = if prefix.is_empty() {
                String::new()
            } else if is_last {
                format!("{}    ", prefix)
            } else {
                format!("{}│   ", prefix)
            };

            match child.child_type {
                ash_flare::ChildType::Worker => {
                    // Workers are leaf nodes
                    let child_connector = if is_last_child {
                        "└── "
                    } else {
                        "├── "
                    };
                    println!("{}{}💼 {}", new_prefix, child_connector, child.id);
                }
                ash_flare::ChildType::Supervisor => {
                    // Supervisors can have children - mark with 📦
                    let child_connector = if is_last_child {
                        "└── "
                    } else {
                        "├── "
                    };
                    print!("{}{}📦 ", new_prefix, child_connector);

                    // Note: We can't recursively query nested supervisors from the top-level handle
                    // in the current API, so we just mark them as supervisors
                    println!("{}", child.id);
                }
            }
        }
    }
}

// ============================================================================
// Main Supervision Tree
// ============================================================================

#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Large Supervision Tree Example - 65+ Workers               ║");
    println!("║  Microservices + 10-Level Deep Nesting Demo                 ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Supervision Tree Structure:
    //
    // microservices-platform (root)
    // ├── api-gateway
    // │   ├── http-gw-1
    // │   ├── http-gw-2
    // │   ├── http-gw-3
    // │   ├── websocket-gw
    // │   └── graphql-gw
    // ├── user-service
    // │   ├── auth-1
    // │   ├── auth-2
    // │   ├── profile-1
    // │   ├── profile-2
    // │   └── notification-1
    // ├── content-service
    // │   ├── indexer-1
    // │   ├── indexer-2
    // │   ├── media-1
    // │   ├── media-2
    // │   ├── cdn-sync-1
    // │   └── cdn-sync-2
    // ├── payment-service
    // │   ├── payment-stripe
    // │   ├── payment-paypal
    // │   ├── fraud-1
    // │   └── fraud-2
    // ├── analytics-service
    // │   ├── event-collector-1
    // │   ├── event-collector-2
    // │   ├── event-collector-3
    // │   ├── metrics-agg-1
    // │   └── metrics-agg-2
    // ├── postgres-layer
    // │   ├── postgres-master
    // │   ├── postgres-replica-1
    // │   └── postgres-replica-2
    // ├── redis-layer
    // │   ├── redis-cache-1
    // │   ├── redis-cache-2
    // │   └── redis-cache-3
    // ├── mongo-layer
    // │   ├── mongo-primary
    // │   └── mongo-secondary
    // ├── email-jobs
    // │   ├── email-worker-1
    // │   ├── email-worker-2
    // │   ├── email-worker-3
    // │   └── email-worker-4
    // ├── batch-jobs
    // │   ├── report-1
    // │   ├── report-2
    // │   ├── sync-1
    // │   └── sync-2
    // ├── deep-tree-root (10 levels deep!)
    // │   └── level-1
    // │       ├── worker-1-1
    // │       └── level-2
    // │           ├── worker-2-1
    // │           └── level-3
    // │               ├── worker-3-1
    // │               └── level-4
    // │                   ├── worker-4-1
    // │                   └── level-5
    // │                       ├── worker-5-1
    // │                       └── level-6
    // │                           ├── worker-6-1
    // │                           └── level-7
    // │                               ├── worker-7-1
    // │                               └── level-8
    // │                                   ├── worker-8-1
    // │                                   └── level-9
    // │                                       ├── worker-9-1
    // │                                       └── level-10
    // │                                           └── worker-10-1
    // ├── health-checker
    // ├── log-aggregator
    // └── alert-manager
    //
    // Total: 20 supervisors (10 in deep tree), 65 workers

    // Build API Gateway Supervisor
    let api_gateway_spec = SupervisorSpec::new("api-gateway")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(5, 10))
        .with_worker(
            "http-gw-1",
            || HttpGateway { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "http-gw-2",
            || HttpGateway { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "http-gw-3",
            || HttpGateway { id: 3 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "websocket-gw",
            || WebSocketGateway { connections: 1000 },
            RestartPolicy::Permanent,
        )
        .with_worker("graphql-gw", || GraphQLGateway, RestartPolicy::Permanent);

    // Build User Service Supervisor
    let user_service_spec = SupervisorSpec::new("user-service")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(3, 5))
        .with_worker("auth-1", || AuthService { id: 1 }, RestartPolicy::Permanent)
        .with_worker("auth-2", || AuthService { id: 2 }, RestartPolicy::Permanent)
        .with_worker(
            "profile-1",
            || ProfileService { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "profile-2",
            || ProfileService { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "notification-1",
            || NotificationService { id: 1 },
            RestartPolicy::Permanent,
        );

    // Build Content Service Supervisor
    let content_service_spec = SupervisorSpec::new("content-service")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(4, 8))
        .with_worker(
            "indexer-1",
            || ContentIndexer { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "indexer-2",
            || ContentIndexer { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "media-1",
            || MediaProcessor { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "media-2",
            || MediaProcessor { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_worker("cdn-sync-1", || CdnSync { id: 1 }, RestartPolicy::Permanent)
        .with_worker("cdn-sync-2", || CdnSync { id: 2 }, RestartPolicy::Permanent);

    // Build Payment Service Supervisor
    let payment_service_spec = SupervisorSpec::new("payment-service")
        .with_restart_strategy(RestartStrategy::OneForAll) // Critical: restart all if any fails
        .with_restart_intensity(RestartIntensity::new(2, 5))
        .with_worker(
            "payment-stripe",
            || PaymentProcessor {
                id: 1,
                provider: "stripe",
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "payment-paypal",
            || PaymentProcessor {
                id: 2,
                provider: "paypal",
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "fraud-1",
            || FraudDetector { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "fraud-2",
            || FraudDetector { id: 2 },
            RestartPolicy::Permanent,
        );

    // Build Analytics Service Supervisor
    let analytics_service_spec = SupervisorSpec::new("analytics-service")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(5, 10))
        .with_worker(
            "event-collector-1",
            || EventCollector { id: 1 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "event-collector-2",
            || EventCollector { id: 2 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "event-collector-3",
            || EventCollector { id: 3 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "metrics-agg-1",
            || MetricsAggregator { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "metrics-agg-2",
            || MetricsAggregator { id: 2 },
            RestartPolicy::Permanent,
        );

    // Build Database Layer Supervisor (Postgres)
    let postgres_spec = SupervisorSpec::new("postgres-layer")
        .with_restart_strategy(RestartStrategy::RestForOne)
        .with_restart_intensity(RestartIntensity::new(3, 10))
        .with_worker(
            "postgres-master",
            || PostgresConnector {
                id: 1,
                pool_size: 20,
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "postgres-replica-1",
            || PostgresConnector {
                id: 2,
                pool_size: 10,
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "postgres-replica-2",
            || PostgresConnector {
                id: 3,
                pool_size: 10,
            },
            RestartPolicy::Permanent,
        );

    // Build Database Layer Supervisor (Redis)
    let redis_spec = SupervisorSpec::new("redis-layer")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(5, 10))
        .with_worker(
            "redis-cache-1",
            || RedisConnector { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "redis-cache-2",
            || RedisConnector { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "redis-cache-3",
            || RedisConnector { id: 3 },
            RestartPolicy::Permanent,
        );

    // Build Database Layer Supervisor (MongoDB)
    let mongo_spec = SupervisorSpec::new("mongo-layer")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(3, 10))
        .with_worker(
            "mongo-primary",
            || MongoConnector { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "mongo-secondary",
            || MongoConnector { id: 2 },
            RestartPolicy::Permanent,
        );

    // Build Background Jobs Supervisor (Email Queue)
    let email_jobs_spec = SupervisorSpec::new("email-jobs")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(10, 20))
        .with_worker(
            "email-worker-1",
            || EmailWorker { id: 1 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "email-worker-2",
            || EmailWorker { id: 2 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "email-worker-3",
            || EmailWorker { id: 3 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "email-worker-4",
            || EmailWorker { id: 4 },
            RestartPolicy::Transient,
        );

    // Build Background Jobs Supervisor (Reports & Sync)
    let batch_jobs_spec = SupervisorSpec::new("batch-jobs")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(5, 15))
        .with_worker(
            "report-1",
            || ReportGenerator { id: 1 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "report-2",
            || ReportGenerator { id: 2 },
            RestartPolicy::Transient,
        )
        .with_worker(
            "sync-1",
            || DataSyncWorker { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "sync-2",
            || DataSyncWorker { id: 2 },
            RestartPolicy::Permanent,
        );

    // Build a deeply nested supervision tree (10 levels deep)
    // Demonstrates extreme nesting for stress testing
    //
    // deep-tree-root
    // └── level-1
    //     ├── worker-1-1
    //     └── level-2
    //         ├── worker-2-1
    //         └── level-3
    //             ├── worker-3-1
    //             └── level-4
    //                 ├── worker-4-1
    //                 └── level-5
    //                     ├── worker-5-1
    //                     └── level-6
    //                         ├── worker-6-1
    //                         └── level-7
    //                             ├── worker-7-1
    //                             └── level-8
    //                                 ├── worker-8-1
    //                                 └── level-9
    //                                     ├── worker-9-1
    //                                     └── level-10
    //                                         └── worker-10-1

    // Build from deepest level upward
    let level_10 = SupervisorSpec::new("level-10")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker("worker-10-1", || LogAggregator, RestartPolicy::Permanent);

    let level_9 = SupervisorSpec::new("level-9")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-9-1",
            || MetricsAggregator { id: 9 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_10);

    let level_8 = SupervisorSpec::new("level-8")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-8-1",
            || EventCollector { id: 8 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_9);

    let level_7 = SupervisorSpec::new("level-7")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-7-1",
            || DataSyncWorker { id: 7 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_8);

    let level_6 = SupervisorSpec::new("level-6")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-6-1",
            || EmailWorker { id: 6 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_7);

    let level_5 = SupervisorSpec::new("level-5")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-5-1",
            || RedisConnector { id: 5 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_6);

    let level_4 = SupervisorSpec::new("level-4")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-4-1",
            || PostgresConnector {
                id: 4,
                pool_size: 5,
            },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_5);

    let level_3 = SupervisorSpec::new("level-3")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker("worker-3-1", || CdnSync { id: 3 }, RestartPolicy::Permanent)
        .with_supervisor(level_4);

    let level_2 = SupervisorSpec::new("level-2")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-2-1",
            || MediaProcessor { id: 2 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_3);

    let level_1 = SupervisorSpec::new("level-1")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-1-1",
            || AuthService { id: 1 },
            RestartPolicy::Permanent,
        )
        .with_supervisor(level_2);

    let deep_tree_root = SupervisorSpec::new("deep-tree-root")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(3, 10))
        .with_supervisor(level_1);

    // Build top-level supervision tree
    let root_spec = SupervisorSpec::new("microservices-platform")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity::new(3, 10))
        // Gateway Layer
        .with_supervisor(api_gateway_spec)
        // Service Layer
        .with_supervisor(user_service_spec)
        .with_supervisor(content_service_spec)
        .with_supervisor(payment_service_spec)
        .with_supervisor(analytics_service_spec)
        // Data Layer
        .with_supervisor(postgres_spec)
        .with_supervisor(redis_spec)
        .with_supervisor(mongo_spec)
        // Background Jobs Layer
        .with_supervisor(email_jobs_spec)
        .with_supervisor(batch_jobs_spec)
        // Deep Nesting Demo (10 levels)
        .with_supervisor(deep_tree_root)
        // Monitoring Layer (direct workers)
        .with_worker("health-checker", || HealthChecker, RestartPolicy::Permanent)
        .with_worker("log-aggregator", || LogAggregator, RestartPolicy::Permanent)
        .with_worker("alert-manager", || AlertManager, RestartPolicy::Permanent);

    println!("\n🚀 Starting supervision tree...\n");
    let handle = SupervisorHandle::start(root_spec);

    // Let it run for a bit
    sleep(Duration::from_secs(3)).await;

    // Query and render the tree structure
    println!("\n\n📊 Runtime Supervision Tree Structure:\n");
    print_tree(&handle, "microservices-platform", "", true).await;

    // Get supervisor info
    println!("\n📈 Supervisor Statistics:");
    if let Ok(strategy) = handle.restart_strategy().await {
        println!("  Restart Strategy: {:?}", strategy);
    }
    if let Ok(uptime) = handle.uptime().await {
        println!("  Uptime: {} seconds", uptime.as_secs());
    }

    let total_requests = REQUEST_COUNTER.load(Ordering::Relaxed);
    println!("  Total Requests Processed: {}", total_requests);

    println!("\n⏳ Letting the system run for 5 more seconds...\n");
    sleep(Duration::from_secs(5)).await;

    println!("\n🛑 Initiating graceful shutdown...\n");
    match handle.shutdown().await {
        Ok(_) => println!("✅ All 65+ workers shut down gracefully (including 10-level deep tree)"),
        Err(e) => eprintln!("❌ Shutdown error: {}", e),
    }

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  Demo Complete - Complex Supervision Tree                   ║");
    println!("║  20 supervisors, 65 workers, max depth: 11 levels           ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
}
