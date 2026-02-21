//! Stateful supervisor example using the stateful_supervision_tree macro
//!
//! This example demonstrates workers sharing state through a WorkerContext
//! using a declarative macro syntax.

use ash_flare::{StatefulSupervisorHandle, Worker, WorkerContext, stateful_supervision_tree};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

// Auction worker that can be either a bidder or monitor
enum AuctionWorker {
    Bidder { id: u32, ctx: Arc<WorkerContext> },
    Monitor { ctx: Arc<WorkerContext> },
}

impl AuctionWorker {
    fn bidder(id: u32, ctx: Arc<WorkerContext>) -> Self {
        Self::Bidder { id, ctx }
    }

    fn monitor(ctx: Arc<WorkerContext>) -> Self {
        Self::Monitor { ctx }
    }
}

#[async_trait]
impl Worker for AuctionWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        match self {
            AuctionWorker::Bidder { id, ctx } => {
                // Bidder logic
                for round in 1..=3 {
                    let current_bid = ctx.get("highest_bid").and_then(|v| v.as_u64()).unwrap_or(0);

                    let my_bid = current_bid + (*id as u64 * 10);
                    ctx.set("highest_bid", serde_json::json!(my_bid));
                    ctx.set("last_bidder", serde_json::json!(id));

                    ctx.update("total_bids", |v| {
                        let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
                        Some(serde_json::json!(count + 1))
                    });

                    println!("Bidder {} placed bid ${} in round {}", id, my_bid, round);
                    sleep(Duration::from_millis(100)).await;
                }
                Ok(())
            }
            AuctionWorker::Monitor { ctx } => {
                // Monitor logic
                for _ in 0..5 {
                    sleep(Duration::from_millis(80)).await;

                    let highest_bid = ctx.get("highest_bid").and_then(|v| v.as_u64()).unwrap_or(0);

                    let last_bidder = ctx.get("last_bidder").and_then(|v| v.as_u64()).unwrap_or(0);

                    let total_bids = ctx.get("total_bids").and_then(|v| v.as_u64()).unwrap_or(0);

                    if total_bids > 0 {
                        println!(
                            "📊 Monitor: Highest bid: ${}, Last bidder: {}, Total bids: {}",
                            highest_bid, last_bidder, total_bids
                        );
                    }
                }
                Ok(())
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔨 Starting stateful auction supervisor...\n");

    // Build supervision tree using the macro
    let spec = stateful_supervision_tree! {
        name: "auction-supervisor",
        strategy: OneForOne,
        intensity: (5, 10),
        workers: [
            ("bidder-1", |ctx| AuctionWorker::bidder(1, ctx), Temporary),
            ("bidder-2", |ctx| AuctionWorker::bidder(2, ctx), Temporary),
            ("bidder-3", |ctx| AuctionWorker::bidder(3, ctx), Temporary),
            ("monitor", |ctx| AuctionWorker::monitor(ctx), Temporary),
        ],
        supervisors: []
    };

    // Get reference to shared context before starting
    let context: Arc<WorkerContext> = Arc::clone(spec.context());

    // Start the supervisor
    let handle = StatefulSupervisorHandle::start(spec);

    println!("👥 Workers: 3 bidders + 1 monitor");
    println!("📦 Shared store: WorkerContext with DashMap\n");

    // Wait for workers to complete
    sleep(Duration::from_millis(500)).await;

    // Print final results
    println!("\n🏁 Auction Results:");
    println!("─────────────────────────────────");

    if let Some(highest_bid) = context.get("highest_bid") {
        println!("💰 Winning bid: ${}", highest_bid);
    }

    if let Some(winner) = context.get("last_bidder") {
        println!("🏆 Winner: Bidder {}", winner);
    }

    if let Some(total) = context.get("total_bids") {
        println!("📊 Total bids placed: {}", total);
    }

    // List all keys in store
    println!("\n🗄️  Items in shared store:");
    for key in ["highest_bid", "last_bidder", "total_bids"] {
        if let Some(value) = context.get(key) {
            println!("   • {}: {}", key, value);
        }
    }

    // Graceful shutdown
    handle.shutdown().await?;
    println!("\n✅ Supervisor shut down gracefully");

    Ok(())
}
