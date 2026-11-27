//! Shared-state auction built with the `supervision_tree!` macro
//!
//! Bidders and a monitor are different worker types sharing one
//! `WorkerContext`; each factory captures its own clone of the context.

use ash_flare::{SupervisorHandle, Worker, WorkerContext, supervision_tree};
use tokio::time::{Duration, sleep};

struct Bidder {
    id: u64,
    ctx: WorkerContext,
}

impl Worker for Bidder {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let (id, ctx) = (self.id, &self.ctx);
        for round in 1..=3 {
            let current_bid = ctx.get("highest_bid").and_then(|v| v.as_u64()).unwrap_or(0);

            let my_bid = current_bid + id * 10;
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
}

struct Monitor {
    ctx: WorkerContext,
}

impl Worker for Monitor {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let ctx = &self.ctx;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔨 Starting stateful auction supervisor...\n");

    let context = WorkerContext::new();
    let bidder = |id| {
        let ctx = context.clone();
        move || Bidder {
            id,
            ctx: ctx.clone(),
        }
    };
    let monitor = {
        let ctx = context.clone();
        move || Monitor { ctx: ctx.clone() }
    };

    // Build supervision tree using the macro
    let spec = supervision_tree! {
        name: "auction-supervisor",
        strategy: OneForOne,
        intensity: (5, 10),
        workers: [
            ("bidder-1", bidder(1), Temporary),
            ("bidder-2", bidder(2), Temporary),
            ("bidder-3", bidder(3), Temporary),
            ("monitor", monitor, Temporary),
        ],
        supervisors: []
    };

    // Start the supervisor
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

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
