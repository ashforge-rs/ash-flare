//! Multi-region cluster control client

use ash_flare::distributed::RemoteSupervisorHandle;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("=== IoT Fleet Management - Cluster Control ===\n");

    // Connect to both regional supervisors
    let us_west = RemoteSupervisorHandle::unix("/tmp/supervisor-us-west.sock");

    let eu_west = RemoteSupervisorHandle::unix("/tmp/supervisor-eu-west.sock");

    println!("✓ Connected to US-West regional supervisor");
    println!("✓ Connected to EU-West regional supervisor\n");

    loop {
        println!("--- Fleet Status Report ---");

        // Query US-West region
        match us_west.which_children().await {
            Ok(children) => {
                println!("\n[US-West Region] - {} devices online:", children.len());
                for child in children {
                    println!(
                        "  • {} - {:?} (restart: {:?})",
                        child.id, child.child_type, child.restart_policy
                    );
                }
            }
            Err(e) => println!("[US-West Region] Error: {}", e),
        }

        // Query EU-West region
        match eu_west.which_children().await {
            Ok(children) => {
                println!("\n[EU-West Region] - {} devices online:", children.len());
                for child in children {
                    println!(
                        "  • {} - {:?} (restart: {:?})",
                        child.id, child.child_type, child.restart_policy
                    );
                }
            }
            Err(e) => println!("[EU-West Region] Error: {}", e),
        }

        println!("\n---------------------------\n");
        sleep(Duration::from_secs(15)).await;
    }
}
