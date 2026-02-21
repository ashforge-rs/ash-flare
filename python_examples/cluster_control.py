#!/usr/bin/env python3
"""
Cluster Control Client - Multi-region supervisor monitoring

This example demonstrates:
- Connecting to remote supervisors via Unix sockets
- Monitoring multiple distributed supervisors
- Periodic status polling
- Fleet management pattern

Python equivalent of: examples/cluster_control.rs

Note: This requires supervisors to be running with distributed servers.
You can start them using the distributed.rs Rust example or create
Python supervisors with server support.
"""

import ash_flare as af
import time
import sys


def query_region(region_name, remote_handle):
    """Query a remote supervisor and display its status"""
    try:
        children = remote_handle.which_children()
        print(f"\n[{region_name}] - {len(children)} devices online:")
        for child in children:
            policy = child.restart_policy
            policy_str = str(policy) if policy else "None"
            child_type = "Supervisor" if child.child_type.is_supervisor() else "Worker"
            print(f"  • {child.id} - {child_type} (restart: {policy_str})")
        return True
    except Exception as e:
        print(f"[{region_name}] Error: {e}")
        return False


def main():
    print("=== IoT Fleet Management - Cluster Control ===\n")
    
    # Try to connect to regional supervisors
    # Note: You'll need to have supervisors running with distributed servers
    # on these Unix socket paths
    
    us_west_path = "/tmp/supervisor-us-west.sock"
    eu_west_path = "/tmp/supervisor-eu-west.sock"
    
    print("Attempting to connect to regional supervisors...")
    print(f"  US-West: {us_west_path}")
    print(f"  EU-West: {eu_west_path}\n")
    
    try:
        us_west = af.RemoteSupervisorHandle.connect_unix(us_west_path)
        print("✓ Connected to US-West regional supervisor")
    except Exception as e:
        print(f"✗ Failed to connect to US-West: {e}")
        print("\nHint: Start a supervisor server first:")
        print("  cargo run --example distributed")
        sys.exit(1)
    
    try:
        eu_west = af.RemoteSupervisorHandle.connect_unix(eu_west_path)
        print("✓ Connected to EU-West regional supervisor")
    except Exception as e:
        print(f"✗ Failed to connect to EU-West: {e}")
        print("\nNote: Continuing with US-West only...\n")
        eu_west = None
    
    print("\nStarting fleet monitoring (press Ctrl+C to exit)...\n")
    
    try:
        iteration = 0
        while True:
            iteration += 1
            print(f"--- Fleet Status Report #{iteration} ---")
            
            # Query US-West region
            query_region("US-West Region", us_west)
            
            # Query EU-West region if available
            if eu_west:
                query_region("EU-West Region", eu_west)
            
            print("\n" + "-" * 50 + "\n")
            time.sleep(15)
            
    except KeyboardInterrupt:
        print("\n\nShutting down cluster control client...")
        print("✓ Disconnected from all regions")


if __name__ == "__main__":
    main()
