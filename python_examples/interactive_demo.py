#!/usr/bin/env python3
"""
Interactive Supervisor Demo - Real-time monitoring and control

This example demonstrates:
- Building a hierarchical supervisor tree
- Real-time monitoring of supervisor state
- Dynamic worker management
- Interactive controls for adding/removing workers
- Statistics and health monitoring

Python equivalent of: examples/interactive_demo.rs (simplified)
"""

import ash_flare as af
import time
import sys
from datetime import datetime


def dummy_worker():
    """Dummy worker function for visualization - sleeps indefinitely"""
    time.sleep(3600)


class SupervisorMonitor:
    """Monitor and display supervisor tree status"""
    
    def __init__(self, root_handle):
        self.root = root_handle
        self.start_time = time.time()
        self.iteration = 0
    
    def clear_screen(self):
        """Clear terminal screen"""
        print("\033[2J\033[1;1H")
    
    def render_header(self):
        """Render status header"""
        uptime = int(time.time() - self.start_time)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print("╔═══════════════════════════════════════════════════════════════════╗")
        print("║           INTERACTIVE SUPERVISOR TREE DEMO                        ║")
        print("╚═══════════════════════════════════════════════════════════════════╝")
        print(f"\nTime: {timestamp} | Uptime: {uptime}s | Refresh #{self.iteration}")
        print("─" * 70 + "\n")
    
    def render_tree(self):
        """Render the supervisor tree"""
        print(f"🌳 {self.root.name()} (root)")
        
        try:
            children = self.root.which_children()
            self.render_children(children)
        except Exception as e:
            print(f"  Error: {e}")
    
    def render_children(self, children, indent=2):
        """Render child processes"""
        for idx, child in enumerate(children):
            prefix = " " * indent
            icon = "📁" if child.child_type.is_supervisor() else "⚙️"
            
            # Policy color coding
            policy = child.restart_policy
            if policy:
                policy_str_obj = str(policy).lower()
                if "permanent" in policy_str_obj:
                    policy_str = "permanent"
                elif "temporary" in policy_str_obj:
                    policy_str = "temporary"
                elif "transient" in policy_str_obj:
                    policy_str = "transient"
                else:
                    policy_str = "unknown"
            else:
                policy_str = "none"
            
            print(f"{prefix}{icon} {child.id} [{policy_str}]")
    
    def render_stats(self):
        """Render statistics"""
        try:
            counts = self.root.count_children()
            print("\n📊 Statistics:")
            print(f"  Supervisors: {counts['supervisors']}")
            print(f"  Workers: {counts['workers']}")
            print(f"  Total Children: {counts['supervisors'] + counts['workers']}")
        except Exception as e:
            print(f"\n📊 Statistics: Error - {e}")
    
    def render_controls(self):
        """Render control instructions"""
        print("\n" + "─" * 70)
        print("Controls:")
        print("  [a] Add dynamic worker | [r] Remove worker | [q] Quit")
        print("─" * 70)
    
    def render(self):
        """Render complete status display"""
        self.clear_screen()
        self.render_header()
        self.render_tree()
        self.render_stats()
        self.render_controls()
        self.iteration += 1


def build_demo_tree():
    """Build a demo supervisor tree"""
    # Create application supervisor
    app_spec = af.SupervisorSpec("application")
    app_spec.with_restart_strategy(af.RestartStrategy.one_for_one())
    app_spec.with_restart_intensity(af.RestartIntensity(3, 5))
    
    # Add workers
    app_spec.add_worker("database-1", af.RestartPolicy.permanent(), dummy_worker)
    app_spec.add_worker("database-2", af.RestartPolicy.permanent(), dummy_worker)
    app_spec.add_worker("api-server", af.RestartPolicy.permanent(), dummy_worker)
    app_spec.add_worker("cache", af.RestartPolicy.transient(), dummy_worker)
    app_spec.add_worker("metrics", af.RestartPolicy.temporary(), dummy_worker)
    
    # Create nested supervisor for background jobs
    jobs_spec = af.SupervisorSpec("background-jobs")
    jobs_spec.with_restart_strategy(af.RestartStrategy.one_for_all())
    jobs_spec.add_worker("email-worker", af.RestartPolicy.transient(), dummy_worker)
    app_spec.add_worker("cleanup-worker", af.RestartPolicy.transient(), dummy_worker)
    
    app_spec.add_supervisor(jobs_spec)
    
    return app_spec


def interactive_loop(root_handle, monitor):
    """Run interactive monitoring loop"""
    next_worker_id = 1
    
    while True:
        # Render current state
        monitor.render()
        
        # In a real interactive demo, you'd use proper input handling
        # For this example, we'll just auto-refresh
        print("\nAuto-refreshing in 3 seconds... (Press Ctrl+C to quit)")
        
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n\nExiting interactive demo...")
            return
        
        # Demo: Dynamically add a worker every 5 iterations
        if monitor.iteration % 5 == 0:
            worker_id = f"dynamic-worker-{next_worker_id}"
            try:
                root_handle.start_child(worker_id, af.RestartPolicy.temporary(), dummy_worker)
                next_worker_id += 1
            except Exception as e:
                pass  # Worker might already exist


def main():
    print("Starting Interactive Supervisor Demo...\n")
    
    # Build and start supervisor tree
    root_spec = build_demo_tree()
    root_handle = af.SupervisorHandle.start(root_spec)
    
    # Give time to initialize
    time.sleep(0.5)
    
    # Create monitor
    monitor = SupervisorMonitor(root_handle)
    
    try:
        # Run interactive loop
        interactive_loop(root_handle, monitor)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    finally:
        print("\nShutting down supervisor tree...")
        root_handle.shutdown()
        print("✓ Shutdown complete")


if __name__ == "__main__":
    main()
