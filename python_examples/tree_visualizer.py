#!/usr/bin/env python3
"""
Tree Visualization Example - Renders supervisor tree structure

This example demonstrates:
- Creating a nested supervisor tree
- Querying children at multiple levels
- Visualizing the tree structure with Unicode box-drawing characters
- Showing restart policies for workers

Python equivalent of: examples/tree_visualizer.rs
"""

import ash_flare as af
import time


def render_children(children, prefix="", is_last=True):
    """Recursively render children with tree structure"""
    for idx, child in enumerate(children):
        is_child_last = idx == len(children) - 1
        connector = "└──" if is_child_last else "├──"
        
        # Icon based on child type
        icon = "📁" if child.child_type.is_supervisor() else "⚙️"
        
        # Policy indicator
        policy = child.restart_policy
        if policy:
            policy_str = str(policy)
            if "permanent" in policy_str.lower():
                policy_icon = " ♻️"
            elif "temporary" in policy_str.lower():
                policy_icon = " ⏱️"
            elif "transient" in policy_str.lower():
                policy_icon = " 🔄"
            else:
                policy_icon = ""
        else:
            policy_icon = ""
        
        print(f"{prefix}{connector} {icon} {child.id}{policy_icon}")
        
        # If it's a supervisor, we'd need its handle to query children
        # (This is a limitation - we don't have recursive handles in this example)
        
        # Update prefix for nested children
        if not is_child_last:
            new_prefix = prefix + "│   "
        else:
            new_prefix = prefix + "    "


def render_supervisor_tree(root_handle):
    """Render the entire supervisor tree"""
    # Clear screen
    print("\033[2J\033[1;1H")
    
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║              SUPERVISOR TREE VISUALIZATION                        ║")
    print("╚═══════════════════════════════════════════════════════════════════╝\n")
    
    print("Legend: 📁 Supervisor  ⚙️  Worker  │  ♻️  Permanent  ⏱️  Temporary  🔄 Transient\n")
    
    # Render root
    print(f"🌳 {root_handle.name()} (root)")
    
    # Get root's children
    try:
        children = root_handle.which_children()
        render_children(children, "")
    except Exception as e:
        print(f"Error getting children: {e}")
    
    print("\n" + "═" * 70)
    print("Tree visualization complete!")


def build_example_tree():
    """Build a complex supervisor tree for demonstration"""
    
    # Dummy worker function (does nothing, just for tree structure)
    def dummy_worker():
        import time
        time.sleep(3600)  # Sleep for a long time
    
    # Create leaf supervisors with workers
    db_supervisor = af.SupervisorSpec("database-pool")
    db_supervisor.with_restart_strategy(af.RestartStrategy.one_for_all())
    db_supervisor.add_worker("db-conn-1", af.RestartPolicy.permanent(), dummy_worker)
    db_supervisor.add_worker("db-conn-2", af.RestartPolicy.permanent(), dummy_worker)
    db_supervisor.add_worker("db-conn-3", af.RestartPolicy.permanent(), dummy_worker)
    
    api_supervisor = af.SupervisorSpec("api-servers")
    api_supervisor.with_restart_strategy(af.RestartStrategy.one_for_one())
    api_supervisor.add_worker("api-1", af.RestartPolicy.permanent(), dummy_worker)
    api_supervisor.add_worker("api-2", af.RestartPolicy.permanent(), dummy_worker)
    api_supervisor.add_worker("api-health", af.RestartPolicy.transient(), dummy_worker)
    
    # Create root supervisor
    root_spec = af.SupervisorSpec("application")
    root_spec.with_restart_strategy(af.RestartStrategy.rest_for_one())
    root_spec.with_restart_intensity(af.RestartIntensity(5, 10))
    
    # Add workers to root
    root_spec.add_worker("config-loader", af.RestartPolicy.permanent(), dummy_worker)
    root_spec.add_worker("metrics-collector", af.RestartPolicy.transient(), dummy_worker)
    root_spec.add_worker("cache-warmer", af.RestartPolicy.temporary(), dummy_worker)
    
    # Add nested supervisors
    root_spec.add_supervisor(db_supervisor)
    root_spec.add_supervisor(api_supervisor)
    
    return root_spec


def main():
    print("=== Supervisor Tree Visualization ===\n")
    print("Building supervisor tree...\n")
    
    # Build and start the supervisor tree
    root_spec = build_example_tree()
    root_handle = af.SupervisorHandle.start(root_spec)
    
    # Give supervisors time to start
    time.sleep(0.5)
    
    # Render the tree
    render_supervisor_tree(root_handle)
    
    # Show some statistics
    print("\nSupervisor Statistics:")
    counts = root_handle.count_children()
    print(f"  Total Supervisors: {counts['supervisors']}")
    print(f"  Total Workers: {counts['workers']}")
    
    # Keep alive briefly then shutdown
    time.sleep(1)
    
    print("\nShutting down supervisor tree...")
    root_handle.shutdown()
    print("✓ Shutdown complete")


if __name__ == "__main__":
    main()
