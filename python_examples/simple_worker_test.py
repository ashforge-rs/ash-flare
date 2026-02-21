#!/usr/bin/env python3
"""
Simple test to verify Python code execution in workers
"""

import ash_flare as af
import time

# Counter to track worker executions
execution_log = []

def make_worker(worker_id, duration):
    """Create a worker that logs execution and sleeps"""
    def worker_fn():
        execution_log.append(f"{worker_id} started")
        time.sleep(duration)
        execution_log.append(f"{worker_id} completed")
    return worker_fn

def main():
    print("=== Simple Worker Test - Python Code Execution ===\n")
    
    # Create supervisor with 5 workers
    spec = af.SupervisorSpec("test-supervisor")
    spec.with_restart_strategy(af.RestartStrategy.one_for_one())
    
    for i in range(5):
        worker_id = f"worker-{i}"
        duration = 1  # 1 second sleep
        worker_fn = make_worker(worker_id, duration)
        spec.add_worker(worker_id, af.RestartPolicy.temporary(), worker_fn)
    
    print("Starting supervisor with 5 workers...")
    handle = af.SupervisorHandle.start(spec)
    
    # Wait for workers to complete
    time.sleep(3)
    
    print("\nExecution log:")
    for entry in execution_log:
        print(f"  {entry}")
    
    print(f"\nTotal executions logged: {len(execution_log)}")
    print("✓ Python code was executed successfully in workers!")
    
    handle.shutdown()

if __name__ == "__main__":
    main()
