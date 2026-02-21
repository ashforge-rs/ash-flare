#!/usr/bin/env python3
"""
Worker Countdown Example - Spawn 1000 workers with random countdown times

This example demonstrates:
- Spawning a large number of workers (1000)
- Each worker counts for 3 + random(0-9) seconds (3-12 seconds total)
- Workers execute actual Python code (sleep for the specified duration)
- Workers shutdown automatically when their countdown completes
- Real-time monitoring of worker progress
- Statistics tracking
"""

import ash_flare as af
import time
import random
from datetime import datetime


def make_countdown_worker(seconds):
    """Create a worker function that sleeps for the specified number of seconds"""
    def worker_fn():
        time.sleep(seconds)
    return worker_fn


def main():
    print("=" * 70)
    print("  Worker Countdown Example - 1000 workers")
    print("=" * 70)
    print()
    
    # Create supervisor spec
    spec = af.SupervisorSpec("countdown-supervisor")
    spec.with_restart_strategy(af.RestartStrategy.one_for_one())
    
    # Track worker durations for statistics
    worker_durations = {}
    
    print("Setting up 1000 workers with random countdown times...")
    start_setup = time.time()
    
    # Add 1000 workers, each with a random countdown duration (3-12 seconds)
    for i in range(1000):
        worker_id = f"worker-{i:04d}"
        # Random countdown time: 3 + random(0-9) seconds
        countdown_seconds = 3 + random.randint(0, 9)
        worker_durations[worker_id] = countdown_seconds
        
        # Create worker function that sleeps for the countdown duration
        worker_fn = make_countdown_worker(countdown_seconds)
        
        # Add worker with Temporary restart policy (won't restart after completion)
        spec.add_worker(worker_id, af.RestartPolicy.temporary(), worker_fn)
        
        # Progress indicator every 100 workers
        if (i + 1) % 100 == 0:
            print(f"  Added {i + 1}/1000 workers...")
    
    setup_time = time.time() - start_setup
    print(f"✓ Setup complete in {setup_time:.2f}s")
    print()
    
    # Calculate statistics
    durations = list(worker_durations.values())
    avg_duration = sum(durations) / len(durations)
    min_duration = min(durations)
    max_duration = max(durations)
    
    print(f"Worker duration statistics:")
    print(f"  Average: {avg_duration:.2f}s")
    print(f"  Minimum: {min_duration}s")
    print(f"  Maximum: {max_duration}s")
    print()
    
    # Start the supervisor
    print("Starting supervisor...")
    start_time = time.time()
    handle = af.SupervisorHandle.start(spec)
    print(f"✓ Supervisor '{handle.name()}' started")
    print()
    
    # Monitor workers
    print("Monitoring workers (press Ctrl+C to shutdown early)...")
    print("-" * 70)
    
    try:
        last_count = 1000
        check_interval = 1.0
        
        while True:
            time.sleep(check_interval)
            
            try:
                counts = handle.count_children()
                current_workers = counts['workers']
                elapsed = time.time() - start_time
                completed = 1000 - current_workers
                
                # Only print when count changes
                if current_workers != last_count:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    completion_rate = (completed / 1000) * 100
                    
                    print(f"[{timestamp}] Elapsed: {elapsed:6.1f}s | "
                          f"Active: {current_workers:4d} | "
                          f"Completed: {completed:4d} ({completion_rate:5.1f}%)")
                    
                    last_count = current_workers
                
                # All workers completed
                if current_workers == 0:
                    print("-" * 70)
                    print("✓ All workers completed!")
                    break
                    
            except Exception as e:
                print(f"Error querying supervisor: {e}")
                break
        
    except KeyboardInterrupt:
        print("\n" + "-" * 70)
        print("Interrupted by user")
    
    # Final statistics
    total_time = time.time() - start_time
    print()
    print("=" * 70)
    print("Final Statistics:")
    print(f"  Total workers spawned: 1000")
    print(f"  Total runtime: {total_time:.2f}s")
    print(f"  Expected max runtime: ~{max_duration}s")
    print("=" * 70)
    
    # Shutdown
    print()
    print("Shutting down supervisor...")
    try:
        handle.shutdown()
        print("✓ Supervisor shutdown complete")
    except Exception as e:
        print(f"Error during shutdown: {e}")


if __name__ == "__main__":
    main()
