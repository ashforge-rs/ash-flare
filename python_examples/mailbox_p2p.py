#!/usr/bin/env python3
"""
Worker-to-Worker Communication Example

This example demonstrates:
- Direct communication between workers using mailboxes
- Worker A sends tasks to Worker B
- Worker B processes and sends results back to Worker A
- Peer-to-peer messaging pattern

Python equivalent of: examples/mailbox_p2p.rs

Note: Since Python bindings use placeholder workers, this example
demonstrates the mailbox API without actual worker integration.
In production, you'd create custom Python workers that use mailboxes.
"""

import ash_flare as af
import time
import asyncio


def simulate_worker_a(mailbox, worker_b_handle):
    """
    Worker A - Sends tasks and receives results
    
    In a real implementation, this would be integrated into a Worker class.
    Here we simulate the pattern.
    """
    print("[Worker A] Started")
    received_count = 0
    
    for _ in range(5):
        try:
            # Try to receive messages (non-blocking)
            msg = mailbox.try_recv()
            print(f"[Worker A] Received: {msg}")
            received_count += 1
            
            # Check for results from Worker B
            if msg.startswith("result:"):
                print(f"[Worker A] ✓ Got result from Worker B: {msg}")
        except Exception:
            # No message available
            pass
        
        time.sleep(0.1)
    
    print(f"[Worker A] Received {received_count} messages")


def simulate_worker_b(mailbox, worker_a_handle):
    """
    Worker B - Processes tasks and sends results
    
    In a real implementation, this would be integrated into a Worker class.
    Here we simulate the pattern.
    """
    print("[Worker B] Started")
    processed_count = 0
    
    for _ in range(5):
        try:
            # Try to receive tasks (non-blocking)
            msg = mailbox.try_recv()
            print(f"[Worker B] Received: {msg}")
            
            # Process tasks from Worker A
            if msg.startswith("process:"):
                task = msg.replace("process:", "", 1)
                
                # Simulate processing
                time.sleep(0.1)
                
                result = f"result: completed '{task}'"
                print(f"[Worker B] Sending result to Worker A: {result}")
                
                try:
                    worker_a_handle.try_send(result)
                    processed_count += 1
                except Exception as e:
                    print(f"[Worker B] Error sending to Worker A: {e}")
        except Exception:
            # No message available
            pass
        
        time.sleep(0.1)
    
    print(f"[Worker B] Processed {processed_count} tasks")


def main():
    print("=== Worker-to-Worker Communication Example ===\n")
    
    # Create mailboxes for both workers
    config = af.MailboxConfig.bounded(10)
    handle_a, mailbox_a = af.mailbox_named("worker-a", config)
    handle_b, mailbox_b = af.mailbox_named("worker-b", config)
    
    print(f"✓ Created mailbox for {handle_a.worker_id()}")
    print(f"✓ Created mailbox for {handle_b.worker_id()}\n")
    
    # Send initial tasks to Worker A (which will forward to Worker B)
    print("Sending tasks to Worker A...\n")
    
    tasks = [
        "task-1: Calculate metrics",
        "task-2: Update database",
        "task-3: Send notifications",
        "task-4: Clean up cache",
        "task-5: Generate report"
    ]
    
    for task in tasks:
        try:
            handle_a.try_send(task)
            print(f"  → Sent to Worker A: {task}")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        time.sleep(0.1)
    
    print("\nWorker A forwarding tasks to Worker B...\n")
    time.sleep(0.2)
    
    # Process messages in Worker A
    for _ in range(len(tasks)):
        try:
            msg = mailbox_a.try_recv()
            # Forward to Worker B
            forward_msg = f"process:{msg}"
            handle_b.try_send(forward_msg)
            print(f"[Worker A] Forwarding to Worker B: {forward_msg}")
        except Exception as e:
            pass
        time.sleep(0.1)
    
    print("\nWorker B processing and sending results...\n")
    time.sleep(0.2)
    
    # Process tasks in Worker B and send results back
    processed = 0
    for _ in range(len(tasks)):
        try:
            msg = mailbox_b.try_recv()
            if msg.startswith("process:"):
                task = msg.replace("process:", "", 1)
                # Simulate processing
                time.sleep(0.1)
                result = f"result: completed '{task}'"
                handle_a.try_send(result)
                print(f"[Worker B] Completed task, sent: {result}")
                processed += 1
        except Exception as e:
            pass
        time.sleep(0.1)
    
    print("\nWorker A receiving results...\n")
    time.sleep(0.2)
    
    # Worker A receives results
    results = 0
    for _ in range(processed):
        try:
            msg = mailbox_a.try_recv()
            print(f"[Worker A] ✓ Received result: {msg}")
            results += 1
        except Exception as e:
            pass
        time.sleep(0.1)
    
    # Summary
    print("\n" + "=" * 60)
    print("Communication Summary:")
    print(f"  Tasks sent: {len(tasks)}")
    print(f"  Tasks processed: {processed}")
    print(f"  Results received: {results}")
    print(f"  Worker A mailbox open: {handle_a.is_open()}")
    print(f"  Worker B mailbox open: {handle_b.is_open()}")
    print("=" * 60)
    
    print("\n✓ Example complete!")


if __name__ == "__main__":
    main()
