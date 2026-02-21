#!/usr/bin/env python3
"""
Async Worker-to-Worker Communication Example

This example demonstrates:
- Direct communication between workers using mailboxes with async/await
- Worker A sends tasks to Worker B asynchronously
- Worker B processes and sends results back to Worker A asynchronously
- Peer-to-peer messaging pattern using Python asyncio
- Wrapping blocking Rust calls with asyncio.to_thread()

Python async equivalent of: examples/mailbox_p2p.rs

This showcases:
- Using asyncio.to_thread() for blocking send/recv operations
- Concurrent async workers communicating via mailboxes
- Graceful async task management
- Proper async Python patterns with Rust bindings
"""

import ash_flare as af
import asyncio
from typing import Optional


class AsyncWorkerA:
    """
    Worker A - Sends tasks and receives results asynchronously
    """
    
    def __init__(self, mailbox: af.Mailbox, handle: af.MailboxHandle, worker_b_handle: af.MailboxHandle):
        self.mailbox = mailbox
        self.handle = handle
        self.worker_b_handle = worker_b_handle
        self.received_count = 0
        self.running = True
        
    async def run(self, tasks: list[str]):
        """Main worker loop - sends tasks and receives results"""
        print(f"[Worker A] Started ({self.handle.worker_id()})")
        
        # Create tasks for sending and receiving concurrently
        send_task = asyncio.create_task(self._send_tasks(tasks))
        recv_task = asyncio.create_task(self._receive_results())
        
        # Wait for sending to complete
        await send_task
        
        # Give Worker B time to process and send results back
        await asyncio.sleep(1.0)
        
        # Stop receiving
        self.running = False
        await recv_task
        
        print(f"[Worker A] Finished - received {self.received_count} results")
        
    async def _send_tasks(self, tasks: list[str]):
        """Forward tasks to Worker B"""
        print(f"[Worker A] Forwarding {len(tasks)} tasks to Worker B...")
        for task in tasks:
            try:
                # Use asyncio.to_thread to wrap the blocking send
                await asyncio.to_thread(self.worker_b_handle.send, f"process:{task}")
                print(f"[Worker A] → Sent to Worker B: {task}")
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"[Worker A] ✗ Error sending: {e}")
                
    async def _receive_results(self):
        """Receive results from Worker B"""
        while self.running:
            try:
                # Use try_recv instead of blocking recv
                msg = self.mailbox.try_recv()
                if msg.startswith("result:"):
                    print(f"[Worker A] ✓ Received result: {msg}")
                    self.received_count += 1
            except Exception:
                # No message available, wait a bit
                await asyncio.sleep(0.05)
                continue


class AsyncWorkerB:
    """
    Worker B - Processes tasks and sends results asynchronously
    """
    
    def __init__(self, mailbox: af.Mailbox, handle: af.MailboxHandle, worker_a_handle: af.MailboxHandle):
        self.mailbox = mailbox
        self.handle = handle
        self.worker_a_handle = worker_a_handle
        self.processed_count = 0
        
    async def run(self, expected_tasks: int):
        """Main worker loop - receives tasks and sends results"""
        print(f"[Worker B] Started ({self.handle.worker_id()})")
        
        processed = 0
        attempts = 0
        max_attempts = 100  # Prevent infinite loop
        
        while processed < expected_tasks and attempts < max_attempts:
            attempts += 1
            try:
                # Use try_recv instead of blocking recv
                msg = self.mailbox.try_recv()
                
                if msg.startswith("process:"):
                    task = msg.replace("process:", "", 1)
                    print(f"[Worker B] Processing: {task}")
                    
                    # Simulate async work
                    await asyncio.sleep(0.1)
                    
                    result = f"result: completed '{task}'"
                    
                    # Use asyncio.to_thread to wrap the blocking send
                    await asyncio.to_thread(self.worker_a_handle.send, result)
                    print(f"[Worker B] ✓ Sent result to Worker A")
                    
                    processed += 1
                    self.processed_count += 1
                    
            except Exception:
                # No message available, wait a bit
                await asyncio.sleep(0.05)
                
        print(f"[Worker B] Finished - processed {self.processed_count} tasks")


async def main():
    print("=== Async Worker-to-Worker Communication Example ===\n")
    
    # Create mailboxes for both workers
    config = af.MailboxConfig.bounded(10)
    handle_a, mailbox_a = af.mailbox_named("async-worker-a", config)
    handle_b, mailbox_b = af.mailbox_named("async-worker-b", config)
    
    print(f"✓ Created mailbox for {handle_a.worker_id()}")
    print(f"✓ Created mailbox for {handle_b.worker_id()}\n")
    
    # Define tasks
    tasks = [
        "task-1: Calculate metrics",
        "task-2: Update database",
        "task-3: Send notifications",
        "task-4: Clean up cache",
        "task-5: Generate report"
    ]
    
    # Create workers
    worker_a = AsyncWorkerA(mailbox_a, handle_a, handle_b)
    worker_b = AsyncWorkerB(mailbox_b, handle_b, handle_a)
    
    # Run both workers concurrently
    print("Starting async workers...\n")
    
    await asyncio.gather(
        worker_a.run(tasks),
        worker_b.run(len(tasks))
    )
    
    # Summary
    print("\n" + "=" * 60)
    print("Communication Summary:")
    print(f"  Tasks sent: {len(tasks)}")
    print(f"  Tasks processed by Worker B: {worker_b.processed_count}")
    print(f"  Results received by Worker A: {worker_a.received_count}")
    print(f"  Worker A mailbox open: {handle_a.is_open()}")
    print(f"  Worker B mailbox open: {handle_b.is_open()}")
    print("=" * 60)
    
    print("\n✓ Async example complete!")


if __name__ == "__main__":
    asyncio.run(main())
