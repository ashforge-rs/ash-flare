#!/usr/bin/env python3
"""
Many short-lived workers under generated ids

This example demonstrates:
- start_unique_child(), which starts a worker under an id "{prefix}-{n}" that
  the supervisor picks, like a child of OTP's simple_one_for_one supervisor
- Temporary workers that leave the tree when they finish
- Each Python worker running on its own thread: far more than 512 workers can
  block at the same time

Python equivalent of: examples/connection_handlers.rs
"""

import threading
import time

import ash_flare as af

WORKERS = 1000


def main():
    print("=== Unique children ===\n")

    handle = af.SupervisorHandle.start(af.SupervisorSpec("jobs"))
    release = threading.Event()
    running = 0
    lock = threading.Lock()

    def job():
        nonlocal running
        with lock:
            running += 1
        release.wait()  # blocks its thread until released

    started = time.monotonic()
    ids = [
        handle.start_unique_child("job", af.RestartPolicy.temporary(), job)
        for _ in range(WORKERS)
    ]
    print(f"started {len(ids)} workers in {time.monotonic() - started:.2f}s")
    print(f"generated ids: {ids[0]}, {ids[1]}, ..., {ids[-1]}")

    deadline = time.monotonic() + 10
    while running < WORKERS and time.monotonic() < deadline:
        time.sleep(0.05)
    print(f"blocked at the same time: {running}")
    print(f"children in the tree: {len(handle.which_children())}")

    release.set()
    deadline = time.monotonic() + 10
    while handle.which_children() and time.monotonic() < deadline:
        time.sleep(0.05)
    print(f"after release, children in the tree: {len(handle.which_children())}")

    handle.shutdown()
    print("\nDone.")


if __name__ == "__main__":
    main()
