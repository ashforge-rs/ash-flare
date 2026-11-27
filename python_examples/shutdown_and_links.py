#!/usr/bin/env python3
"""
Shutdown modes, events, links and auto shutdown from Python

This example demonstrates:
- add_worker(shutdown=...) with Shutdown.infinity(), Shutdown.timeout() and
  Shutdown.brutal_kill(), and with_shutdown_order()
- subscribe() and the SupervisorEvent stream
- link() between children of different supervisors, and kill_child()
- add_worker(significant=True) with with_auto_shutdown(), and
  with_restart_policy() on a nested supervisor

Python equivalent of: examples/graceful_shutdown.rs, examples/linked_services.rs
and examples/batch_job.rs
"""

import threading
import time

import ash_flare as af


def print_events(handle, prefix):
    """Prints a supervisor's events on a background thread until it stops."""
    stream = handle.subscribe()

    def run():
        for event in stream:
            print(f"  [{prefix}] {event!r}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def service(should_stop):
    """Runs until asked to stop."""
    while not should_stop():
        time.sleep(0.02)


def with_cleanup(name, seconds):
    """A service that needs `seconds` to clean up once asked to stop."""

    def worker(should_stop):
        service(should_stop)
        print(f"  {name}: cleaning up ({seconds}s needed)")
        time.sleep(seconds)
        print(f"  {name}: cleanup finished")

    return worker


def shutdown_modes():
    print("--- 1. shutdown modes (reverse order) ---")
    spec = af.SupervisorSpec("app")
    spec.with_shutdown_order(af.ShutdownOrder.reverse())
    spec.add_worker(
        "db-writer",
        af.RestartPolicy.permanent(),
        with_cleanup("db-writer", 0.6),
        shutdown=af.Shutdown.infinity(),
    )
    spec.add_worker(
        "http",
        af.RestartPolicy.permanent(),
        with_cleanup("http", 1.5),
        shutdown=af.Shutdown.timeout(0.3),
    )
    spec.add_worker(
        "metrics",
        af.RestartPolicy.permanent(),
        service,
        shutdown=af.Shutdown.brutal_kill(),
    )

    handle = af.SupervisorHandle.start(spec)
    watcher = print_events(handle, "app")
    time.sleep(0.2)

    started = time.monotonic()
    handle.shutdown()  # blocks until the tree has stopped
    print(f"  shutdown returned after {time.monotonic() - started:.1f}s")
    watcher.join()
    # The abandoned http thread is still sleeping; let it finish quietly.
    time.sleep(1.5)


def links():
    print("\n--- 2. a link between two supervisors ---")
    db_spec = af.SupervisorSpec("db")
    db_spec.with_restart_delay(0.1)
    db_spec.add_worker("pool", af.RestartPolicy.permanent(), service)

    web_spec = af.SupervisorSpec("web")
    web_spec.with_restart_delay(0.1)
    web_spec.add_worker("cache", af.RestartPolicy.permanent(), service)
    web_spec.add_worker("api", af.RestartPolicy.permanent(), service)

    db = af.SupervisorHandle.start(db_spec)
    web = af.SupervisorHandle.start(web_spec)
    watchers = [print_events(db, "db"), print_events(web, "web")]

    pool_cache = af.link(db, "pool", web, "cache")
    print(f"  linked db/pool <-> web/cache (active: {pool_cache.is_active()})")
    print("  killing db/pool; the cache goes down with it, the api does not")
    db.kill_child("pool")
    time.sleep(0.5)

    pool_cache.unlink()
    print("  unlinked; killing db/pool again")
    db.kill_child("pool")
    time.sleep(0.5)

    web.shutdown()
    db.shutdown()
    for watcher in watchers:
        watcher.join()


def auto_shutdown():
    print("\n--- 3. a job that stops itself when its significant child is done ---")

    def importer():
        for batch in range(1, 4):
            time.sleep(0.15)
            print(f"  importer: batch {batch}/3")

    job = af.SupervisorSpec("import-job")
    job.with_auto_shutdown(af.AutoShutdown.any_significant())
    job.with_restart_policy(af.RestartPolicy.transient())
    job.add_worker("importer", af.RestartPolicy.transient(), importer, significant=True)
    job.add_worker("progress", af.RestartPolicy.permanent(), service)

    app = af.SupervisorSpec("app")
    app.add_worker("api", af.RestartPolicy.permanent(), service)
    app.add_supervisor(job)

    handle = af.SupervisorHandle.start(app)
    events = handle.subscribe()
    while True:
        event = events.recv(timeout=5)
        if event is None:
            break
        if event.kind == "child_terminated" and event.child_id == "import-job":
            print(f"  [app] {event!r}")
            break

    time.sleep(0.3)  # a Transient job is not started again
    print(f"  [app] children now: {[child.id for child in handle.which_children()]}")
    handle.shutdown()


def main():
    print("=== Shutdown, links and auto shutdown ===\n")
    shutdown_modes()
    links()
    auto_shutdown()
    print("\nDone.")


if __name__ == "__main__":
    main()
