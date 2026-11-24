"""Tests for supervision features exposed to Python: child options, events,
killing and linking children, generated ids, auto shutdown, and the number of
workers a process can run.

Runnable directly (``python python/tests/test_supervision_features.py``) as
well as under pytest.
"""

import threading
import time

from ash_flare import (
    AutoShutdown,
    RemoteSupervisorHandle,
    RestartPolicy,
    Shutdown,
    ShutdownOrder,
    SupervisorHandle,
    SupervisorSpec,
    link,
)


def idle(should_stop):
    while not should_stop():
        time.sleep(0.01)


class Counter:
    """Counts calls of a worker that idles until stopped."""

    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, should_stop):
        with self._lock:
            self.calls += 1
        idle(should_stop)


def test_more_workers_than_the_blocking_pool_holds():
    """Each Python worker has its own thread; tokio's blocking pool of 512
    threads used to leave every worker beyond it waiting, silently."""
    started = threading.Semaphore(0)

    def worker(should_stop):
        started.release()
        idle(should_stop)

    spec = SupervisorSpec("many")
    for i in range(600):
        spec.add_worker(f"w{i}", RestartPolicy.permanent(), worker)
    handle = SupervisorHandle.start(spec)

    deadline = time.monotonic() + 10
    for _ in range(600):
        assert started.acquire(timeout=max(0.0, deadline - time.monotonic())), "a worker never started"
    handle.shutdown()


def test_events_report_a_kill_and_the_restart():
    spec = SupervisorSpec("events")
    spec.add_worker("w", RestartPolicy.permanent(), idle)
    handle = SupervisorHandle.start(spec)
    events = handle.subscribe()
    time.sleep(0.05)

    assert handle.kill_child("w") is True
    terminated = events.recv(timeout=1)
    restarted = events.recv(timeout=1)
    handle.shutdown()

    assert terminated.kind == "child_terminated"
    assert terminated.child_id == "w"
    assert terminated.reason.is_killed()
    assert restarted.kind == "child_restarted"


def test_unique_children_get_generated_ids():
    handle = SupervisorHandle.start(SupervisorSpec("unique"))
    ids = {handle.start_unique_child("conn", RestartPolicy.temporary(), idle) for _ in range(5)}
    handle.shutdown()

    assert len(ids) == 5
    assert all(i.startswith("conn-") for i in ids)


def test_a_significant_child_shuts_its_supervisor_down():
    spec = SupervisorSpec("batch")
    spec.with_auto_shutdown(AutoShutdown.any_significant())
    spec.add_worker("job", RestartPolicy.transient(), lambda: time.sleep(0.05), significant=True)
    spec.add_worker("service", RestartPolicy.permanent(), idle)
    handle = SupervisorHandle.start(spec)

    kinds = [event.kind for event in handle.subscribe()]
    assert kinds[-1] == "stopped", kinds


def test_an_invalid_spec_is_rejected_at_start():
    spec = SupervisorSpec("invalid")
    spec.add_worker("job", RestartPolicy.permanent(), idle, significant=True)
    try:
        SupervisorHandle.start(spec)
    except ValueError:
        return
    raise AssertionError("a permanent significant child must be rejected")


def test_linked_children_share_their_fate():
    a_calls, b_calls = Counter(), Counter()
    a_spec = SupervisorSpec("a")
    a_spec.add_worker("a", RestartPolicy.permanent(), a_calls)
    b_spec = SupervisorSpec("b")
    b_spec.add_worker("b", RestartPolicy.permanent(), b_calls)
    a, b = SupervisorHandle.start(a_spec), SupervisorHandle.start(b_spec)

    linked = link(a, "a", b, "b")
    time.sleep(0.05)
    a.kill_child("a")
    time.sleep(0.3)
    assert (a_calls.calls, b_calls.calls) == (2, 2)

    linked.unlink()
    a.kill_child("a")
    time.sleep(0.3)
    assert (a_calls.calls, b_calls.calls) == (3, 2), "no propagation once unlinked"

    a.shutdown()
    b.shutdown()


def test_child_shutdown_options_are_accepted():
    spec = SupervisorSpec("options")
    spec.with_shutdown_order(ShutdownOrder.concurrent())
    spec.add_worker("killed", RestartPolicy.permanent(), idle, shutdown=Shutdown.brutal_kill())
    spec.add_worker("patient", RestartPolicy.permanent(), idle, shutdown=Shutdown.timeout(2.0))
    spec.add_worker("forever", RestartPolicy.permanent(), idle, shutdown=Shutdown.infinity())
    handle = SupervisorHandle.start(spec)
    time.sleep(0.05)

    began = time.monotonic()
    handle.shutdown()
    assert time.monotonic() - began < 2.0


def test_remote_handle_accepts_a_cookie():
    remote = RemoteSupervisorHandle.connect_unix("/nonexistent/ash-flare.sock", cookie="s3cret")
    try:
        remote.status()
    except RuntimeError:
        return
    raise AssertionError("connecting to a missing socket must fail")


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"✓ {test.__name__}")
    print(f"✓ {len(tests)} supervision feature tests passed")
