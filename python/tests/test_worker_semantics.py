"""Regression tests for the Python worker bindings.

Runnable directly (``python python/tests/test_worker_semantics.py``) as well as
under pytest, so ``make python-test`` needs no extra dependency.

Each test here covers behaviour that is invisible from Rust: how a Python
callable's signature is chosen, what happens to a callable that is still running
when the supervisor stops it, and what an out-of-range timeout does.
"""

import time

from ash_flare import RestartPolicy, SupervisorHandle, SupervisorSpec


def test_invalid_durations_are_rejected():
    """A negative or non-finite timeout used to become ``Duration::ZERO``,
    silently turning into "abort every worker immediately, no cleanup"."""
    spec = SupervisorSpec("durations")

    for call in (
        lambda: spec.with_shutdown_timeout(-1.0),
        lambda: spec.with_shutdown_timeout(float("nan")),
        lambda: spec.with_restart_delay(-0.5),
        lambda: spec.with_restart_backoff(0.1, float("inf")),
    ):
        try:
            call()
        except ValueError:
            continue
        raise AssertionError("an out-of-range duration must raise ValueError")

    # Valid values still work.
    spec.with_shutdown_timeout(0.5)
    spec.with_restart_delay(0.01)
    spec.with_restart_backoff(0.01, 0.05)


def _needs_an_argument(a):
    return a


def test_a_typeerror_from_the_worker_body_is_not_retried():
    """The worker body must run once. It used to be retried with fewer
    arguments whenever its TypeError happened to read like an arity mismatch,
    running any side effects a second time and hiding the real exception."""
    runs = []

    def worker(should_stop):
        runs.append(1)
        _needs_an_argument()  # TypeError worded exactly like an arity error

    spec = SupervisorSpec("body-typeerror")
    spec.with_restart_delay(5.0)  # do not crash-loop during the test
    spec.add_worker("boom", RestartPolicy.temporary(), worker)

    handle = SupervisorHandle.start(spec)
    time.sleep(0.6)
    handle.shutdown()

    assert len(runs) == 1, f"worker body ran {len(runs)} times, expected once"


def test_each_supported_signature_is_called():
    """Workers may take nothing, a ``should_stop`` handle, or ``*args``."""
    seen = {}

    def zero_arg():
        seen["zero"] = True

    def one_arg(should_stop):
        seen["one"] = should_stop.is_set() is False

    def star_args(*args):
        seen["star"] = len(args)

    def keyword_only(should_stop, *, unused=1):
        seen["kwonly"] = True

    spec = SupervisorSpec("signatures")
    spec.with_restart_delay(5.0)
    for name, fn in (
        ("zero", zero_arg),
        ("one", one_arg),
        ("star", star_args),
        ("kwonly", keyword_only),
    ):
        spec.add_worker(name, RestartPolicy.temporary(), fn)

    handle = SupervisorHandle.start(spec)
    time.sleep(0.6)
    handle.shutdown()

    assert seen.get("zero") is True
    assert seen.get("one") is True
    assert seen.get("star") == 1, "a *args worker receives the should_stop handle"
    assert seen.get("kwonly") is True, "keyword-only parameters do not count as positional"


def test_stopping_waits_for_the_python_body():
    """``spawn_blocking`` cannot be cancelled, so stopping a worker has to wait
    for the callable to observe ``should_stop`` and return. Reporting a
    graceful stop early left the callable running while its replacement
    started on top of it."""
    state = {"finished": False}

    def cooperative(should_stop):
        while not should_stop():
            time.sleep(0.02)
        time.sleep(0.15)  # cleanup that must not be abandoned
        state["finished"] = True

    spec = SupervisorSpec("cooperative")
    spec.with_shutdown_timeout(3.0)
    spec.add_worker("coop", RestartPolicy.permanent(), cooperative)

    handle = SupervisorHandle.start(spec)
    time.sleep(0.3)
    handle.terminate_child("coop")

    assert state["finished"] is True, "terminate_child returned before the body finished"
    handle.shutdown()


def test_an_uncooperative_worker_is_bounded_by_the_shutdown_timeout():
    """Waiting for the body must not mean waiting forever."""

    def stubborn(should_stop):
        time.sleep(5)

    spec = SupervisorSpec("stubborn")
    spec.with_shutdown_timeout(0.3)
    spec.add_worker("stubborn", RestartPolicy.permanent(), stubborn)

    handle = SupervisorHandle.start(spec)
    time.sleep(0.3)

    began = time.monotonic()
    handle.terminate_child("stubborn")
    elapsed = time.monotonic() - began
    handle.shutdown()

    assert elapsed < 2.0, f"abandoning the worker took {elapsed:.2f}s, timeout was 0.3s"


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print(f"✓ {test.__name__}")
    print(f"✓ {len(tests)} Python binding tests passed")
