"""Type stubs for ash-flare.

Fault-tolerant supervision trees, inspired by Erlang/OTP.

Worker callables are invoked with a signature that ash-flare adapts to:

- ``def worker() -> None`` — simplest form.
- ``def worker(should_stop: ShouldStop) -> None`` — can stop cooperatively.

Stateful supervisors additionally pass their shared context first:

- ``def worker(ctx: WorkerContext) -> None``
- ``def worker(ctx: WorkerContext, should_stop: ShouldStop) -> None``
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

version: str
__version__: str

class RestartPolicy:
    """When a terminated child should be restarted."""

    def __init__(self, policy: str) -> None: ...
    @staticmethod
    def permanent() -> RestartPolicy:
        """Always restart, however the child exited."""

    @staticmethod
    def temporary() -> RestartPolicy:
        """Never restart."""

    @staticmethod
    def transient() -> RestartPolicy:
        """Restart only after an abnormal exit."""

class RestartStrategy:
    """Which children are restarted when one fails."""

    def __init__(self, strategy: str) -> None: ...
    @staticmethod
    def one_for_one() -> RestartStrategy:
        """Restart only the failed child."""

    @staticmethod
    def one_for_all() -> RestartStrategy:
        """Restart every child."""

    @staticmethod
    def rest_for_one() -> RestartStrategy:
        """Restart the failed child and everything started after it."""

class RestartIntensity:
    """Restart limit: at most ``max_restarts`` within ``within_seconds``."""

    def __init__(self, max_restarts: int, within_seconds: int) -> None: ...

class ChildType:
    """Whether a child is a worker or a nested supervisor."""

    def is_worker(self) -> bool: ...
    def is_supervisor(self) -> bool: ...

class ChildExitReason:
    """Why a child stopped."""

    def is_normal(self) -> bool: ...
    def is_abnormal(self) -> bool: ...
    def is_shutdown(self) -> bool: ...
    def is_killed(self) -> bool:
        """Stopped by being aborted: killed, or past its shutdown timeout."""

class ChildInfo:
    """A child's registered identity and policy."""

    id: str
    child_type: ChildType
    restart_policy: Optional[RestartPolicy]

class ShouldStop:
    """Cooperative shutdown flag handed to a worker callable.

    Poll it inside long-running loops and return promptly once it is set;
    a worker that ignores it is abandoned when the shutdown timeout elapses.
    """

    def is_set(self) -> bool: ...
    def __call__(self) -> bool: ...
    def __bool__(self) -> bool: ...

class WorkerContext:
    """Shared key-value store for workers of a stateful supervisor."""

    def __init__(self) -> None: ...
    def get(self, key: str) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...
    def delete(self, key: str) -> Any: ...
    def contains_key(self, key: str) -> bool: ...
    def len(self) -> int: ...
    def is_empty(self) -> bool: ...

class Shutdown:
    """How a child is stopped when its supervisor terminates it."""

    @staticmethod
    def brutal_kill() -> Shutdown:
        """Abandon the worker at once."""

    @staticmethod
    def timeout(seconds: float) -> Shutdown:
        """Ask the worker to stop and wait up to ``seconds``."""

    @staticmethod
    def infinity() -> Shutdown:
        """Ask the worker to stop and wait however long it takes."""

class ShutdownOrder:
    """Order in which children stopped together are stopped."""

    @staticmethod
    def reverse() -> ShutdownOrder:
        """One at a time, last started first (default)."""

    @staticmethod
    def concurrent() -> ShutdownOrder: ...

class AutoShutdown:
    """When a supervisor stops itself because its significant children ended."""

    @staticmethod
    def never() -> AutoShutdown: ...
    @staticmethod
    def any_significant() -> AutoShutdown: ...
    @staticmethod
    def all_significant() -> AutoShutdown: ...

class GiveUp:
    """What a top-level supervisor does when it gives up."""

    @staticmethod
    def stop() -> GiveUp:
        """Stop; ``SupervisorHandle.wait()`` reports it (the default)."""
    @staticmethod
    def exit_process(code: int) -> GiveUp:
        """Exit the process with ``code`` once the tree has shut down.

        The interpreter does not clean up (no ``finally`` blocks or ``atexit``
        handlers run). To clean up first, keep the default and call
        ``sys.exit`` after ``SupervisorHandle.wait()`` returns.
        """

class SupervisorEvent:
    """Something that happened in a supervisor.

    ``kind`` is ``"child_started"``, ``"child_restarted"``,
    ``"child_terminated"``, ``"child_stalled"``,
    ``"restart_intensity_exceeded"`` or ``"stopped"``.
    """

    kind: str
    child_id: Optional[str]
    reason: Optional[ChildExitReason]

class EventStream:
    """A supervisor's events. Iteration ends after the ``stopped`` event.

    No event is ever missed: events queue until read, so read the stream or
    drop it.
    """

    def recv(self, timeout: Optional[float] = None) -> Optional[SupervisorEvent]:
        """Next event, or ``None`` after ``timeout`` seconds."""

    def __iter__(self) -> Iterator[SupervisorEvent]: ...
    def __next__(self) -> SupervisorEvent: ...

class Link:
    """Keeps two children linked until dropped or unlinked."""

    def unlink(self) -> None: ...
    def is_active(self) -> bool: ...

def link(
    a: SupervisorHandle | StatefulSupervisorHandle,
    a_child: str,
    b: SupervisorHandle | StatefulSupervisorHandle,
    b_child: str,
) -> Link:
    """Link two children: when either ends other than normally, the other is
    killed and restarted by its supervisor."""

class MailboxConfig:
    """Capacity configuration for a mailbox."""

    @staticmethod
    def unbounded() -> MailboxConfig: ...
    @staticmethod
    def bounded(capacity: int) -> MailboxConfig: ...

class MailboxHandle:
    """Sending half of a mailbox."""

    def send(self, message: str) -> None:
        """Send, waiting for capacity. Releases the GIL while waiting."""

    def try_send(self, message: str) -> None:
        """Send without waiting; raises if the mailbox is full or closed."""

    def worker_id(self) -> str: ...
    def is_open(self) -> bool: ...

class Mailbox:
    """Receiving half of a mailbox."""

    def recv(self) -> Optional[str]:
        """Block until a message arrives, or ``None`` once all senders drop.

        Releases the GIL while waiting, so other Python threads keep running.
        """

    def try_recv(self) -> str:
        """Return a queued message; raises if none is available."""

def mailbox(config: MailboxConfig) -> Tuple[MailboxHandle, Mailbox]: ...
def mailbox_named(
    worker_id: str, config: MailboxConfig
) -> Tuple[MailboxHandle, Mailbox]: ...

class SupervisorSpec:
    """Describes a supervision tree of stateless workers."""

    def __init__(self, name: str) -> None: ...
    def with_restart_strategy(self, strategy: RestartStrategy) -> None: ...
    def with_restart_intensity(self, intensity: RestartIntensity) -> None: ...
    def with_restart_delay(self, seconds: float) -> None:
        """Fixed delay between a child terminating and being restarted."""

    def with_restart_backoff(
        self, initial_seconds: float, max_seconds: float
    ) -> None:
        """Exponential restart backoff, doubling up to ``max_seconds``."""

    def with_shutdown_timeout(self, seconds: float) -> None:
        """How long a worker may take to stop before it is abandoned."""

    def with_shutdown_order(self, order: ShutdownOrder) -> None: ...
    def with_auto_shutdown(self, auto_shutdown: AutoShutdown) -> None: ...
    def with_restart_policy(self, policy: RestartPolicy) -> None:
        """How this supervisor is restarted when nested in another."""

    def with_give_up(self, give_up: GiveUp) -> None:
        """What a top-level supervisor does when it gives up."""

    def with_registered_name(self, name: str) -> None:
        """Register each run under ``name``; see ``SupervisorHandle.whereis``."""

    def add_worker(
        self,
        id: str,
        restart_policy: RestartPolicy,
        worker_fn: Callable[..., Any],
        *,
        shutdown: Optional[Shutdown] = None,
        significant: bool = False,
    ) -> None:
        """Register a worker. Raises ``TypeError`` if ``worker_fn`` is not
        callable, or ``ValueError`` if ``id`` is already registered."""

    def add_supervisor(self, supervisor: SupervisorSpec) -> None: ...

class SupervisorHandle:
    """Controls a running supervision tree."""

    @staticmethod
    def whereis(name: str) -> Optional[SupervisorHandle]:
        """The current run of the supervisor registered under ``name``.

        Unlike a handle, which refers to one run and stops working when that
        run stops, a name always leads to the run that is up.
        """

    @staticmethod
    def start(spec: SupervisorSpec) -> SupervisorHandle:
        """Start the tree. Raises ``ValueError`` for an invalid spec."""
    def name(self) -> str: ...
    def uptime(self) -> int:
        """Seconds since the supervisor started."""

    def restart_strategy(self) -> RestartStrategy: ...
    def which_children(self) -> List[ChildInfo]: ...
    def count_children(self) -> Dict[str, int]: ...
    def start_child(
        self,
        id: str,
        restart_policy: RestartPolicy,
        worker_fn: Callable[..., Any],
    ) -> str: ...
    def start_child_linked(
        self,
        id: str,
        restart_policy: RestartPolicy,
        timeout_secs: int,
        worker_fn: Callable[..., Any],
    ) -> str:
        """Start a child and wait for it to initialize."""

    def start_unique_child(
        self,
        prefix: str,
        restart_policy: RestartPolicy,
        worker_fn: Callable[..., Any],
        *,
        shutdown: Optional[Shutdown] = None,
    ) -> str:
        """Start a child under a generated id ``{prefix}-{n}``; returns it."""

    def kill_child(self, child_id: str) -> bool:
        """Kill a child as if it crashed, so its restart policy applies."""

    def wait(self, timeout: Optional[float] = None) -> Optional[ChildExitReason]:
        """Block until the supervisor has stopped and return why, or ``None``
        if ``timeout`` seconds pass first."""

    def child_supervisor(self, child_id: str) -> SupervisorHandle:
        """The handle of a nested supervisor, for its current run.

        If the nested supervisor restarts, ask again, or give it a registered
        name and use ``SupervisorHandle.whereis``.
        """

    def subscribe(self) -> EventStream: ...
    def terminate_child(self, child_id: str) -> None: ...
    def shutdown(self) -> None:
        """Stop the tree and block until every worker has stopped.

        Workers are stopped last-started first, each within its shutdown
        timeout; one that ignores ``should_stop`` is abandoned once it elapses.
        """
        ...

class StatefulSupervisorSpec:
    """Describes a supervision tree whose workers share a ``WorkerContext``."""

    def __init__(self, name: str) -> None: ...
    def with_restart_strategy(self, strategy: RestartStrategy) -> None: ...
    def with_restart_intensity(self, intensity: RestartIntensity) -> None: ...
    def with_shutdown_timeout(self, seconds: float) -> None: ...
    def with_shutdown_order(self, order: ShutdownOrder) -> None: ...
    def with_auto_shutdown(self, auto_shutdown: AutoShutdown) -> None: ...
    def with_restart_policy(self, policy: RestartPolicy) -> None: ...
    def with_registered_name(self, name: str) -> None:
        """Register each run under ``name``; see ``SupervisorHandle.whereis``."""
    def context(self) -> WorkerContext:
        """The shared context handed to every worker."""

    def add_worker(
        self,
        id: str,
        restart_policy: RestartPolicy,
        worker_fn: Callable[..., Any],
        *,
        shutdown: Optional[Shutdown] = None,
        significant: bool = False,
    ) -> None:
        """Register a worker; it is called with the shared ``WorkerContext``."""

    def add_supervisor(self, supervisor: StatefulSupervisorSpec) -> None: ...

class StatefulSupervisorHandle:
    """Controls a running stateful supervision tree."""

    @staticmethod
    def start(spec: StatefulSupervisorSpec) -> StatefulSupervisorHandle: ...
    def name(self) -> str: ...
    def which_children(self) -> List[ChildInfo]: ...
    def count_children(self) -> Dict[str, int]: ...
    def kill_child(self, child_id: str) -> bool: ...
    def subscribe(self) -> EventStream: ...
    def terminate_child(self, child_id: str) -> None: ...
    def shutdown(self) -> None:
        """Stop the tree and block until every worker has stopped.

        Workers are stopped last-started first, each within its shutdown
        timeout; one that ignores ``should_stop`` is abandoned once it elapses.
        """
        ...

class SupervisorAddress:
    """Address of a remote supervisor."""

    @staticmethod
    def tcp(addr: str) -> SupervisorAddress: ...
    @staticmethod
    def unix(path: str) -> SupervisorAddress: ...

class RemoteSupervisorHandle:
    """Controls a supervisor in another process or host.

    With ``cookie`` set, both ends authenticate with the shared secret. Traffic
    is not encrypted; only connect over a network you control.
    """

    @staticmethod
    def connect_tcp(addr: str, *, cookie: Optional[str] = None) -> RemoteSupervisorHandle: ...
    @staticmethod
    def connect_unix(path: str, *, cookie: Optional[str] = None) -> RemoteSupervisorHandle: ...
    def status(self) -> Dict[str, Any]: ...
    def which_children(self) -> List[ChildInfo]: ...
    def terminate_child(self, child_id: str) -> None: ...
    def shutdown(self) -> None: ...
