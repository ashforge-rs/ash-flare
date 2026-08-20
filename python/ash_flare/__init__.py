"""Fault-tolerant supervision trees for Python, powered by Rust.

See https://github.com/ashforge-rs/ash-flare for documentation.
"""

from .ash_flare import (
    ChildExitReason,
    ChildInfo,
    ChildType,
    Mailbox,
    MailboxConfig,
    MailboxHandle,
    RemoteSupervisorHandle,
    RestartIntensity,
    RestartPolicy,
    RestartStrategy,
    ShouldStop,
    StatefulSupervisorHandle,
    StatefulSupervisorSpec,
    SupervisorAddress,
    SupervisorHandle,
    SupervisorSpec,
    WorkerContext,
    mailbox,
    mailbox_named,
    version,
)

__version__ = version

__all__ = [
    "ChildExitReason",
    "ChildInfo",
    "ChildType",
    "Mailbox",
    "MailboxConfig",
    "MailboxHandle",
    "RemoteSupervisorHandle",
    "RestartIntensity",
    "RestartPolicy",
    "RestartStrategy",
    "ShouldStop",
    "StatefulSupervisorHandle",
    "StatefulSupervisorSpec",
    "SupervisorAddress",
    "SupervisorHandle",
    "SupervisorSpec",
    "WorkerContext",
    "__version__",
    "mailbox",
    "mailbox_named",
    "version",
]
