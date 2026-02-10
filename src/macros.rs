//! Convenience macros for building supervision trees

/// Implement the Worker trait with minimal boilerplate for the run method only
///
/// # Examples
///
/// ```
/// use ash_flare::impl_worker;
/// use std::time::Duration;
///
/// struct MyWorker;
///
/// impl_worker! {
///     MyWorker, std::io::Error => {
///         // do work
///         tokio::time::sleep(Duration::from_millis(1)).await;
///         Ok(())
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_worker {
    ($worker:ty, $error:ty => $run_body:block) => {
        #[async_trait::async_trait]
        impl $crate::Worker for $worker {
            type Error = $error;

            async fn run(&mut self) -> Result<(), Self::Error> $run_body
        }
    };
}

/// Implement the Worker trait with access to self for stateful workers
///
/// # Examples
///
/// ```
/// use ash_flare::impl_worker_stateful;
/// use std::time::Duration;
///
/// struct MyWorker {
///     counter: usize,
/// }
///
/// impl_worker_stateful! {
///     MyWorker, std::io::Error => |self| {
///         self.counter += 1;
///         tokio::time::sleep(Duration::from_millis(1)).await;
///         Ok(())
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_worker_stateful {
    ($worker:ty, $error:ty => |$self:ident| $run_body:block) => {
        #[async_trait::async_trait]
        impl $crate::Worker for $worker {
            type Error = $error;

            async fn run(&mut $self) -> Result<(), Self::Error> $run_body
        }
    };
}

/// Implement the Worker trait with a mailbox for message-passing workers
///
/// This macro creates a worker that receives string messages from a mailbox.
/// The worker will loop receiving messages until the mailbox is closed.
/// The process body should not return a Result - just process the message.
///
/// # Examples
///
/// ```
/// use ash_flare::impl_worker_mailbox;
///
/// struct MessageWorker {
///     worker_id: usize,
///     mailbox: ash_flare::Mailbox,
/// }
///
/// impl_worker_mailbox! {
///     MessageWorker, std::io::Error => |self, msg| {
///         println!("[Worker {}] Received: {}", self.worker_id, msg);
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_worker_mailbox {
    ($worker:ty, $error:ty => |$self:ident, $msg:ident| $process_body:block) => {
        #[async_trait::async_trait]
        impl $crate::Worker for $worker {
            type Error = $error;

            async fn run(&mut $self) -> Result<(), Self::Error> {
                while let Some($msg) = $self.mailbox.recv().await $process_body
                Ok(())
            }
        }
    };
}

/// Build a stateful supervision tree with shared in-memory key-value store
///
/// # Examples
///
/// ```
/// use ash_flare::{stateful_supervision_tree, Worker, WorkerContext};
/// use async_trait::async_trait;
/// use std::sync::Arc;
///
/// struct MyWorker {
///     ctx: Arc<WorkerContext>,
/// }
///
/// #[async_trait]
/// impl Worker for MyWorker {
///     type Error = std::io::Error;
///     async fn run(&mut self) -> Result<(), Self::Error> {
///         self.ctx.set("key", serde_json::json!("value"));
///         Ok(())
///     }
/// }
///
/// let spec = stateful_supervision_tree! {
///     name: "app",
///     strategy: OneForOne,
///     intensity: (5, 10),
///     workers: [
///         ("worker-1", |ctx| MyWorker { ctx }, Permanent),
///         ("worker-2", |ctx| MyWorker { ctx }, Transient),
///     ],
///     supervisors: []
/// };
/// ```
#[macro_export]
macro_rules! stateful_supervision_tree {
    (
        name: $name:expr,
        strategy: $strategy:ident,
        intensity: ($max:expr, $secs:expr),
        workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
        supervisors: [ $($sup:expr),* $(,)? ]
    ) => {
        {
            let spec = $crate::StatefulSupervisorSpec::new($name)
                .with_restart_strategy($crate::RestartStrategy::$strategy)
                .with_restart_intensity($crate::RestartIntensity {
                    max_restarts: $max,
                    within_seconds: $secs,
                })
                $(
                    .with_worker($id, $factory, $crate::RestartPolicy::$policy)
                )*
                $(
                    .with_supervisor($sup)
                )*;
            spec
        }
    };
    (
        name: $name:expr,
        strategy: $strategy:ident,
        workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
        supervisors: [ $($sup:expr),* $(,)? ]
    ) => {
        {
            let spec = $crate::StatefulSupervisorSpec::new($name)
                .with_restart_strategy($crate::RestartStrategy::$strategy)
                $(
                    .with_worker($id, $factory, $crate::RestartPolicy::$policy)
                )*
                $(
                    .with_supervisor($sup)
                )*;
            spec
        }
    };
}

/// Build a supervision tree with a declarative syntax
///
/// # Examples
///
/// ```
/// use ash_flare::{supervision_tree, Worker};
/// use async_trait::async_trait;
///
/// struct MyWorker;
///
/// #[async_trait]
/// impl Worker for MyWorker {
///     type Error = std::io::Error;
///     async fn run(&mut self) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// impl MyWorker {
///     fn new() -> Self { Self }
/// }
///
/// let spec = supervision_tree! {
///     name: "app",
///     strategy: OneForOne,
///     intensity: (5, 10), // max_restarts, within_seconds
///     workers: [
///         ("worker-1", || MyWorker::new(), Permanent),
///         ("worker-2", || MyWorker::new(), Transient),
///     ],
///     supervisors: []
/// };
/// ```
#[macro_export]
macro_rules! supervision_tree {
    (
        name: $name:expr,
        strategy: $strategy:ident,
        intensity: ($max:expr, $secs:expr),
        workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
        supervisors: [ $($sup:expr),* $(,)? ]
    ) => {
        {
            let spec = $crate::SupervisorSpec::new($name)
                .with_restart_strategy($crate::RestartStrategy::$strategy)
                .with_restart_intensity($crate::RestartIntensity {
                    max_restarts: $max,
                    within_seconds: $secs,
                })
                $(
                    .with_worker($id, $factory, $crate::RestartPolicy::$policy)
                )*
                $(
                    .with_supervisor($sup)
                )*;
            spec
        }
    };
    (
        name: $name:expr,
        strategy: $strategy:ident,
        workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
        supervisors: [ $($sup:expr),* $(,)? ]
    ) => {
        {
            let spec = $crate::SupervisorSpec::new($name)
                .with_restart_strategy($crate::RestartStrategy::$strategy)
                $(
                    .with_worker($id, $factory, $crate::RestartPolicy::$policy)
                )*
                $(
                    .with_supervisor($sup)
                )*;
            spec
        }
    };
}

/// Start a distributed supervisor server with TCP or Unix socket
///
/// # Examples
///
/// ```ignore
/// // Ignored due to requiring actual network binding
/// use ash_flare::serve_supervisor;
///
/// // TCP server
/// serve_supervisor!(tcp, handle, "127.0.0.1:8080");
///
/// // Unix socket server (Unix only)
/// serve_supervisor!(unix, handle, "/tmp/supervisor.sock");
/// ```
#[macro_export]
macro_rules! serve_supervisor {
    (tcp, $handle:expr, $addr:expr) => {{
        let server = $crate::distributed::SupervisorServer::new($handle);
        tokio::spawn(async move { server.listen_tcp($addr).await })
    }};
    (unix, $handle:expr, $path:expr) => {{
        let server = $crate::distributed::SupervisorServer::new($handle);
        tokio::spawn(async move { server.listen_unix($path).await })
    }};
}

/// Connect to a remote supervisor via TCP or Unix socket
///
/// # Examples
///
/// ```ignore
/// // Ignored due to requiring actual network connection
/// use ash_flare::connect_supervisor;
///
/// // Connect via TCP
/// let remote = connect_supervisor!(tcp, "127.0.0.1:8080").await?;
///
/// // Connect via Unix socket (Unix only)
/// let remote = connect_supervisor!(unix, "/tmp/supervisor.sock").await?;
/// ```
#[macro_export]
macro_rules! connect_supervisor {
    (tcp, $addr:expr) => {
        $crate::distributed::RemoteSupervisorHandle::connect_tcp($addr)
    };
    (unix, $path:expr) => {
        $crate::distributed::RemoteSupervisorHandle::connect_unix($path)
    };
}

/// Create and start a distributed supervision system with server and client
///
/// # Examples
///
/// ```ignore
/// // Ignored due to requiring actual network binding
/// use ash_flare::distributed_system;
///
/// distributed_system! {
///     server: tcp @ "127.0.0.1:8080" => {
///         name: "remote-app",
///         strategy: OneForOne,
///         workers: [
///             ("worker-1", || MyWorker::new(), Permanent),
///         ],
///         supervisors: []
///     }
/// }
/// ```
#[macro_export]
macro_rules! distributed_system {
    (
        server: tcp @ $addr:expr => {
            name: $name:expr,
            strategy: $strategy:ident,
            workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
            supervisors: [ $($sup:expr),* $(,)? ]
        }
    ) => {
        {
            let spec = supervision_tree! {
                name: $name,
                strategy: $strategy,
                workers: [ $(($id, $factory, $policy)),* ],
                supervisors: [ $($sup),* ]
            };
            let handle = $crate::SupervisorHandle::start(spec);
            let server = $crate::distributed::SupervisorServer::new(handle.clone());
            let server_task = tokio::spawn(async move {
                server.listen_tcp($addr).await
            });
            (handle, server_task)
        }
    };
    (
        server: unix @ $path:expr => {
            name: $name:expr,
            strategy: $strategy:ident,
            workers: [ $(($id:expr, $factory:expr, $policy:ident)),* $(,)? ],
            supervisors: [ $($sup:expr),* $(,)? ]
        }
    ) => {
        {
            let spec = supervision_tree! {
                name: $name,
                strategy: $strategy,
                workers: [ $(($id, $factory, $policy)),* ],
                supervisors: [ $($sup),* ]
            };
            let handle = $crate::SupervisorHandle::start(spec);
            let server = $crate::distributed::SupervisorServer::new(handle.clone());
            let server_task = tokio::spawn(async move {
                server.listen_unix($path).await
            });
            (handle, server_task)
        }
    };
}
