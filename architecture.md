# Ash-Flare Architecture

## System Overview

```mermaid
graph TB
    subgraph "Public API"
        SupervisorSpec[SupervisorSpec<br/>Builder Pattern]
        SupervisorHandle[SupervisorHandle<br/>External Control]
        Worker[Worker Trait<br/>run/initialize/shutdown]
    end

    subgraph "Supervision Core"
        Runtime[SupervisorRuntime<br/>State Machine]
        RestartTracker[RestartTracker<br/>Time-windowed Limiting]
        CommandChannel[Command Channel<br/>mpsc::unbounded]
    end

    subgraph "Worker Management"
        WorkerProcess[WorkerProcess<br/>Lifecycle Manager]
        WorkerSpec[WorkerSpec<br/>Factory Pattern]
        RunWorker[run_worker<br/>Async Executor]
    end

    subgraph "Restart Policies"
        RestartStrategy[RestartStrategy<br/>OneForOne/OneForAll/RestForOne]
        RestartPolicy[RestartPolicy<br/>Permanent/Temporary/Transient]
        RestartIntensity[RestartIntensity<br/>max_restarts/within_seconds]
    end

    subgraph "Distributed Layer"
        RemoteHandle[RemoteSupervisorHandle<br/>Client]
        SupervisorServer[SupervisorServer<br/>TCP/Unix Socket Server]
        RemoteCommand[RemoteCommand/Response<br/>Bincode Serialization]
    end

    SupervisorSpec -->|start| SupervisorHandle
    SupervisorHandle -->|commands| CommandChannel
    CommandChannel -->|receives| Runtime
    Runtime -->|spawns| WorkerProcess
    WorkerProcess -->|uses| WorkerSpec
    WorkerProcess -->|runs| RunWorker
    RunWorker -->|implements| Worker
    Runtime -->|applies| RestartStrategy
    Runtime -->|checks| RestartPolicy
    Runtime -->|tracks| RestartTracker
    RestartTracker -->|enforces| RestartIntensity
    SupervisorHandle -->|wraps| SupervisorServer
    RemoteHandle -->|connects| SupervisorServer
    SupervisorServer -->|forwards| CommandChannel
    RemoteHandle -->|sends| RemoteCommand
```

## Module Structure

```mermaid
graph LR
    subgraph "src/"
        lib[lib.rs<br/>Public API]
        restart[restart.rs<br/>Policies & Tracking]
        types[types.rs<br/>Common Types]
        worker[worker.rs<br/>Worker Trait & Process]
        supervisor[supervisor.rs<br/>Supervisor Logic]
        distributed[distributed.rs<br/>Network Layer]
        mailbox[mailbox.rs<br/>Message Passing]
        macros[macros.rs<br/>Convenience Macros]
    end

    lib --> restart
    lib --> types
    lib --> worker
    lib --> supervisor
    lib --> distributed
    lib --> mailbox
    lib --> macros
    
    types --> restart
    worker --> types
    supervisor --> restart
    supervisor --> types
    supervisor --> worker
    distributed --> types
```

## Supervision Tree Flow

```mermaid
sequenceDiagram
    participant User
    participant Handle as SupervisorHandle
    participant Runtime as SupervisorRuntime
    participant Worker as WorkerProcess
    participant Child as Worker Impl

    User->>Handle: start(spec)
    Handle->>Runtime: spawn runtime
    Runtime->>Worker: spawn workers
    Worker->>Child: initialize()
    Worker->>Child: run()
    
    Note over Child: Worker crashes
    
    Child-->>Worker: Error
    Worker->>Runtime: ChildTerminated
    Runtime->>Runtime: check RestartPolicy
    Runtime->>Runtime: check RestartIntensity
    Runtime->>Runtime: apply RestartStrategy
    Runtime->>Worker: spawn new worker
    Worker->>Child: initialize()
    Worker->>Child: run()
```

## Restart Strategy Behavior

```mermaid
graph TB
    subgraph "OneForOne Strategy"
        direction LR
        W1[Worker 1]
        W2[Worker 2 ❌]
        W3[Worker 3]
        W2_new[Worker 2 🔄]
        W2 -.->|restart only| W2_new
    end

    subgraph "OneForAll Strategy"
        direction LR
        W4[Worker 1 🔄]
        W5[Worker 2 ❌]
        W6[Worker 3 🔄]
        W5 -.->|restart all| W4
        W5 -.->|restart all| W6
    end

    subgraph "RestForOne Strategy"
        direction LR
        W7[Worker 1]
        W8[Worker 2 ❌]
        W9[Worker 3 🔄]
        W8 -.->|restart self| W8
        W8 -.->|restart after| W9
    end
```

## Distributed Supervision

```mermaid
graph TB
    subgraph "Process 1: US-West Region"
        SV1[SupervisorHandle]
        RT1[SupervisorRuntime]
        SVR1[SupervisorServer<br/>Unix Socket]
        W1[IoT Device 1]
        W2[IoT Device 2]
        
        SV1 --> RT1
        RT1 --> W1
        RT1 --> W2
        SV1 --> SVR1
    end

    subgraph "Process 2: EU-West Region"
        SV2[SupervisorHandle]
        RT2[SupervisorRuntime]
        SVR2[SupervisorServer<br/>Unix Socket]
        W3[IoT Device 3]
        W4[IoT Device 4]
        
        SV2 --> RT2
        RT2 --> W3
        RT2 --> W4
        SV2 --> SVR2
    end

    subgraph "Process 3: Cluster Control"
        CC[Cluster Controller]
        RH1[RemoteSupervisorHandle]
        RH2[RemoteSupervisorHandle]
        
        CC --> RH1
        CC --> RH2
    end

    RH1 -.->|which_children<br/>terminate_child<br/>shutdown| SVR1
    RH2 -.->|which_children<br/>terminate_child<br/>shutdown| SVR2
```

## Channel Communication Pattern

```mermaid
graph LR
    subgraph "SupervisorRuntime"
        control_rx[control_rx<br/>Command Receiver]
        control_tx[control_tx<br/>Command Sender Clone]
    end

    subgraph "External API"
        handle[SupervisorHandle<br/>control_tx clone]
    end

    subgraph "Workers"
        wp1[WorkerProcess 1<br/>control_tx clone]
        wp2[WorkerProcess 2<br/>control_tx clone]
        wp3[WorkerProcess 3<br/>control_tx clone]
    end

    handle -->|StartChild| control_rx
    handle -->|TerminateChild| control_rx
    handle -->|WhichChildren| control_rx
    handle -->|Shutdown| control_rx
    
    wp1 -->|ChildTerminated| control_rx
    wp2 -->|ChildTerminated| control_rx
    wp3 -->|ChildTerminated| control_rx
    
    control_rx -->|processes| control_tx
```

## Restart Intensity Tracking

```mermaid
stateDiagram-v2
    [*] --> Normal: Supervisor starts
    Normal --> Tracking: Child fails
    Tracking --> Normal: Restart successful<br/>(under limit)
    Tracking --> ShuttingDown: Too many restarts<br/>(intensity exceeded)
    ShuttingDown --> [*]: Supervisor terminates
    
    note right of Tracking
        RestartTracker maintains
        time-windowed restart history
        Default: 3 restarts / 5 seconds
    end note
```

## Worker Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Created: Factory creates worker
    Created --> Initializing: WorkerProcess spawn
    Initializing --> Running: initialize succeeds
    Initializing --> Failed: initialize fails
    Running --> Completed: run returns Ok
    Running --> Failed: run returns Err
    Completed --> ShuttingDown: Normal exit
    Failed --> ShuttingDown: Abnormal exit
    ShuttingDown --> Terminated: shutdown called
    Terminated --> [*]
    
    Failed --> Restarting: RestartPolicy applies
    Restarting --> Created: Create new instance
```

## Start-Link Initialization

```mermaid
sequenceDiagram
    participant Supervisor
    participant WorkerProcess
    participant Worker

    Supervisor->>WorkerProcess: spawn worker
    WorkerProcess->>Worker: initialize()
    
    alt Initialization Success
        Worker-->>WorkerProcess: Ok(())
        WorkerProcess->>Supervisor: child ready
        WorkerProcess->>Worker: run()
    else Initialization Failure
        Worker-->>WorkerProcess: Err
        WorkerProcess->>Supervisor: child failed (not started)
        Note over Supervisor: Apply restart policy
    end
```

## Mailbox Communication

```mermaid
graph TB
    subgraph "Worker Pool Pattern"
        Producer[Producer]
        SharedMB[Arc&lt;Mutex&lt;Mailbox&gt;&gt;]
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker 3]
        
        Producer -->|send| SharedMB
        SharedMB -->|recv| W1
        SharedMB -->|recv| W2
        SharedMB -->|recv| W3
    end

    subgraph "Peer-to-Peer Pattern"
        WA[Worker A]
        MBA[Mailbox A]
        WB[Worker B]
        MBB[Mailbox B]
        
        WA -->|send| MBB
        MBB -->|recv| WB
        WB -->|send| MBA
        MBA -->|recv| WA
    end
```
