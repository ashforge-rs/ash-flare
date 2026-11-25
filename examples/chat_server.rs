//! A small chat service, structured the way it would be on the BEAM.
//!
//! ```text
//! chat (one_for_one)          rooms (dynamic)        sessions (dynamic)
//! ├── history  gen_server     ├── room:general       ├── user-alice
//! └── lobby    gen_server     └── room:rust          ├── user-bob
//!                                                    └── user-carol
//! ```
//!
//! - **history** stores messages per room, so they outlive a crashing room,
//!   and saves them when the service stops (`Shutdown::Infinity`).
//! - **lobby** opens rooms on demand, starting each one as a child of `rooms`.
//!   It keeps no state of its own: open rooms are found in the `Registry`.
//! - **room** is a gen_server per room. It monitors its members through the
//!   `sessions` supervisor's events, as `monitor/2` and `'DOWN'` messages
//!   would, and has a bug that makes it crash on an empty message.
//! - **session** is one process per connected user (`Temporary`). It is
//!   registered as `user:<name>` for direct messages, and monitors the rooms it
//!   is in so it can rejoin one that was restarted.
//!
//! Messages between processes are casts (`send`, or `try_send`, which like
//! Erlang's `!` never blocks) and calls (`call` with a `Reply`).
//!
//! ash-flare does not hand out handles to nested supervisors, so the two
//! dynamic supervisors are started on their own and given to the workers that
//! start children on them; in OTP they would sit under `chat`.
//!
//! Run with: `cargo run --example chat_server`

use ash_flare::{
    ChildExitReason, ChildSpec, Mailbox, MailboxConfig, MailboxHandle, Registration, Registry,
    Reply, RestartPolicy, Shutdown, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker,
    mailbox_named,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;

const CALL_TIMEOUT: Duration = Duration::from_secs(1);

// ============================================================================
// Protocol: what each kind of process understands
// ============================================================================

enum HistoryMsg {
    Append {
        room: String,
        line: String,
    },
    Recent {
        room: String,
        count: usize,
        reply: Reply<Vec<String>>,
    },
}

enum LobbyMsg {
    Open {
        room: String,
        reply: Reply<Result<RoomRef, String>>,
    },
}

enum RoomMsg {
    Join {
        user: String,
        session: String,
        to: SessionRef,
        /// Rejoining a room that restarted: no greeting, no replay.
        rejoin: bool,
    },
    Say {
        user: String,
        text: String,
    },
}

enum SessionMsg {
    /// A line typed by the user, as if read from their connection.
    Input(String),
    /// Something to show the user.
    Deliver(String),
}

type HistoryRef = MailboxHandle<HistoryMsg>;
type LobbyRef = MailboxHandle<LobbyMsg>;
type RoomRef = MailboxHandle<RoomMsg>;
type SessionRef = MailboxHandle<SessionMsg>;

// ============================================================================
// history: a gen_server holding every room's messages
// ============================================================================

struct History {
    inbox: Mailbox<HistoryMsg>,
    lines: HashMap<String, Vec<String>>,
}

impl Worker for History {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(message) = self.inbox.recv().await {
            match message {
                HistoryMsg::Append { room, line } => self.lines.entry(room).or_default().push(line),
                HistoryMsg::Recent { room, count, reply } => {
                    let lines = self.lines.get(&room).map_or(&[][..], Vec::as_slice);
                    reply.send(lines[lines.len().saturating_sub(count)..].to_vec());
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        let total: usize = self.lines.values().map(Vec::len).sum();
        sleep(Duration::from_millis(200)).await; // stands in for writing to disk
        println!(
            "[history] saved {total} messages in {} rooms",
            self.lines.len()
        );
        Ok(())
    }
}

// ============================================================================
// lobby: opens rooms on demand
// ============================================================================

struct Lobby {
    inbox: Mailbox<LobbyMsg>,
    rooms: SupervisorHandle,
    sessions: SupervisorHandle,
    history: HistoryRef,
    registry: Registry,
}

impl Lobby {
    async fn open(&self, room: &str) -> Result<RoomRef, String> {
        let key = format!("room:{room}");
        if let Some(existing) = self.registry.whereis::<RoomRef>(&key) {
            return Ok(existing);
        }

        // The room's mailbox outlives the room: its replacement after a crash
        // gets a clone, so every handle given out keeps working.
        let (handle, inbox) = mailbox_named::<RoomMsg>(MailboxConfig::default(), key.clone());
        let (name, registry, history, sessions, me) = (
            room.to_owned(),
            self.registry.clone(),
            self.history.clone(),
            self.sessions.clone(),
            handle.clone(),
        );
        let factory = move || Room {
            name: name.clone(),
            inbox: inbox.clone(),
            history: history.clone(),
            // Subscribed afresh by every instance: its members' 'DOWN' messages.
            monitor: sessions.subscribe(),
            members: HashMap::new(),
            _registered: registry.register(format!("room:{name}"), me.clone()),
        };
        // Waits for `initialize`, so the room is ready when the caller gets it.
        self.rooms
            .start_child_linked(key, factory, RestartPolicy::Permanent, CALL_TIMEOUT)
            .await
            .map_err(|error| error.to_string())?;
        Ok(handle)
    }
}

impl Worker for Lobby {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        while let Some(LobbyMsg::Open { room, reply }) = self.inbox.recv().await {
            reply.send(self.open(&room).await);
        }
        Ok(())
    }
}

// ============================================================================
// room: one gen_server per room
// ============================================================================

struct Member {
    user: String,
    to: SessionRef,
}

struct Room {
    name: String,
    inbox: Mailbox<RoomMsg>,
    history: HistoryRef,
    monitor: Receiver<SupervisorEvent>,
    /// By session id, which is what the monitor reports.
    members: HashMap<String, Member>,
    _registered: Registration,
}

impl Room {
    fn broadcast(&self, text: &str) {
        for member in self.members.values() {
            let _ = member
                .to
                .try_send(SessionMsg::Deliver(format!("#{} {text}", self.name)));
        }
    }

    async fn handle(&mut self, message: RoomMsg) -> Result<(), String> {
        match message {
            RoomMsg::Join {
                user,
                session,
                to,
                rejoin,
            } => {
                if rejoin {
                    self.members.insert(session, Member { user, to });
                    return Ok(());
                }
                let recent = self
                    .history
                    .call(
                        |reply| HistoryMsg::Recent {
                            room: self.name.clone(),
                            count: 2,
                            reply,
                        },
                        CALL_TIMEOUT,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                for line in recent {
                    let _ = to.try_send(SessionMsg::Deliver(format!(
                        "#{} (earlier) {line}",
                        self.name
                    )));
                }
                self.broadcast(&format!("* {user} joined"));
                self.members.insert(session, Member { user, to });
            }
            RoomMsg::Say { user, text } => {
                // The bug: a message is assumed to be non-empty.
                let first = text
                    .chars()
                    .next()
                    .expect("a message has at least one character");
                let line = format!("{user}: {}{}", first, &text[first.len_utf8()..]);
                self.history
                    .send(HistoryMsg::Append {
                        room: self.name.clone(),
                        line: line.clone(),
                    })
                    .await
                    .map_err(|error| error.to_string())?;
                self.broadcast(&line);
            }
        }
        Ok(())
    }

    fn member_down(&mut self, session: &str, reason: ChildExitReason) {
        if let Some(member) = self.members.remove(session) {
            let why = match reason {
                ChildExitReason::Normal => "quit",
                ChildExitReason::Killed => "connection lost",
                _ => "disconnected",
            };
            self.broadcast(&format!("* {} left ({why})", member.user));
        }
    }
}

impl Worker for Room {
    type Error = String;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[room:{}] started", self.name);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::select! {
                message = self.inbox.recv() => match message {
                    Some(message) => self.handle(message).await?,
                    None => return Ok(()),
                },
                event = self.monitor.recv() => {
                    if let Ok(SupervisorEvent::ChildTerminated { id, reason }) = event {
                        self.member_down(&id, reason);
                    }
                }
            }
        }
    }
}

// ============================================================================
// session: one process per connected user
// ============================================================================

struct Session {
    user: String,
    id: String,
    inbox: Mailbox<SessionMsg>,
    me: SessionRef,
    lobby: LobbyRef,
    registry: Registry,
    /// Restarts of rooms, so this session can rejoin them.
    room_events: Receiver<SupervisorEvent>,
    joined: HashMap<String, RoomRef>,
    current: Option<RoomRef>,
    _registered: Option<Registration>,
}

impl Session {
    fn show(&self, text: &str) {
        println!("  {:>5} | {text}", self.user);
    }

    async fn join(&mut self, room: &str, rejoin: bool) -> Result<(), String> {
        if !rejoin && let Some(handle) = self.joined.get(room) {
            // Already a member: just switch to it.
            self.current = Some(handle.clone());
            return Ok(());
        }
        let handle = self
            .lobby
            .call(
                |reply| LobbyMsg::Open {
                    room: room.to_owned(),
                    reply,
                },
                CALL_TIMEOUT,
            )
            .await
            .map_err(|error| error.to_string())??;
        handle
            .send(RoomMsg::Join {
                user: self.user.clone(),
                session: self.id.clone(),
                to: self.me.clone(),
                rejoin,
            })
            .await
            .map_err(|error| error.to_string())?;
        if !rejoin {
            self.current = Some(handle.clone());
        }
        self.joined.insert(room.to_owned(), handle);
        Ok(())
    }

    /// Handles a line typed by the user. Returns `false` when they quit.
    async fn input(&mut self, line: String) -> Result<bool, String> {
        let mut words = line.splitn(3, ' ');
        match (words.next(), words.next(), words.next()) {
            (Some("/join"), Some(room), None) => self.join(room, false).await?,
            (Some("/msg"), Some(to), Some(text)) => {
                match self.registry.whereis::<SessionRef>(&format!("user:{to}")) {
                    Some(peer) => {
                        let message = format!("(direct from {}) {text}", self.user);
                        let _ = peer.try_send(SessionMsg::Deliver(message));
                    }
                    None => self.show(&format!("{to} is offline")),
                }
            }
            (Some("/quit"), None, None) => return Ok(false),
            _ => match &self.current {
                Some(room) => room
                    .send(RoomMsg::Say {
                        user: self.user.clone(),
                        text: line,
                    })
                    .await
                    .map_err(|error| error.to_string())?,
                None => self.show("join a room first"),
            },
        }
        Ok(true)
    }

    async fn room_event(&mut self, event: SupervisorEvent) -> Result<(), String> {
        if let SupervisorEvent::ChildRestarted { id } = event
            && let Some(room) = id.strip_prefix("room:")
            && self.joined.contains_key(room)
        {
            self.show(&format!("#{room} restarted, rejoining"));
            self.join(room, true).await?;
        }
        Ok(())
    }
}

impl Worker for Session {
    type Error = String;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        let name = format!("user:{}", self.user);
        self._registered = Some(self.registry.register(name, self.me.clone()));
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::select! {
                message = self.inbox.recv() => match message {
                    Some(SessionMsg::Input(line)) => {
                        if !self.input(line).await? {
                            return Ok(());
                        }
                    }
                    Some(SessionMsg::Deliver(text)) => self.show(&text),
                    None => return Ok(()),
                },
                event = self.room_events.recv() => {
                    if let Ok(event) = event {
                        self.room_event(event).await?;
                    }
                }
            }
        }
    }
}

// ============================================================================
// The service
// ============================================================================

struct Chat {
    app: SupervisorHandle,
    rooms: SupervisorHandle,
    sessions: SupervisorHandle,
    lobby: LobbyRef,
    registry: Registry,
}

impl Chat {
    fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let registry = Registry::new();
        let rooms = SupervisorHandle::start(SupervisorSpec::new("rooms"))?;
        let sessions = SupervisorHandle::start(SupervisorSpec::new("sessions"))?;

        let (history, history_inbox) = mailbox_named(MailboxConfig::default(), "history");
        let (lobby, lobby_inbox) = mailbox_named(MailboxConfig::default(), "lobby");
        let (lobby_rooms, lobby_sessions, lobby_history, lobby_registry) = (
            rooms.clone(),
            sessions.clone(),
            history.clone(),
            registry.clone(),
        );

        let app = SupervisorHandle::start(
            SupervisorSpec::new("chat")
                .with_child(
                    ChildSpec::new("history", move || History {
                        inbox: history_inbox.clone(),
                        lines: HashMap::new(),
                    })
                    // Never cut short while saving.
                    .shutdown(Shutdown::Infinity),
                )
                .with_worker(
                    "lobby",
                    move || Lobby {
                        inbox: lobby_inbox.clone(),
                        rooms: lobby_rooms.clone(),
                        sessions: lobby_sessions.clone(),
                        history: lobby_history.clone(),
                        registry: lobby_registry.clone(),
                    },
                    RestartPolicy::Permanent,
                ),
        )?;

        Ok(Self {
            app,
            rooms,
            sessions,
            lobby,
            registry,
        })
    }

    /// A user connects: start a session process for them.
    async fn connect(&self, user: &str) -> Result<SessionRef, Box<dyn std::error::Error>> {
        let id = format!("user-{user}");
        let (handle, inbox) = mailbox_named(MailboxConfig::default(), id.clone());
        let (name, session_id, me, lobby, registry, rooms) = (
            user.to_owned(),
            id.clone(),
            handle.clone(),
            self.lobby.clone(),
            self.registry.clone(),
            self.rooms.clone(),
        );
        let factory = move || Session {
            user: name.clone(),
            id: session_id.clone(),
            inbox: inbox.clone(),
            me: me.clone(),
            lobby: lobby.clone(),
            registry: registry.clone(),
            room_events: rooms.subscribe(),
            joined: HashMap::new(),
            current: None,
            _registered: None,
        };
        // Temporary: a session that ends, for whatever reason, stays ended.
        self.sessions
            .start_child_linked(id, factory, RestartPolicy::Temporary, CALL_TIMEOUT)
            .await?;
        Ok(handle)
    }

    /// Stops the service: users first, then rooms, then the core.
    async fn stop(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.sessions.shutdown().await?;
        self.rooms.shutdown().await?;
        self.app.shutdown().await?;
        Ok(())
    }
}

/// Types a line into a user's session and gives the system a moment.
async fn say(user: &SessionRef, line: &str) -> Result<(), Box<dyn std::error::Error>> {
    user.send(SessionMsg::Input(line.to_owned())).await?;
    sleep(Duration::from_millis(50)).await;
    Ok(())
}

fn step(title: &str) {
    println!("\n-- {title}");
}

/// The message a panic was raised with.
fn panic_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== chat_server ===");
    std::panic::set_hook(Box::new(|info| {
        println!("[panic] {}", panic_message(info));
    }));

    let chat = Chat::start()?;

    step("alice and bob connect and join #general");
    let alice = chat.connect("alice").await?;
    let bob = chat.connect("bob").await?;
    say(&alice, "/join general").await?;
    say(&bob, "/join general").await?;
    say(&alice, "hi bob").await?;
    say(&bob, "hey alice").await?;

    step("bob opens #rust; alice sends him a direct message");
    say(&bob, "/join rust").await?;
    say(&bob, "anyone here?").await?;
    say(&alice, "/msg bob lunch?").await?;

    step("an empty message hits a bug in #general: it crashes and is restarted");
    say(&alice, "/join general").await?; // back to #general
    say(&alice, "").await?;
    sleep(Duration::from_millis(300)).await;
    say(&alice, "still there?").await?;

    step("carol joins #general and sees messages from before the crash");
    let carol = chat.connect("carol").await?;
    say(&carol, "/join general").await?;

    step("bob's connection drops; alice quits");
    chat.sessions.kill_child("user-bob").await?;
    sleep(Duration::from_millis(50)).await;
    say(&carol, "/msg bob are you there?").await?;
    say(&alice, "/quit").await?;

    step("the service stops");
    chat.stop().await?;
    Ok(())
}
