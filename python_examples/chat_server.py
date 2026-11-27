#!/usr/bin/env python3
"""
A small chat service, structured the way it would be on the BEAM

    chat (one_for_one)          rooms (dynamic)        sessions (dynamic)
    ├── history  gen_server     ├── room:general       ├── user-alice
    └── lobby    gen_server     └── room:rust          ├── user-bob
                                                       └── user-carol

- history stores messages per room, so they outlive a crashing room, and
  saves them when the service stops (Shutdown.infinity()).
- lobby opens rooms on demand, starting each one as a child of `rooms`.
- room is a gen_server per room. It monitors its members through the
  `sessions` supervisor's events, and has a bug that makes it crash on an
  empty message.
- session is one process per connected user (temporary). It registers as
  user:<name> for direct messages, and monitors the rooms it is in so it can
  rejoin one that was restarted.

Python mailboxes carry strings, so messages here are JSON, and processes are
found by name in a small registry. A call sends the name of a one-off reply
mailbox along with the request, the way an Erlang call carries a reference
to reply to. Workers poll their mailbox and should_stop, so they notice when
they are asked to stop.

Python counterpart of: examples/chat_server.rs
"""

import itertools
import json
import threading
import time

import ash_flare as af

CALL_TIMEOUT = 1.0
POLL = 0.01


# ============================================================================
# Plumbing: a registry, and receive / call / reply over string mailboxes
# ============================================================================


class Registry:
    """Names to mailbox handles, like Erlang's register/whereis."""

    def __init__(self):
        self._names = {}
        self._lock = threading.Lock()

    def register(self, name, handle):
        with self._lock:
            self._names[name] = handle

    def unregister(self, name):
        with self._lock:
            self._names.pop(name, None)

    def whereis(self, name):
        with self._lock:
            return self._names.get(name)


registry = Registry()
_references = itertools.count()


def poll(inbox):
    """The next message if one is waiting, else None."""
    try:
        return json.loads(inbox.try_recv())
    except RuntimeError:
        return None


def cast(handle, **message):
    """Sends without waiting, like Erlang's `!`; a full or closed mailbox drops it."""
    try:
        handle.try_send(json.dumps(message))
    except RuntimeError:
        pass


def call(handle, **request):
    """Sends a request and waits for its reply, like gen_server:call."""
    reference = f"reply-{next(_references)}"
    reply_handle, reply_inbox = af.mailbox_named(reference, af.MailboxConfig.bounded(1))
    registry.register(reference, reply_handle)
    try:
        handle.send(json.dumps({**request, "reply_to": reference}))
        deadline = time.monotonic() + CALL_TIMEOUT
        while time.monotonic() < deadline:
            answer = poll(reply_inbox)
            if answer is not None:
                return answer
            time.sleep(POLL)
        raise TimeoutError(f"no reply to {request['op']} within {CALL_TIMEOUT}s")
    finally:
        registry.unregister(reference)


def reply(request, value):
    to = registry.whereis(request["reply_to"])
    if to is not None:
        cast(to, value=value)


# ============================================================================
# history: a gen_server holding every room's messages
# ============================================================================


def history_worker(inbox):
    lines = {}

    def worker(should_stop):
        while not should_stop():
            message = poll(inbox)
            if message is None:
                time.sleep(POLL)
            elif message["op"] == "append":
                lines.setdefault(message["room"], []).append(message["line"])
            elif message["op"] == "recent":
                room = lines.get(message["room"], [])
                reply(message, room[-message["count"] :])
        time.sleep(0.2)  # stands in for writing to disk
        total = sum(len(room) for room in lines.values())
        print(f"[history] saved {total} messages in {len(lines)} rooms")

    return worker


# ============================================================================
# lobby: opens rooms on demand
# ============================================================================


def lobby_worker(inbox, rooms, sessions, history):
    def open_room(name):
        key = f"room:{name}"
        if registry.whereis(key) is None:
            # The room's mailbox outlives the room: its replacement after a
            # crash reads from the same one, so senders keep working.
            handle, room_inbox = af.mailbox_named(key, af.MailboxConfig.bounded(100))
            registry.register(key, handle)
            worker = room_worker(name, room_inbox, sessions, history)
            rooms.start_child_linked(key, af.RestartPolicy.permanent(), 1, worker)

    def worker(should_stop):
        while not should_stop():
            message = poll(inbox)
            if message is None:
                time.sleep(POLL)
            elif message["op"] == "open":
                open_room(message["room"])
                reply(message, True)

    return worker


# ============================================================================
# room: one gen_server per room
# ============================================================================


def room_worker(name, inbox, sessions, history):
    def worker(should_stop):
        print(f"[room:{name}] started")
        monitor = sessions.subscribe()  # its members' 'DOWN' messages
        members = {}  # session id -> (user, handle)

        def broadcast(text):
            for _, to in members.values():
                cast(to, op="deliver", text=f"#{name} {text}")

        while not should_stop():
            message = poll(inbox)
            event = monitor.recv(timeout=0)
            if message is None and event is None:
                time.sleep(POLL)

            if message and message["op"] == "join":
                to = registry.whereis(f"user:{message['user']}")
                if to is None:
                    continue
                if not message["rejoin"]:
                    for line in call(history, op="recent", room=name, count=2)["value"]:
                        cast(to, op="deliver", text=f"#{name} (earlier) {line}")
                    broadcast(f"* {message['user']} joined")
                members[message["session"]] = (message["user"], to)

            elif message and message["op"] == "say":
                # The bug: a message is assumed to be non-empty.
                first, rest = message["text"][0], message["text"][1:]
                line = f"{message['user']}: {first}{rest}"
                cast(history, op="append", room=name, line=line)
                broadcast(line)

            if event and event.kind == "child_terminated" and event.child_id in members:
                user, _ = members.pop(event.child_id)
                if event.reason.is_normal():
                    why = "quit"
                elif event.reason.is_killed():
                    why = "connection lost"
                else:
                    why = "disconnected"
                broadcast(f"* {user} left ({why})")

    return worker


# ============================================================================
# session: one process per connected user
# ============================================================================


def session_worker(user, session_id, inbox, lobby, rooms):
    def worker(should_stop):
        me = f"user:{user}"
        registry.register(me, registry.whereis(f"inbox:{session_id}"))
        room_events = rooms.subscribe()  # restarts of rooms, to rejoin them
        joined = set()
        current = None

        def show(text):
            print(f"  {user:>5} | {text}")

        def join(room, rejoin):
            nonlocal current
            if not rejoin and room in joined:
                current = room  # already a member: just switch to it
                return
            call(lobby, op="open", room=room)
            cast(
                registry.whereis(f"room:{room}"),
                op="join",
                user=user,
                session=session_id,
                rejoin=rejoin,
            )
            joined.add(room)
            if not rejoin:
                current = room

        def handle_input(line):
            words = line.split(" ", 2)
            if words[0] == "/join" and len(words) == 2:
                join(words[1], rejoin=False)
            elif words[0] == "/msg" and len(words) == 3:
                peer = registry.whereis(f"user:{words[1]}")
                if peer is None:
                    show(f"{words[1]} is offline")
                else:
                    cast(peer, op="deliver", text=f"(direct from {user}) {words[2]}")
            elif line == "/quit":
                return False
            elif current is None:
                show("join a room first")
            else:
                cast(
                    registry.whereis(f"room:{current}"), op="say", user=user, text=line
                )
            return True

        try:
            while not should_stop():
                message = poll(inbox)
                event = room_events.recv(timeout=0)
                if message is None and event is None:
                    time.sleep(POLL)

                if message and message["op"] == "input":
                    if not handle_input(message["line"]):
                        return
                elif message and message["op"] == "deliver":
                    show(message["text"])

                if event and event.kind == "child_restarted":
                    room = event.child_id.removeprefix("room:")
                    if room in joined:
                        show(f"#{room} restarted, rejoining")
                        join(room, rejoin=True)
        finally:
            # A registered name lives as long as its process.
            registry.unregister(me)

    return worker


# ============================================================================
# The service
# ============================================================================


class Chat:
    def __init__(self):
        rooms_spec = af.SupervisorSpec("rooms")
        rooms_spec.with_restart_delay(0.1)
        self.rooms = af.SupervisorHandle.start(rooms_spec)
        self.sessions = af.SupervisorHandle.start(af.SupervisorSpec("sessions"))

        history, history_inbox = af.mailbox_named(
            "history", af.MailboxConfig.bounded(100)
        )
        self.lobby, lobby_inbox = af.mailbox_named(
            "lobby", af.MailboxConfig.bounded(100)
        )

        app = af.SupervisorSpec("chat")
        app.add_worker(
            "history",
            af.RestartPolicy.permanent(),
            history_worker(history_inbox),
            shutdown=af.Shutdown.infinity(),  # never cut short while saving
        )
        app.add_worker(
            "lobby",
            af.RestartPolicy.permanent(),
            lobby_worker(lobby_inbox, self.rooms, self.sessions, history),
        )
        self.app = af.SupervisorHandle.start(app)

    def connect(self, user):
        """A user connects: start a session process for them."""
        session_id = f"user-{user}"
        handle, inbox = af.mailbox_named(session_id, af.MailboxConfig.bounded(100))
        registry.register(f"inbox:{session_id}", handle)
        worker = session_worker(user, session_id, inbox, self.lobby, self.rooms)
        # Temporary: a session that ends, for whatever reason, stays ended.
        self.sessions.start_child_linked(
            session_id, af.RestartPolicy.temporary(), 1, worker
        )
        return handle

    def stop(self):
        """Stops the service: users first, then rooms, then the core."""
        self.sessions.shutdown()
        self.rooms.shutdown()
        self.app.shutdown()


def say(user, line):
    """Types a line into a user's session and gives the system a moment."""
    cast(user, op="input", line=line)
    time.sleep(0.1)


def step(title):
    print(f"\n-- {title}")


def main():
    print("=== chat_server ===")
    chat = Chat()

    step("alice and bob connect and join #general")
    alice = chat.connect("alice")
    bob = chat.connect("bob")
    say(alice, "/join general")
    say(bob, "/join general")
    say(alice, "hi bob")
    say(bob, "hey alice")

    step("bob opens #rust; alice sends him a direct message")
    say(bob, "/join rust")
    say(bob, "anyone here?")
    say(alice, "/msg bob lunch?")

    step("an empty message hits a bug in #general: it crashes and is restarted")
    say(alice, "/join general")  # back to #general
    say(alice, "")
    time.sleep(0.4)
    say(alice, "still there?")

    step("carol joins #general and sees messages from before the crash")
    carol = chat.connect("carol")
    say(carol, "/join general")

    step("bob's connection drops; alice quits")
    chat.sessions.kill_child("user-bob")
    time.sleep(0.2)
    say(carol, "/msg bob are you there?")
    say(alice, "/quit")

    step("the service stops")
    chat.stop()


if __name__ == "__main__":
    main()
