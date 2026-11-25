#!/usr/bin/env python3
"""
Controlling a cookie-protected remote supervisor

This example demonstrates:
- RemoteSupervisorHandle.connect_tcp(addr, cookie=...)
- status(), which_children(), terminate_child() and shutdown() on a remote
  supervisor
- Clients without the cookie, or with the wrong one, being refused

Start the server first, in another terminal:

    cargo run --example secure_remote -- serve

Python counterpart of: examples/secure_remote.rs
"""

import sys

import ash_flare as af

ADDRESS = "127.0.0.1:47311"
COOKIE = "correct-horse-battery-staple"


def try_status(label, handle):
    try:
        status = handle.status()
        print(f"{label:<14} ok: {status}")
        return True
    except RuntimeError as error:
        print(f"{label:<14} refused: {error}")
        return False


def main():
    print(f"=== Remote supervisor on {ADDRESS} ===\n")

    try_status("no cookie:", af.RemoteSupervisorHandle.connect_tcp(ADDRESS))
    try_status(
        "wrong cookie:", af.RemoteSupervisorHandle.connect_tcp(ADDRESS, cookie="guess")
    )

    operator = af.RemoteSupervisorHandle.connect_tcp(ADDRESS, cookie=COOKIE)
    if not try_status("right cookie:", operator):
        print("\nIs the server running? cargo run --example secure_remote -- serve")
        sys.exit(1)

    print(f"{'':<14} children: {[child.id for child in operator.which_children()]}")
    operator.terminate_child("reports")
    print(
        f"{'':<14} after terminating reports: {[c.id for c in operator.which_children()]}"
    )
    operator.shutdown()
    print(f"{'':<14} shut the supervisor down")


if __name__ == "__main__":
    main()
