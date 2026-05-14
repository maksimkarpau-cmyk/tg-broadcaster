#!/usr/bin/env python3
"""
Railway entrypoint.
Restores the Telethon .session file from base64 env var TG_SESSION_B64,
then runs the broadcaster.
"""
import base64
import os
import sys

session_b64 = os.environ.get("TG_SESSION_B64", "")
if session_b64:
    session_path = os.environ.get("TG_SESSION_NAME", "broadcaster") + ".session"
    with open(session_path, "wb") as f:
        f.write(base64.b64decode(session_b64))
    print(f"[entrypoint] Session restored → {session_path}")
else:
    print("[entrypoint] TG_SESSION_B64 not set — expecting existing session file")

# Now run broadcaster
import asyncio
from broadcaster import run_broadcast
asyncio.run(run_broadcast())
