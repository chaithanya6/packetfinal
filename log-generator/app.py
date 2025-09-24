#!/usr/bin/env python3
# simple log generator that periodically posts to listener
import os
import time
import uuid
import socket
import random
import requests
from datetime import datetime, timezone

LISTENER_URL = os.environ.get("LISTENER_URL", "http://log-listener:5001/logs")
CLIENT_NAME = os.environ.get("CLIENT_NAME") or socket.gethostname()
SLEEP = float(os.environ.get("GEN_INTERVAL", "1.0"))  # seconds between events

# only these 4 levels allowed
LEVELS = ["ERROR", "WARNING", "INFO", "DEBUG"]

MESSAGES = [
    "User login succeeded",
    "User login failed",
    "Payment processed",
    "Payment declined",
    "System timeout",
    "Cache refreshed",
    "Debugging request",
    "Service started",
    "Service stopped",
    "Configuration updated"
]

def gen_event():
    ev = {
        "event_id": str(uuid.uuid4()),
        "level": random.choice(LEVELS),        # pick simple random level
        "message": random.choice(MESSAGES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "client_name": CLIENT_NAME
    }
    return ev

def send(event):
    try:
        r = requests.post(LISTENER_URL, json=event, timeout=5)
        return r.status_code, r.text
    except Exception as e:
        return None, str(e)

def run():
    print(f"Log generator started — sending to {LISTENER_URL} as client {CLIENT_NAME}")
    while True:
        ev = gen_event()
        status, info = send(ev)
        if status:
            print(f"sent {ev['event_id'][:6]} {ev['level']} -> {status}")
        else:
            print(f"send error: {info}")
        time.sleep(SLEEP)

if __name__ == "__main__":
    run()
