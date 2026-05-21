"""
Mesh demo node. Run 3 in 3 terminals:

  python3 node.py A 8001 B=8002,C=8003
  python3 node.py B 8002 A=8001,C=8003
  python3 node.py C 8003 A=8001,B=8002

Add 'offline' at the end to start a node offline:
  python3 node.py C 8003 A=8001,B=8002 offline

Toggle a node's network from a 4th terminal:
  curl -X POST localhost:8001/offline
  curl -X POST localhost:8001/online
"""

import logging
import sys
import threading
import time

import requests
from flask import Flask, jsonify, request

MY_ID = sys.argv[1]
MY_PORT = int(sys.argv[2])
PEERS = {p.split("=")[0]: int(p.split("=")[1]) for p in sys.argv[3].split(",")}
offline = len(sys.argv) > 4 and sys.argv[4] == "offline"

messages = {}            # msg_id -> content
lock = threading.Lock()
app = Flask(__name__)


@app.route("/messages", methods=["POST"])
def receive():
    if offline:
        return jsonify({"error": "offline"}), 503
    sender = request.json["from"]
    incoming = request.json["messages"]
    new_msgs = []
    with lock:
        for mid, content in incoming.items():
            if mid not in messages:
                messages[mid] = content
                new_msgs.append(mid)
    if new_msgs:
        print(f"  [{MY_ID}] <- {sender}: got {', '.join(sorted(new_msgs))}")
    return jsonify({"ok": True})


@app.route("/offline", methods=["POST"])
def go_offline():
    global offline
    offline = True
    print(f"\n========== [{MY_ID}] OFFLINE ==========\n")
    return "ok"


@app.route("/online", methods=["POST"])
def go_online():
    global offline
    offline = False
    print(f"\n========== [{MY_ID}] BACK ONLINE ==========\n")
    return "ok"


def generate():
    seq = 0
    while True:
        time.sleep(3)
        if offline:
            continue
        seq += 1
        mid = f"{MY_ID}{seq}"
        with lock:
            messages[mid] = f"data from {MY_ID}"
        print(f"[{MY_ID}] generated {mid}")


def sync():
    while True:
        time.sleep(2)
        if offline:
            continue
        with lock:
            snapshot = dict(messages)
        if not snapshot:
            continue
        for peer_id, port in PEERS.items():
            try:
                requests.post(
                    f"http://localhost:{port}/messages",
                    json={"from": MY_ID, "messages": snapshot},
                    timeout=1,
                )
            except requests.RequestException:
                pass  # peer unreachable, just skip


if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    state = "OFFLINE" if offline else "online"
    print(f"[{MY_ID}] starting on :{MY_PORT}, peers={PEERS}, state={state}")
    threading.Thread(target=generate, daemon=True).start()
    threading.Thread(target=sync, daemon=True).start()
    app.run(host="0.0.0.0", port=MY_PORT)
