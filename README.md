# Mesh Sync Demo

A small working demo of the inter-node sync design — three nodes, each with their
own Redis (AOF on) and a Python sync agent. Nodes generate fake CoT messages,
exchange knowledge vectors on connect, push what's missing, and recover cleanly
from partitions and restarts.

## What's in here

```
mesh-sync-demo/
├── docker-compose.yml      3 nodes + 3 Redis instances
└── node/
    ├── Dockerfile
    ├── requirements.txt
    ├── main.py             entry point — wires the four pieces together
    ├── state.py            knowledge vector + per-peer cursor model
    ├── storage.py          Redis Streams interface + restart recovery
    ├── generator.py        fake CoT message producer
    └── sync_agent.py       HTTP server + peer connection state machines
```

The four pieces inside a node match the design doc one-to-one:
**generator → storage → sync agent → state**.

## Run it

You need Docker + Docker Compose. From the project root:

```bash
docker compose up --build
```

You'll see logs like:

```
[A] [GEN] wrote A:1
[A] [EDGE-UP] B reachable — handshake done
[A] [TX] sent 1 msgs to B
[B] [RX] from A: 1 new
```

## Demo flow (what to show on screen)

### 1. Happy path — watch the mesh converge

After `docker compose up --build`, leave it running 15–20 seconds, then:

```bash
curl -s localhost:8081/status | jq    # node A
curl -s localhost:8082/status | jq    # node B
curl -s localhost:8083/status | jq    # node C
```

All three `knowledge_vector` fields should converge — each node knows the latest
seq from every other node.

### 2. Partition — kill a node, keep generating

```bash
docker compose stop node-c
```

A and B keep generating and sync to each other. Their logs show edge-down on C
with exponential backoff. C's stream piles up in Redis on A and B, waiting.

### 3. Reconnect — clean catchup, no dupes, no loss

```bash
docker compose start node-c
```

You'll see:
- C boots, calls `rebuild_vector` and recovers its own state from Redis.
- A and B detect C is back, vector handshake fires.
- A and B push everything C is missing in batches of 50.
- C's status endpoint now shows the same vector as A and B — fully caught up.

Crucially, no message is sent twice. The vector handshake is what makes that
work — B doesn't have to remember "I told C about A already"; the vector tells
it C's state at the moment of reconnection.

### 4. Restart recovery — kill, restart, resume

```bash
docker compose restart node-a
```

Node A comes back, reads its Redis streams off disk, rebuilds its knowledge
vector, then re-handshakes with B and C. No data loss.

## Tear down

```bash
docker compose down -v     # -v also wipes the Redis volumes
```

## Things to point out during the demo

- **Knowledge vector vs. cursor.** Vector tracks content ("what exists in the
  world that I know about"). Cursor tracks transport ("what have I shipped to
  this peer"). Mixed up → confusing bugs.
- **No central server.** Every node is symmetric. If two nodes can see each
  other they sync; if not, they wait and retry.
- **Batch cap.** `BATCH_SIZE = 50` in `sync_agent.py` keeps a long catchup
  burst from blocking steady-state writes — the send loop returns to the outer
  scheduler between batches.
- **Exponential backoff with jitter, capped at 30s.** No tight retry loops
  burning CPU when peers are down.
- **AOF persistence on Redis.** Survives container restarts. The vector is
  rebuildable from the streams, so all durable state lives in Redis.
