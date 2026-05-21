"""
SyncAgent — the protocol logic. Two halves:

  Receiver (HTTP server)
    POST /vector    Peer sends their knowledge vector, we send ours.
                    This is Event 2: peer discovery / handshake.
    POST /messages  Peer pushes a batch of messages. We dedupe via the vector,
                    persist new ones, and ACK with our updated vector so the peer
                    can advance its per-peer cursor.
    GET  /status    Inspect current state — handy during the demo.

  Sender (one task per peer)
    Loop: try to sync with the peer.
      - On success: do vector handshake, figure out what they're missing, push it
        in capped batches (so a long catchup can't starve fresh local writes).
      - On failure: mark edge-down, exponential backoff with jitter capped at 30s,
        then try again. When peer reappears the handshake figures out the rest.
"""

import asyncio
import logging
import random
from aiohttp import web, ClientSession

logger = logging.getLogger(__name__)

BATCH_SIZE = 50  # cap catchup bursts; lets steady-state writes interleave
HEALTHY_INTERVAL = 2.0  # how often to sync with a healthy peer


class SyncAgent:
    def __init__(self, node_id, state, storage, peers: dict, port: int = 8080):
        self.node_id = node_id
        self.state = state
        self.storage = storage
        self.peers = peers  # {peer_id: "http://host:port"}
        self.port = port
        self.peer_up = {p: False for p in peers}
        self.peer_backoff = {p: 1.0 for p in peers}
        self._session: ClientSession | None = None

    # ====================================================================
    # Receiver side (HTTP server)
    # ====================================================================

    async def _handle_vector(self, request):
        """Handshake. Peer sends vector, we update our model and send ours back."""
        body = await request.json()
        peer_id = body["from"]
        self.state.update_peer_vector(peer_id, body["vector"])
        return web.json_response(
            {"from": self.node_id, "vector": self.state.get_vector()}
        )

    async def _handle_messages(self, request):
        """Receive a batch. Dedupe via vector, persist new ones, ACK with our vector."""
        body = await request.json()
        peer_id = body["from"]
        messages = body["messages"]
        new_count = 0
        for msg in messages:
            origin, seq = msg["origin"], int(msg["seq"])
            if self.state.record_received(origin, seq):
                await self.storage.write_message(
                    origin, seq, msg["payload"], int(msg["timestamp"])
                )
                new_count += 1
        if messages:
            dupes = len(messages) - new_count
            logger.info(
                f"[RX] from {peer_id}: {new_count} new"
                + (f", {dupes} dupes" if dupes else "")
            )
        return web.json_response(
            {
                "from": self.node_id,
                "vector": self.state.get_vector(),
                "received": len(messages),
            }
        )

    async def _handle_status(self, request):
        return web.json_response(
            {
                "node_id": self.node_id,
                "knowledge_vector": self.state.get_vector(),
                "peer_models": self.state.peer_vectors,
                "peer_up": self.peer_up,
                "total_messages_stored": await self.storage.count_all(),
            }
        )

    async def _start_server(self):
        app = web.Application()
        app.router.add_post("/vector", self._handle_vector)
        app.router.add_post("/messages", self._handle_messages)
        app.router.add_get("/status", self._handle_status)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()
        logger.info(f"[SRV] listening on :{self.port}")

    # ====================================================================
    # Sender side (one task per peer)
    # ====================================================================

    async def _sync_once(self, peer_id: str, peer_url: str) -> bool:
        """One round-trip cycle: handshake + push everything peer is missing."""
        try:
            # --- handshake (Event 2) ---
            async with self._session.post(
                f"{peer_url}/vector",
                json={"from": self.node_id, "vector": self.state.get_vector()},
                timeout=5,
            ) as r:
                if r.status != 200:
                    raise RuntimeError(f"vector exchange status {r.status}")
                data = await r.json()
                self.state.update_peer_vector(peer_id, data["vector"])

            if not self.peer_up[peer_id]:
                logger.info(f"[EDGE-UP] {peer_id} reachable — handshake done")
            self.peer_up[peer_id] = True
            self.peer_backoff[peer_id] = 1.0

            # --- push what they're missing (Event 3) ---
            needs = self.state.messages_peer_needs(peer_id)
            total_sent = 0
            # Round-robin across origins so no single origin starves the others.
            origins = list(needs.keys())
            random.shuffle(origins)
            for origin in origins:
                from_seq, to_seq = needs[origin]
                cur = from_seq
                while cur <= to_seq:
                    batch_end = min(cur + BATCH_SIZE - 1, to_seq)
                    msgs = await self.storage.read_range(origin, cur, batch_end)
                    if not msgs:
                        break
                    async with self._session.post(
                        f"{peer_url}/messages",
                        json={"from": self.node_id, "messages": msgs},
                        timeout=10,
                    ) as r2:
                        if r2.status != 200:
                            raise RuntimeError(f"push status {r2.status}")
                        ack = await r2.json()
                        for m in msgs:
                            self.state.advance_peer_cursor(
                                peer_id, m["origin"], m["seq"]
                            )
                        self.state.update_peer_vector(peer_id, ack["vector"])
                    total_sent += len(msgs)
                    cur = batch_end + 1
            if total_sent:
                logger.info(f"[TX] sent {total_sent} msgs to {peer_id}")
            return True

        except Exception as e:
            if self.peer_up[peer_id]:
                logger.warning(f"[EDGE-DOWN] {peer_id}: {type(e).__name__}: {e}")
            self.peer_up[peer_id] = False
            return False

    async def _peer_loop(self, peer_id: str, peer_url: str):
        """One forever-loop per peer. Healthy = fast tick. Down = exp backoff."""
        # Tiny stagger so peers don't all probe simultaneously on startup.
        await asyncio.sleep(random.uniform(0, 1.0))
        while True:
            ok = await self._sync_once(peer_id, peer_url)
            if ok:
                await asyncio.sleep(HEALTHY_INTERVAL + random.random())
            else:
                # exponential backoff + jitter, capped (Event 4)
                self.peer_backoff[peer_id] = min(self.peer_backoff[peer_id] * 2, 30.0)
                wait = self.peer_backoff[peer_id] + random.uniform(
                    0, self.peer_backoff[peer_id] * 0.3
                )
                await asyncio.sleep(wait)

    async def run(self):
        self._session = ClientSession()
        await self._start_server()
        await asyncio.gather(
            *(self._peer_loop(pid, purl) for pid, purl in self.peers.items())
        )
