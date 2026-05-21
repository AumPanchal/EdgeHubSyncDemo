"""
Entry point. Reads config from env vars (set by docker-compose), wires the
four pieces together, and runs them concurrently on one asyncio loop.

Environment:
  NODE_ID    e.g. "A"
  REDIS_URL  e.g. "redis://redis-a:6379"
  PORT       HTTP port to listen on  (default 8080)
  PEERS      comma list, e.g. "B=http://node-b:8080,C=http://node-c:8080"
  GEN_RATE   messages per second     (default 0.5)
"""

import asyncio
import logging
import os

from state import NodeState
from storage import RedisStorage
from generator import MessageGenerator
from sync_agent import SyncAgent


def parse_peers(raw: str) -> dict[str, str]:
    peers = {}
    for spec in (s.strip() for s in raw.split(",") if s.strip()):
        pid, purl = spec.split("=", 1)
        peers[pid.strip()] = purl.strip()
    return peers


async def main():
    node_id = os.environ["NODE_ID"]
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    rate = float(os.environ.get("GEN_RATE", "0.5"))
    port = int(os.environ.get("PORT", "8080"))
    peers = parse_peers(os.environ.get("PEERS", ""))

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{node_id}] %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(__name__)
    log.info(f"booting node {node_id}, peers={list(peers)}")

    state = NodeState(node_id)
    storage = RedisStorage(redis_url)

    # Rebuild knowledge vector from persisted streams (restart recovery).
    # If the node was killed and restarted, this is how we pick up where we left off.
    try:
        recovered = await storage.rebuild_vector()
        for origin, seq in recovered.items():
            cur = state.knowledge_vector.get(origin, 0)
            state.knowledge_vector[origin] = max(cur, seq)
        if recovered:
            log.info(f"recovered vector from disk: {state.get_vector()}")
    except Exception as e:
        log.warning(f"recovery failed (probably first boot): {e}")

    generator = MessageGenerator(node_id, state, storage, rate)
    sync = SyncAgent(node_id, state, storage, peers, port)

    await asyncio.gather(generator.run(), sync.run())


if __name__ == "__main__":
    asyncio.run(main())
