"""
MessageGenerator — pretends to be the CoT producer.

Generates fake Cursor-on-Target XML at a configurable rate. The point is to
keep messages flowing so the sync layer has work to do. Each message is written
locally and the node's own entry in the knowledge vector is bumped.

This is Event 1 in the design doc: "Local write". Works whether or not the node
is connected to anything — the offline-first property.
"""

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class MessageGenerator:
    def __init__(self, node_id, state, storage, rate_per_sec: float = 0.5):
        self.node_id = node_id
        self.state = state
        self.storage = storage
        self.interval = 1.0 / rate_per_sec

    def _make_cot_xml(self, seq: int) -> str:
        ts = time.time()
        return (
            f'<event uid="{self.node_id}-{seq}" type="a-f-G-U-C" time="{ts:.3f}">'
            f'<point lat="45.0" lon="-75.0" hae="0"/>'
            f"<detail><contact callsign=\"{self.node_id}{seq}\"/></detail>"
            f"</event>"
        )

    async def run(self):
        while True:
            await asyncio.sleep(self.interval)
            seq = self.state.record_local_write()
            payload = self._make_cot_xml(seq)
            await self.storage.write_message(self.node_id, seq, payload, int(time.time()))
            logger.info(f"[GEN] wrote {self.node_id}:{seq}")
