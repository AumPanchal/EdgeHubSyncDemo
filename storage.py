"""
RedisStorage — thin wrapper over Redis Streams.

One stream per origin node: stream:A, stream:B, stream:C, ...
We key by origin so range queries during catchup are simple.

Redis is started with AOF (`--appendonly yes`) so messages survive a node restart.
"""

import redis.asyncio as redis


class RedisStorage:
    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url, decode_responses=True)

    async def write_message(self, origin: str, seq: int, payload: str, timestamp: int):
        """Append a message. Idempotent at the application level via the seq check upstream."""
        await self.redis.xadd(
            f"stream:{origin}",
            {
                "origin": origin,
                "seq": str(seq),
                "timestamp": str(timestamp),
                "payload": payload,
            },
        )

    async def read_range(self, origin: str, from_seq: int, to_seq: int) -> list[dict]:
        """
        Return all messages from origin with from_seq <= seq <= to_seq.
        Naive scan — fine for the demo. In a real build you'd index by seq.
        """
        entries = await self.redis.xrange(f"stream:{origin}", "-", "+")
        result = []
        for _entry_id, fields in entries:
            seq = int(fields["seq"])
            if from_seq <= seq <= to_seq:
                result.append(
                    {
                        "origin": fields["origin"],
                        "seq": seq,
                        "timestamp": int(fields["timestamp"]),
                        "payload": fields["payload"],
                    }
                )
        return result

    async def rebuild_vector(self) -> dict[str, int]:
        """
        On startup, scan all streams to rebuild the knowledge vector from disk.
        This is what makes restart-recovery work — the vector lives in RAM but
        the data lives in Redis, so we can always reconstruct.
        """
        vector = {}
        keys = await self.redis.keys("stream:*")
        for key in keys:
            origin = key.split(":", 1)[1]
            entries = await self.redis.xrange(key, "-", "+")
            max_seq = 0
            for _eid, fields in entries:
                s = int(fields["seq"])
                if s > max_seq:
                    max_seq = s
            if max_seq > 0:
                vector[origin] = max_seq
        return vector

    async def count_all(self) -> int:
        total = 0
        for key in await self.redis.keys("stream:*"):
            total += await self.redis.xlen(key)
        return total
