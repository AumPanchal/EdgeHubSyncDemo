"""
NodeState — the two pieces of state every node carries.

  knowledge_vector : origin_node_id -> highest seq # I've ever seen from that origin.
                     "What messages exist in the world that I know about?"
                     This is the vector clock. It's what makes reconciliation work
                     and what prevents looping.

  peer_vectors     : peer_id -> {origin_node_id -> highest seq # I'm sure they have}
                     "What does each peer's knowledge look like, from my point of view?"
                     This is bookkeeping for transport. Updated from handshakes and ACKs.

Keeping these two separate is important — the vector tracks *content*, the cursors
track *transport*. Mixing them gets confusing fast when debugging.

Note: no locks here. Everything runs on one asyncio event loop, so dict ops are
atomic between awaits. We never await inside these methods.
"""


class NodeState:
    def __init__(self, my_node_id: str):
        self.my_node_id = my_node_id
        self.knowledge_vector: dict[str, int] = {my_node_id: 0}
        self.peer_vectors: dict[str, dict[str, int]] = {}

    # ---------- knowledge vector ----------

    def record_local_write(self) -> int:
        """Bump my own seq # and return the new value."""
        self.knowledge_vector[self.my_node_id] += 1
        return self.knowledge_vector[self.my_node_id]

    def record_received(self, origin: str, seq: int) -> bool:
        """Update vector for a received msg. Returns True if it's new, False if dupe."""
        current = self.knowledge_vector.get(origin, 0)
        if seq > current:
            self.knowledge_vector[origin] = seq
            return True
        return False

    def get_vector(self) -> dict[str, int]:
        return dict(self.knowledge_vector)

    # ---------- peer model ----------

    def update_peer_vector(self, peer_id: str, vector: dict[str, int]):
        """Called after a handshake or an ACK — we now know peer is at least this far."""
        existing = self.peer_vectors.get(peer_id, {})
        merged = dict(existing)
        for origin, seq in vector.items():
            if seq > merged.get(origin, 0):
                merged[origin] = seq
        self.peer_vectors[peer_id] = merged

    def messages_peer_needs(self, peer_id: str) -> dict[str, tuple[int, int]]:
        """
        For each origin, what range of seqs does the peer need?
        Returns {origin: (from_seq, to_seq)} inclusive. Empty if peer is caught up.
        """
        peer_vec = self.peer_vectors.get(peer_id, {})
        needs = {}
        for origin, our_seq in self.knowledge_vector.items():
            their_seq = peer_vec.get(origin, 0)
            if our_seq > their_seq:
                needs[origin] = (their_seq + 1, our_seq)
        return needs

    def advance_peer_cursor(self, peer_id: str, origin: str, seq: int):
        """We just confirmed peer has origin:seq (because we sent it and they ACKed)."""
        if peer_id not in self.peer_vectors:
            self.peer_vectors[peer_id] = {}
        if seq > self.peer_vectors[peer_id].get(origin, 0):
            self.peer_vectors[peer_id][origin] = seq
