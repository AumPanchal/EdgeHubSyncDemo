# Mesh Demo

Three nodes that share messages with each other. Each one generates messages, and every other node ends up with them.

## Setup

```
pip3 install -r requirements.txt
```

## Basic run (all 3 nodes online)

Open three terminals.

```
# Terminal 1
python3 node.py A 8001 B=8002,C=8003

# Terminal 2
python3 node.py B 8002 A=8001,C=8003

# Terminal 3
python3 node.py C 8003 A=8001,B=8002
```

You'll see all three nodes generating and receiving messages from each other.

## Simulating a disconnection

From a 4th terminal, take any node offline:

```
curl -X POST localhost:8001/offline    # A goes offline
curl -X POST localhost:8001/online     # A comes back
```

## The walkthrough scenario

This shows that **B can forward A's data to C even after A is offline**. No loss, no duplicates.

**Step 1.** Open terminals 1 and 2 only. Start A and B normally:

```
# Terminal 1
python3 node.py A 8001 B=8002,C=8003

# Terminal 2
python3 node.py B 8002 A=8001,C=8003
```

Wait about 10 seconds. A and B will exchange messages. B now holds A1, A2, A3, etc.

**Step 2.** In terminal 3, start C **offline**:

```
python3 node.py C 8003 A=8001,B=8002 offline
```

C is silent. It can't talk to anyone, can't be reached.

**Step 3.** In a 4th terminal, take A offline and bring C online:

```
curl -X POST localhost:8001/offline
curl -X POST localhost:8003/online
```

**Step 4.** Watch terminal 3 (C). You'll see something like:

```
========== [C] BACK ONLINE ==========

  [C] <- B: got A1, A2, A3, A4, B1, B2, B3, B4
```

C now holds A's data. Even though A is unreachable. B forwarded it.
That's mesh sync.
