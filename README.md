# 💬 Real-Time Chat Application

A scalable WebSocket-based messaging system with monotonic message ordering, transactional outbox, offline buffering, and heartbeat-based presence tracking — the architecture behind apps like Slack and Discord.

**Live:** [http://localhost:8003](http://localhost:8003)  
**Stack:** FastAPI · WebSockets · Python · Vanilla JS

---

## 📦 Architecture & Where Data Lives

This service features a **Multi-Node WebSocket Backplane** powered by Redis Pub/Sub with automatic in-memory fallback:

| Component | What it does | Implementation |
|-----------|--------------|----------------|
| `RedisPubSubBackplane` | Cross-node cluster message distribution | Redis Pub/Sub pattern `chat:rooms:*` via `redis.asyncio` (with auto-reconnection) |
| `In-Memory Fallback` | Local zero-dependency broadcast | Active whenever Redis is offline (prevents server crashes) |
| `OutboxRelay` | Transactional Outbox pattern | Background coroutine draining pending queue → publishing to Backplane |
| `MessageStore` | Deduplication index & message ledger | Partitioned by `room_id`, deduplicated by `message_id` |
| `SequenceGen` | Monotonic 64-bit sequence counters | Strictly increasing ordering counter per room |
| `OfflineQueue` | Buffer for disconnected clients | Flushed on reconnection (`offline_flush` event) |
| `PresenceTracker` | Heartbeat & liveness tracking | 30s TTL with background stale connection janitor |
| `ConnectionManager` | Local server instance WebSockets | Slow consumer detection (backpressure signal) |

---

## 🚀 How to Run

### Option A: Local Single-Node (Automatic In-Memory Fallback)
```bash
cd 03_chat_app
python -m uvicorn main:app --port 8003 --host 0.0.0.0 --reload
```

### Option B: Multi-Node Horizontal Scaling Cluster (with Redis)
Run two chat instances connected to the same Redis Pub/Sub backplane:
```bash
# Terminal 1 — Start Redis (Docker or local)
docker run -d --name redis -p 6379:6379 redis:alpine

# Terminal 2 — Start Chat Node 1 (Port 8003)
REDIS_URL=redis://localhost:6379 NODE_ID=node_1 python -m uvicorn main:app --port 8003

# Terminal 3 — Start Chat Node 2 (Port 8006)
REDIS_URL=redis://localhost:6379 NODE_ID=node_2 python -m uvicorn main:app --port 8006
```

> 💡 **Try it live:** Open `http://localhost:8003` in Window 1 and `http://localhost:8006` in Window 2. Messages sent to Node 1 instantly appear on Node 2 via Redis Pub/Sub!

---

## 🛠️ API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `WS`   | `/ws/{room_id}/{user_id}` | WebSocket connection — bidirectional messaging channel |
| `GET`  | `/api/pubsub-status` | Cluster Backplane status: mode (`redis_pubsub` vs `in_memory_fallback`), node ID, published & received counters |
| `GET`  | `/api/rooms` | List all chat rooms with online counts |
| `GET`  | `/api/rooms/{room_id}/messages` | Message history (paginated by sequence number) |
| `GET`  | `/api/rooms/{room_id}/presence` | Online users & slow consumer detection |
| `POST` | `/api/rooms` | Create a new room |
| `GET`  | `/api/health` | Health check (reports backplane mode and node ID) |

### WebSocket Message Protocol

**Client → Server:**

```json
// Send a message
{"type": "message", "text": "Hello!", "message_id": "client-generated-uuid"}

// Heartbeat (keep presence alive)
{"type": "heartbeat"}

// Acknowledge receipt of a message
{"type": "ack", "message_id": "msg-uuid"}
```

**Server → Client:**

```json
// Message ACK (sent back to sender)
{"type": "ack", "message_id": "msg-uuid", "seq": 42, "duplicate": false}

// Broadcast to room
{"type": "message", "user_id": "alice", "text": "Hello!", "seq": 42, "ts": "..."}

// Presence update
{"type": "presence", "event": "join", "user_id": "alice", "online_users": ["alice", "bob"]}

// Offline messages flushed on reconnect
{"type": "offline_flush", "messages": [...], "count": 3}
```

---

## ⚙️ Concepts Implemented

### 1. WebSockets — Full-Duplex Real-Time Communication
- HTTP is request-response (half-duplex). WebSockets upgrade the connection to a persistent full-duplex TCP channel.
- No polling needed: server **pushes** messages to clients instantly.
- FastAPI's `@app.websocket()` decorator handles the upgrade handshake automatically.

### 2. Monotonic Sequence Numbers
- Every message in a room gets a globally-ordered 64-bit integer sequence number (`seq`).
- Clients detect **gaps** in sequence: if they receive seq=45 after seq=43, they know seq=44 was dropped and can request it.
- Prevents message reordering even under network packet reordering.

```python
class SequenceGen:
    def next(self, room_id: str) -> int:
        self._counters[room_id] += 1
        return self._counters[room_id]
```

### 3. Transactional Outbox Pattern
- **Problem:** If you save a message to DB and then publish to WebSocket, and the publish fails — the message is lost.
- **Solution:** Write message + outbox entry atomically in the same transaction. A background relay worker reads the outbox and publishes, marking entries as delivered.
- Guarantees **at-least-once delivery** with deduplication via message IDs.

```
Client → Server:
  1. Save message to MessageStore  ─┐  (atomic — both or neither)
  2. Add to OutboxRelay._pending   ─┘
  3. Send ACK to sender

Background relay (every 50ms):
  4. Pop from outbox → broadcast to room WebSockets
```

### 4. Offline Queue & Reconnect Replay
- When a user disconnects, messages addressed to them are buffered in `OfflineQueue` (max 1,000 per user).
- On reconnect: server flushes all buffered messages before live stream begins.
- Prevents message loss during brief network blips.

### 5. Heartbeat Presence Tracking (TTL-based)
- Every 30 seconds, connected clients send `{"type": "heartbeat"}`.
- Server updates `_last_seen[user_id] = now`.
- A background cleanup coroutine (runs every 15s) removes users whose last heartbeat is >30s old.
- Prevents "ghost" online indicators for users with dead TCP connections.

### 6. Slow Consumer Detection (Backpressure)
- Tracks how long each `ws.send_json()` call takes per user.
- If send exceeds 100ms threshold → user is flagged as a "slow consumer".
- In production: slow consumers get buffered, rate-limited, or disconnected to prevent one slow client from blocking the broadcast loop.

### 7. Message Deduplication
- Clients generate `message_id` (UUID) before sending.
- Server checks `MessageStore._seen` set before saving.
- Safe retries on network failure — same message won't be stored twice.

---

## 💡 Interview Talking Points

- **"How do you scale WebSockets across multiple servers?"** → WebSocket connections are stateful (tied to one server). For horizontal scaling, use a **Redis Pub/Sub backplane**: Server 1 publishes message to Redis channel; all servers subscribe and forward to their local connected clients.
- **"How do you handle message ordering in distributed systems?"** → Per-room monotonic sequence numbers assigned by a single leader or a distributed counter (e.g. Cassandra LWT, Redis INCR). Clients detect gaps and can request history replay.
- **"What is the Transactional Outbox pattern?"** → A pattern that guarantees DB writes and message publishes happen atomically by writing both to the DB in one transaction, and using a separate relay process to publish — decoupling persistence from transport.
- **"How do you know a user is online?"** → Heartbeats with TTL. If no heartbeat within N seconds, presence is revoked. This handles TCP zombie connections that appear open but are actually dead.
