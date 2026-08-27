Real-Time Data Streaming & API Integration Engine
An event-driven data streaming engine that ingests, validates, and persists
high-velocity feeds, with resilient reconnection, backpressure flow
control, heartbeat monitoring, and real-time WebSocket fan-out.
Stack: Python, AsyncIO, WebSockets, PostgreSQL, Redis Pub/Sub, FastAPI
Features
Event-driven ingestion — REST endpoint plus an optional upstream
WebSocket feed, both funneling into the same async worker pool
Async worker pool — validates, persists, and fans out events
concurrently instead of processing them one at a time
Real backpressure — a bounded asyncio.Queue means producers are
actually slowed or rejected (HTTP 503) once capacity is hit, rather than
buffering unboundedly in memory
Automatic reconnection — the upstream feed client reconnects with
capped exponential backoff + jitter on any disconnect, so a single
network blip doesn't interrupt ingestion
Heartbeat monitoring — periodic heartbeat frames pushed to every
connected WebSocket client, with stale connections cleaned up automatically
Structured JSON logging — every log line is machine-parseable, ready
to ship to a log aggregator
Interactive API docs — FastAPI auto-generates Swagger UI (/docs)
and a machine-readable OpenAPI spec (/openapi.json) you can import
straight into Postman
Requirements
Python 3.11+
A running PostgreSQL instance
A running Redis instance
Setup
Bash
Configure via environment variables or a .env file in the project root:
Env
Run it:
Bash
The events table is created automatically on startup. Interactive docs:
http://localhost:8000/docs
Architecture
Code
Backpressure
BackpressureQueue wraps a bounded asyncio.Queue. StreamEngine.ingest
awaits queue.put() with a timeout; if the queue is full and stays full
past that timeout, the request is rejected with 503 rather than being
buffered indefinitely — this is what keeps memory bounded under sustained
overload instead of degrading silently.
Reconnection
ReconnectingFeedClient wraps the upstream WebSocket connection in a retry
loop. On any exception (including a clean disconnect) it waits
base_delay * 2^attempt seconds, capped at RECONNECT_MAX_DELAY_SECONDS
and randomized with jitter, then reconnects. The attempt counter resets
after every successful connection.
Heartbeat
Every HEARTBEAT_INTERVAL_SECONDS, the engine pushes a heartbeat frame to
each connected WebSocket client. Send failures mark that connection stale
and it's cleaned up immediately, so connected_websocket_clients in
/api/v1/health always reflects live connections.
API
Method
Path
Description
POST
/api/v1/events
Ingest a single event (202 or 503 if saturated)
GET
/api/v1/health
Liveness/readiness + queue/connection stats
GET
/api/v1/metrics
Throughput and backpressure metrics
WS
/ws/stream
Real-time event + heartbeat fan-out
GET
/docs
Interactive Swagger UI
GET
/openapi.json
OpenAPI spec (importable into Postman)
Example flow
Bash
Notes on scope
The upstream feed client is only started if UPSTREAM_FEED_URL is set;
without it, the engine just serves REST ingestion + WebSocket fan-out.
Postgres writes happen per-event inside each worker; at very high
throughput you'd batch inserts (e.g. COPY or multi-row INSERT) rather
than one round-trip per event.
The table is created via a raw CREATE TABLE IF NOT EXISTS on startup
rather than migrations — fine for a demo, swap in Alembic for anything
long-lived.
Redis Pub/Sub here fans out to other backend instances/subscribers, which
is separate from the WebSocket broadcast to browser/API clients.
License
MIT