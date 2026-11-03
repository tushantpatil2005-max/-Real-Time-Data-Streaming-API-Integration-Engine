# Real-Time Data Streaming & API Integration Engine

A service that accepts events over REST and from an optional upstream WebSocket feed, validates them, saves them to
PostgreSQL, fans them out through Redis Pub/Sub, and streams them live to connected WebSocket clients. It applies
backpressure when overloaded, reconnects to the upstream feed automatically, and protects itself from slow clients.

The whole application is a single file, `main.py`.

**Stack:** Python (asyncio), FastAPI, asyncpg (PostgreSQL), redis-py (Redis Pub/Sub), websockets.

## Features

- REST ingestion (`POST /api/v1/events`) and an optional upstream WebSocket feed.
- Bounded in-memory queue with real backpressure: REST callers get `503` with `Retry-After`; the upstream feed waits
  instead of dropping data.
- Worker pool that saves events to PostgreSQL, with retries and idempotent writes.
- Redis Pub/Sub fan-out: every instance forwards events to its own WebSocket clients.
- Slow-client protection (bounded per-client outbox) and heartbeat monitoring.
- Automatic reconnection with capped exponential backoff and jitter.
- Graceful shutdown that drains the queue.
- Structured JSON logs, readiness and metrics endpoints, optional API key.
- Interactive API docs at `/docs`, OpenAPI spec at `/openapi.json`.

## Requirements

- Python 3.10 or newer
- PostgreSQL
- Redis

```bash
pip install fastapi "uvicorn[standard]" asyncpg "redis>=5" websockets
```

## Run

```bash
# Local PostgreSQL and Redis, if you do not have them
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=streaming_engine postgres:16
docker run -d -p 6379:6379 redis:7

uvicorn main:app          # or: python main.py
```

The `events` table is created on startup if it does not exist. Configuration comes from environment variables (see
[Configuration](#configuration)); to load a `.env` file use `uvicorn main:app --env-file .env`.

## Usage

```bash
# Submit an event
curl -X POST localhost:8000/api/v1/events -H "Content-Type: application/json" -d '{
  "source": "sensor-7",
  "event_type": "temperature",
  "payload": {"celsius": 21.5},
  "occurred_at": "2026-01-01T10:00:00Z"
}'

# Read it back (use the id from the response)
curl localhost:8000/api/v1/events/<id>

# List recent events, newest first
curl "localhost:8000/api/v1/events?source=sensor-7&limit=20"

# Health and metrics
curl localhost:8000/api/v1/health
curl localhost:8000/api/v1/metrics
```

Listen to the live stream:

```python
import asyncio, websockets

async def main():
    async with websockets.connect("ws://localhost:8000/ws/stream") as ws:
        async for text in ws:
            print(text)
            await ws.send("pong")      # any message counts as "I'm alive"

asyncio.run(main())
```

If `API_KEY` is set, send `X-API-Key: <key>` on REST requests, and on the WebSocket either the same header or
`?api_key=<key>` (browsers cannot set WebSocket headers). A key in the URL can end up in access logs; prefer the header.

## API

### `POST /api/v1/events`

| Field | Type | Rules |
|---|---|---|
| `source` | string | Required, 1 to 100 characters |
| `event_type` | string | Required, 1 to 100 characters |
| `payload` | object | Required, not empty, valid JSON without `NaN` or `Infinity`, at most `MAX_PAYLOAD_BYTES` |
| `occurred_at` | ISO-8601 timestamp | Optional. Timestamps without a zone are taken as UTC |
| `id` | UUID | Optional. Makes retries idempotent (see [Delivery guarantees](#delivery-guarantees)) |

| Status | Meaning |
|---|---|
| `202` | Queued. The body is the event; `processed_at` is `null` until it is saved |
| `401` | Missing or invalid API key |
| `413` | Payload larger than `MAX_PAYLOAD_BYTES` |
| `422` | Invalid event |
| `503` | Queue is full, or the service is shutting down. Includes `Retry-After: 1` |

### `GET /api/v1/events/{id}`

Returns the saved event, or `404`. An event that was accepted but not saved yet also returns `404`.

### `GET /api/v1/events`

Query parameters: `source`, `event_type`, `before` (only events received before this time), and `limit`
(1 to 200, default 50). Newest first. To page, pass the `received_at` of the last event as `before`.

### `GET /api/v1/health`

Checks PostgreSQL and Redis. Returns `200` with `status: "ok"`, or `503` with `status: "degraded"`. The body also
includes `database`, `redis`, `uptime_seconds`, `connected_websocket_clients`, `queue_depth`, `queue_capacity`,
`events_processed`, `events_rejected` and `events_failed`. Does not need an API key.

### `GET /api/v1/metrics`

Counters: `events_accepted`, `events_processed`, `events_duplicate`, `events_failed`, `events_rejected`,
`publish_failures`, `invalid_messages`, `slow_clients_dropped`, `clients_timed_out`, plus queue depth and capacity,
connected clients and uptime.

### `GET /live` and `GET /`

`/live` is a liveness probe. `/` returns a small banner with the documentation links. Neither needs an API key.

### `WS /ws/stream`

The server sends:

```json
{"type": "event", "data": {"id": "…", "source": "…", "event_type": "…", "payload": {}, "occurred_at": "…", "received_at": "…", "processed_at": "…"}}
{"type": "heartbeat", "ts": "2026-01-01T10:00:15+00:00"}
```

Clients must send any message (for example `pong`) at least every `CLIENT_PONG_TIMEOUT_SECONDS`, or the server
disconnects them. Set it to `0` to turn the check off. Close codes: `1008` unauthorised, `1013` server full or client
too slow, `1001` heartbeat timeout or server shutdown.

### Upstream feed

When `UPSTREAM_FEED_URL` is set, the service connects to it and reads one JSON event per message, with the same fields
as `POST /api/v1/events`. Invalid messages are counted in `invalid_messages` and skipped. The URL's credentials are never
logged; only the host name is.

## How it works

1. **Intake.** An event is validated and placed in a bounded queue. REST callers wait up to
   `INGEST_BACKPRESSURE_TIMEOUT_SECONDS`, then receive `503`. The upstream feed waits as long as needed, which stops it
   reading from its socket and pushes the pressure back to the source.
2. **Processing.** Workers save each event to PostgreSQL, retrying transient failures with backoff. The insert uses
   `ON CONFLICT (id) DO NOTHING`, so a repeated id is stored once and is not announced again.
3. **Fan-out.** After a successful save, the worker publishes the event to a Redis channel. Every instance subscribes
   to the channel and forwards messages to its own WebSocket clients, so clients see all events regardless of which
   instance saved them. If publishing fails, the worker delivers to its own clients directly.
4. **Clients.** Each WebSocket client has its own bounded outbox and writer task, so one slow client cannot delay the
   workers or other clients. A client whose outbox fills up is disconnected.
5. **Shutdown.** The service stops accepting events (new requests get `503`), stops the upstream feed, drains the queue
   for up to `DRAIN_TIMEOUT_SECONDS`, gives the last events a moment to reach clients, then closes everything.

Reconnects to the upstream feed and to the Redis subscription use capped exponential backoff with jitter. The backoff
only resets after a connection has stayed up for `RECONNECT_STABLE_SECONDS`, so a connection that drops immediately
cannot cause a tight loop, and a clean close is treated like any other disconnect.

## Delivery guarantees

Read this before relying on the service.

- **Accepted events live in memory until saved.** A crash or forced kill loses events that were queued but not yet saved.
  If an event cannot be saved after `PERSIST_MAX_ATTEMPTS` tries, it is counted in `events_failed` and logged; it is not
  retried later. For durability, put a durable queue in front of this service.
- **Fan-out is best effort.** WebSocket clients can miss events while disconnected, or when dropped as too slow.
  PostgreSQL is the source of truth: use `GET /api/v1/events` to catch up.
- **Idempotency needs a client-supplied `id`.** Without one, every request creates a new event. With one, retries and
  re-delivery after a reconnect are stored and announced once.
- **Redis messages are envelopes.** Other consumers of the Redis channel receive `{"type": "event", "data": {...}}`.

## Configuration

Each setting is an environment variable with the same name in upper case.

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | unset | When set, REST (except health and liveness) and WebSocket access require it |
| `POSTGRES_DSN` | `postgresql://postgres:postgres@localhost:5432/streaming_engine` | PostgreSQL connection string |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `2` / `10` | Connection pool size |
| `DB_COMMAND_TIMEOUT_SECONDS` | `10` | Database command timeout |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `REDIS_CHANNEL` | `events:stream` | Pub/Sub channel |
| `WORKER_POOL_SIZE` | `4` | Concurrent workers |
| `QUEUE_MAX_SIZE` | `1000` | Queue capacity |
| `INGEST_BACKPRESSURE_TIMEOUT_SECONDS` | `2.0` | How long REST callers wait on a full queue |
| `PERSIST_MAX_ATTEMPTS` | `4` | Save attempts per event |
| `PERSIST_RETRY_BASE_DELAY_SECONDS` / `PERSIST_RETRY_MAX_DELAY_SECONDS` | `0.2` / `2.0` | Save retry backoff |
| `DRAIN_TIMEOUT_SECONDS` | `15` | Longest wait for the queue to drain on shutdown |
| `SHUTDOWN_GRACE_SECONDS` | `0.5` | Pause so the last events reach clients before closing |
| `MAX_PAYLOAD_BYTES` | `65536` | Largest accepted payload |
| `HEARTBEAT_INTERVAL_SECONDS` | `15` | Heartbeat frame interval |
| `CLIENT_PONG_TIMEOUT_SECONDS` | `45` | Disconnect silent clients after this long; `0` disables |
| `CLIENT_QUEUE_SIZE` | `256` | Per-client outbox size |
| `MAX_WS_CLIENTS` | `1000` | Most concurrent WebSocket clients |
| `UPSTREAM_FEED_URL` | unset | Upstream WebSocket feed to consume; unset disables it |
| `RECONNECT_BASE_DELAY_SECONDS` / `RECONNECT_MAX_DELAY_SECONDS` | `1.0` / `30` | Reconnect backoff |
| `RECONNECT_STABLE_SECONDS` | `10` | A connection this old resets the backoff |
| `LOG_LEVEL` | `INFO` | Logging level |
| `APP_NAME` / `API_V1_STR` | service name / `/api/v1` | Title and REST path prefix |

## Logging

Logs are JSON, one object per line on standard output, with `timestamp` (UTC), `level`, `logger`, `message` and any
extra fields such as `event_id`, `worker_id` or `client_id`. Event payloads are never logged, and validation messages
never include payload content.

## Tests

```bash
pip install httpx
python -m unittest test_main -v
```

The tests replace PostgreSQL, Redis and the upstream feed with in-memory fakes, so no services are needed. They cover
validation, backoff, reconnect supervision (including no tight loop after a clean close), the WebSocket manager (slow
clients, client limits, heartbeats, timeouts), the engine (saving, exactly-once delivery, duplicates, retries,
publish failure, backpressure, graceful shutdown, readiness), the upstream feed, and the REST and WebSocket API.

They do not cover the real asyncpg and redis adapters. Try those against real PostgreSQL and Redis before relying on
them in production.

## Limitations

- Not included: rate limiting, per-client authorisation, batch ingestion, and schema migrations (the table is only
  created if missing).
- Listing uses a single `before` timestamp as its cursor. Events with identical receive times at a page boundary can be
  skipped.
- One WebSocket channel carries every event; there is no per-client filtering.
