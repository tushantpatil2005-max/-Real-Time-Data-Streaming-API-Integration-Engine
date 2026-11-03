"""
Real-Time Data Streaming & API Integration Engine
=================================================
Accepts events over REST and from an optional upstream WebSocket feed, validates them, persists them to
PostgreSQL, fans them out through Redis Pub/Sub, and streams them to connected WebSocket clients.

Stack: Python (asyncio), FastAPI, asyncpg (PostgreSQL), redis-py (Redis Pub/Sub), websockets.

Install:   pip install fastapi "uvicorn[standard]" asyncpg "redis>=5" websockets
Run:       uvicorn main:app             (or: python main.py)
Test:      pip install httpx && python -m unittest test_main -v

Requires PostgreSQL and Redis. Configuration is read from environment variables (names are the upper-case
field names of `Settings`); with uvicorn use `--env-file .env`. Interactive docs: /docs. Spec: /openapi.json.

How it works
  1. REST (POST /api/v1/events) or the upstream feed submits an event to a bounded in-memory queue.
     REST callers wait up to INGEST_BACKPRESSURE_TIMEOUT_SECONDS and then get 503 + Retry-After. The feed
     waits as long as needed, which pushes backpressure to the upstream socket instead of dropping data.
  2. A pool of workers saves each event to PostgreSQL (retrying transient failures). Saves are idempotent on
     the event id, so a retry or a re-sent event is stored once and announced once.
  3. After a successful save the worker publishes the event to a Redis channel. Every instance subscribes to
     that channel and forwards messages to its own WebSocket clients, so clients see all events no matter
     which instance handled them. If Redis is down, the worker delivers to its local clients directly.
  4. Each WebSocket client has a bounded outbox and its own writer task, so one slow client cannot stall
     others; clients that fall behind are disconnected. The server sends heartbeats; clients must send any
     message (for example "pong") within CLIENT_PONG_TIMEOUT_SECONDS or they are disconnected (0 disables).
  5. On shutdown the engine stops accepting events, drains the queue (up to DRAIN_TIMEOUT_SECONDS), then closes.

Delivery guarantees (read before relying on this)
  * Accepted events live in memory until saved. A crash or kill loses events that were queued but not yet
    saved, and an event whose save fails after all retries is counted (events_failed) and logged, not retried
    later. For durability, put a durable queue in front of this service.
  * Fan-out is best effort: WebSocket clients may miss events while disconnected or when dropped as slow.
    PostgreSQL is the source of truth; use GET /api/v1/events to catch up.
  * Idempotency needs a client-supplied "id" (UUID). Without one, every request creates a new event.

Not included: rate limiting, per-client authorisation, schema migrations (the events table is created if
missing), batch ingestion.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Protocol
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger("streaming_engine")


# ============================================================================
# Settings
# ============================================================================

@dataclass(frozen=True)
class Settings:
    app_name: str = "Real-Time Data Streaming & API Integration Engine"
    api_v1_str: str = "/api/v1"
    api_key: Optional[str] = None                      # when set, REST and WebSocket access require it

    postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/streaming_engine"
    db_pool_min: int = 2
    db_pool_max: int = 10
    db_command_timeout_seconds: float = 10.0

    redis_url: str = "redis://localhost:6379/0"
    redis_channel: str = "events:stream"

    worker_pool_size: int = 4
    queue_max_size: int = 1000
    ingest_backpressure_timeout_seconds: float = 2.0
    persist_max_attempts: int = 4
    persist_retry_base_delay_seconds: float = 0.2
    persist_retry_max_delay_seconds: float = 2.0
    drain_timeout_seconds: float = 15.0
    shutdown_grace_seconds: float = 0.5                # lets the last published events reach clients
    max_payload_bytes: int = 65_536

    heartbeat_interval_seconds: float = 15.0
    client_pong_timeout_seconds: float = 45.0          # 0 disables the check
    client_queue_size: int = 256
    max_ws_clients: int = 1000

    upstream_feed_url: Optional[str] = None
    reconnect_base_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 30.0
    reconnect_stable_seconds: float = 10.0             # a connection this old resets the backoff

    log_level: str = "INFO"

    def __post_init__(self) -> None:
        positive = ("db_pool_min", "db_pool_max", "db_command_timeout_seconds", "worker_pool_size", "queue_max_size",
                    "ingest_backpressure_timeout_seconds", "persist_max_attempts", "persist_retry_base_delay_seconds",
                    "persist_retry_max_delay_seconds", "drain_timeout_seconds", "max_payload_bytes",
                    "heartbeat_interval_seconds", "client_queue_size", "max_ws_clients",
                    "reconnect_base_delay_seconds", "reconnect_max_delay_seconds")
        for name in positive:
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("db_pool_min must not exceed db_pool_max")
        if self.client_pong_timeout_seconds < 0 or self.reconnect_stable_seconds < 0 or self.shutdown_grace_seconds < 0:
            raise ValueError("timeouts must not be negative")

    @classmethod
    def from_env(cls) -> "Settings":
        """Read each field from the environment variable with the same name in upper case."""
        values: Dict[str, Any] = {}
        for f in fields(cls):
            raw = os.getenv(f.name.upper())
            if raw is None or raw == "":
                continue
            try:
                values[f.name] = type(f.default)(raw) if isinstance(f.default, (int, float)) else raw
            except ValueError:
                raise ValueError(f"{f.name.upper()} has an invalid value: {raw!r}") from None
        if "log_level" in values:
            values["log_level"] = values["log_level"].upper()
        return cls(**values)


# ============================================================================
# Structured (JSON) logging
# ============================================================================

_STANDARD_RECORD_KEYS = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _STANDARD_RECORD_KEYS and key not in payload:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


# ============================================================================
# Domain: events, validation, errors
# ============================================================================

class InvalidEvent(Exception):
    """The event is malformed. The message never includes payload content, so it is safe to log and return."""
    status_code = 422


class PayloadTooLarge(InvalidEvent):
    status_code = 413


class EngineUnavailable(Exception):
    """The engine cannot take the event right now (clients should retry)."""


class BackpressureError(EngineUnavailable):
    pass


class EngineStopped(EngineUnavailable):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_utc(value: datetime) -> datetime:
    """Naive timestamps are taken to be UTC."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


@dataclass
class Event:
    id: uuid.UUID
    source: str
    event_type: str
    payload: Dict[str, Any]
    received_at: datetime
    occurred_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        iso = lambda d: d.isoformat() if d else None
        return {"id": str(self.id), "source": self.source, "event_type": self.event_type, "payload": self.payload,
                "occurred_at": iso(self.occurred_at), "received_at": iso(self.received_at),
                "processed_at": iso(self.processed_at)}

    def to_message(self) -> str:
        """The envelope sent to Redis and to WebSocket clients."""
        return json.dumps({"type": "event", "data": self.to_dict()}, allow_nan=False)


def _clean_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidEvent(f"{label} must be a non-empty string")
    value = value.strip()
    if len(value) > 100:
        raise InvalidEvent(f"{label} must be at most 100 characters")
    return value


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        try:
            return to_utc(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
        except ValueError:
            pass
    raise InvalidEvent("occurred_at must be an ISO-8601 timestamp")


def _parse_id(value: Any) -> Optional[uuid.UUID]:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except ValueError:
        raise InvalidEvent("id must be a UUID") from None


def build_event(source: Any, event_type: Any, payload: Any, *, occurred_at: Any = None, event_id: Any = None,
                max_payload_bytes: int = 65_536) -> Event:
    """The single place where event rules are enforced, for both REST and the upstream feed."""
    source, event_type = _clean_name(source, "source"), _clean_name(event_type, "event_type")
    if not isinstance(payload, dict) or not payload:
        raise InvalidEvent("payload must be a non-empty JSON object")
    try:
        encoded = json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise InvalidEvent("payload must be valid JSON without NaN or Infinity values") from None
    if len(encoded.encode("utf-8")) > max_payload_bytes:
        raise PayloadTooLarge(f"payload exceeds {max_payload_bytes} bytes")
    return Event(id=_parse_id(event_id) or uuid.uuid4(), source=source, event_type=event_type, payload=payload,
                 received_at=utcnow(), occurred_at=_parse_datetime(occurred_at))


def parse_event_message(raw: Any, max_payload_bytes: int) -> Event:
    """Parse one message from the upstream feed."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        raise InvalidEvent("message is not valid JSON") from None
    if not isinstance(data, dict):
        raise InvalidEvent("message must be a JSON object")
    return build_event(data.get("source"), data.get("event_type"), data.get("payload"),
                       occurred_at=data.get("occurred_at"), event_id=data.get("id"),
                       max_payload_bytes=max_payload_bytes)


@dataclass
class Metrics:
    accepted: int = 0
    rejected: int = 0              # refused because the queue was full or the engine was stopping
    processed: int = 0             # saved for the first time and announced
    duplicates: int = 0            # already stored; not announced again
    failed: int = 0                # could not be saved after all retries
    publish_failures: int = 0
    invalid_messages: int = 0      # bad messages from the upstream feed
    slow_clients_dropped: int = 0
    clients_timed_out: int = 0

    def snapshot(self) -> Dict[str, int]:
        return asdict(self)


def backoff_delay(attempt: int, base: float, cap: float, rand: Callable[[], float] = random.random) -> float:
    """Exponential backoff capped at `cap`, with jitter between 50% and 100% of the delay."""
    return min(cap, base * (2 ** attempt)) * (0.5 + 0.5 * rand())


async def supervise(name: str, run_once: Callable[[], Awaitable[None]], stop: asyncio.Event, *, base: float,
                    cap: float, stable_seconds: float, rand: Callable[[], float] = random.random) -> None:
    """Run `run_once` until `stop` is set, restarting it after every exit (clean or not) with backoff.
    The backoff resets only after a run lasted at least `stable_seconds`, so a connection that drops
    immediately cannot cause a tight reconnect loop."""
    attempt = 0
    while not stop.is_set():
        started = time.monotonic()
        try:
            await run_once()
            logger.info("%s_ended", name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                                  # log the class only; messages may hold secrets
            logger.warning("%s_failed", name, extra={"error": type(exc).__name__})
        if stop.is_set():
            return
        if time.monotonic() - started >= stable_seconds:
            attempt = 0
        delay = backoff_delay(attempt, base, cap, rand)
        attempt += 1
        logger.info("%s_retrying", name, extra={"retry_in_seconds": round(delay, 2)})
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)    # a stop request interrupts the wait


# ============================================================================
# Storage and message-bus adapters
# ============================================================================

class EventStore(Protocol):
    async def init(self) -> None: ...
    async def save(self, event: Event) -> bool: ...              # True if stored, False if the id already existed
    async def get(self, event_id: uuid.UUID) -> Optional[Event]: ...
    async def list(self, *, source: Optional[str], event_type: Optional[str], before: Optional[datetime],
                   limit: int) -> List[Event]: ...
    async def ping(self) -> None: ...
    async def close(self) -> None: ...


class MessageBus(Protocol):
    async def publish(self, channel: str, message: str) -> None: ...
    def messages(self, channel: str) -> AsyncIterator[str]: ...
    async def ping(self) -> None: ...
    async def close(self) -> None: ...


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_events_source_type ON events (source, event_type);
CREATE INDEX IF NOT EXISTS ix_events_received_at ON events (received_at);
"""
_COLUMNS = "id, source, event_type, payload, occurred_at, received_at, processed_at"


class PostgresEventStore:
    def __init__(self, cfg: Settings) -> None:
        self._cfg = cfg
        self._pool = None

    async def init(self) -> None:
        import asyncpg

        self._pool = await asyncpg.create_pool(
            dsn=self._cfg.postgres_dsn, min_size=self._cfg.db_pool_min, max_size=self._cfg.db_pool_max,
            command_timeout=self._cfg.db_command_timeout_seconds)
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)

    async def save(self, event: Event) -> bool:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                f"INSERT INTO events ({_COLUMNS}) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7) "
                "ON CONFLICT (id) DO NOTHING",
                event.id, event.source, event.event_type, json.dumps(event.payload, allow_nan=False),
                event.occurred_at, event.received_at, event.processed_at)
        return status == "INSERT 0 1"

    @staticmethod
    def _to_event(row) -> Event:
        payload = row["payload"]
        return Event(id=row["id"], source=row["source"], event_type=row["event_type"],
                     payload=json.loads(payload) if isinstance(payload, str) else payload,
                     received_at=row["received_at"], occurred_at=row["occurred_at"], processed_at=row["processed_at"])

    async def get(self, event_id: uuid.UUID) -> Optional[Event]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT {_COLUMNS} FROM events WHERE id = $1", event_id)
        return self._to_event(row) if row else None

    async def list(self, *, source, event_type, before, limit) -> List[Event]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_COLUMNS} FROM events WHERE ($1::text IS NULL OR source = $1) "
                "AND ($2::text IS NULL OR event_type = $2) AND ($3::timestamptz IS NULL OR received_at < $3) "
                "ORDER BY received_at DESC, id DESC LIMIT $4", source, event_type, before, limit)
        return [self._to_event(r) for r in rows]

    async def ping(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()


class RedisBus:
    def __init__(self, url: str) -> None:
        import redis.asyncio as aioredis

        self._client = aioredis.from_url(url, decode_responses=True)

    async def publish(self, channel: str, message: str) -> None:
        await self._client.publish(channel, message)

    async def messages(self, channel: str) -> AsyncIterator[str]:
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        try:
            async for item in pubsub.listen():
                if item.get("type") == "message":
                    yield item["data"]
        finally:
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
                await (getattr(pubsub, "aclose", None) or pubsub.close)()

    async def ping(self) -> None:
        await self._client.ping()

    async def close(self) -> None:
        await (getattr(self._client, "aclose", None) or self._client.close)()


# ============================================================================
# WebSocket fan-out with slow-client protection and heartbeats
# ============================================================================

class Socket(Protocol):
    async def send_text(self, data: str) -> None: ...
    async def close(self, code: int = 1000) -> None: ...


class TooManyClients(Exception):
    pass


class ClientSession:
    __slots__ = ("id", "socket", "outbox", "last_seen", "writer", "closing")

    def __init__(self, client_id: str, socket: Socket, queue_size: int) -> None:
        self.id, self.socket = client_id, socket
        self.outbox: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.last_seen = time.monotonic()
        self.writer: Optional[asyncio.Task] = None
        self.closing = False


class ConnectionManager:
    """Each client gets a bounded outbox and a writer task. `broadcast` never waits, so a slow or stuck client
    cannot delay the workers or other clients; a client whose outbox fills up is disconnected."""

    def __init__(self, queue_size: int, max_clients: int, metrics: Metrics) -> None:
        self._queue_size, self._max, self._metrics = queue_size, max_clients, metrics
        self._sessions: Dict[str, ClientSession] = {}
        self._tasks: set = set()

    @property
    def count(self) -> int:
        return len(self._sessions)

    def has_capacity(self) -> bool:
        return len(self._sessions) < self._max

    def register(self, client_id: str, socket: Socket) -> None:
        if not self.has_capacity():
            raise TooManyClients()
        session = ClientSession(client_id, socket, self._queue_size)
        session.writer = asyncio.create_task(self._write_loop(session))
        self._sessions[client_id] = session
        logger.info("client_connected", extra={"client_id": client_id})

    async def _write_loop(self, session: ClientSession) -> None:
        try:
            while True:
                await session.socket.send_text(await session.outbox.get())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.info("client_write_failed", extra={"client_id": session.id})
            self._detach(session, 1011)

    def _detach(self, session: ClientSession, code: int) -> None:
        """Disconnect a client from synchronous code."""
        if session.closing:
            return
        session.closing = True
        task = asyncio.create_task(self.unregister(session.id, code))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def unregister(self, client_id: str, code: int = 1000) -> None:
        session = self._sessions.pop(client_id, None)
        if session is None:
            return
        session.closing = True
        if session.writer is not None and session.writer is not asyncio.current_task():
            session.writer.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await session.writer
        with contextlib.suppress(Exception):
            await session.socket.close(code)
        logger.info("client_disconnected", extra={"client_id": client_id})

    def touch(self, client_id: str) -> None:
        session = self._sessions.get(client_id)
        if session is not None:
            session.last_seen = time.monotonic()

    def broadcast(self, message: str) -> None:
        for session in list(self._sessions.values()):
            if session.closing:
                continue
            try:
                session.outbox.put_nowait(message)
            except asyncio.QueueFull:
                self._metrics.slow_clients_dropped += 1
                logger.warning("slow_client_dropped", extra={"client_id": session.id})
                self._detach(session, 1013)                       # 1013: try again later

    def send_heartbeats(self) -> None:
        self.broadcast(json.dumps({"type": "heartbeat", "ts": utcnow().isoformat()}))

    async def reap_stale(self, timeout: float) -> List[str]:
        """Disconnect clients that have sent nothing for `timeout` seconds."""
        now = time.monotonic()
        stale = [s.id for s in self._sessions.values() if not s.closing and now - s.last_seen > timeout]
        for client_id in stale:
            logger.warning("client_heartbeat_timeout", extra={"client_id": client_id})
            await self.unregister(client_id, 1001)
        self._metrics.clients_timed_out += len(stale)
        return stale

    async def close_all(self) -> None:
        for client_id in list(self._sessions):
            await self.unregister(client_id, 1001)


# ============================================================================
# Stream engine: queue, workers, retries, graceful shutdown
# ============================================================================

class StreamEngine:
    def __init__(self, cfg: Settings, store: EventStore, bus: MessageBus, connections: ConnectionManager,
                 metrics: Metrics) -> None:
        self.cfg, self.store, self.bus = cfg, store, bus
        self.connections, self.metrics = connections, metrics
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_max_size)
        self._accepting = False
        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._started = time.monotonic()

    # ---- intake -------------------------------------------------------------
    async def submit(self, event: Event, timeout: Optional[float] = None) -> None:
        """Queue an event. When the queue is full, wait up to `timeout` seconds (forever if None) and then raise
        BackpressureError. Waiting forever is what lets the upstream feed apply backpressure to its source."""
        if not self._accepting:
            self.metrics.rejected += 1
            raise EngineStopped("The stream engine is not accepting events")
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            try:
                if timeout is None:
                    await self.queue.put(event)
                else:
                    await asyncio.wait_for(self.queue.put(event), timeout)
            except asyncio.TimeoutError:
                self.metrics.rejected += 1
                logger.warning("queue_saturated_event_rejected", extra={"event_id": str(event.id)})
                raise BackpressureError("The stream engine is saturated; retry shortly") from None
        self.metrics.accepted += 1

    # ---- processing ---------------------------------------------------------
    async def _worker(self, worker_id: int) -> None:
        while True:
            event = await self.queue.get()
            try:
                await self._process(event, worker_id)
            except Exception:
                logger.exception("event_processing_failed", extra={"event_id": str(event.id), "worker_id": worker_id})
            finally:
                self.queue.task_done()

    async def _process(self, event: Event, worker_id: int) -> None:
        event.processed_at = utcnow()
        inserted = await self._save_with_retry(event)
        if inserted is None:
            self.metrics.failed += 1
            return
        if not inserted:
            self.metrics.duplicates += 1          # stored earlier: do not announce it a second time
            return
        self.metrics.processed += 1
        await self._fan_out(event)

    async def _save_with_retry(self, event: Event) -> Optional[bool]:
        attempts = self.cfg.persist_max_attempts
        for attempt in range(attempts):
            try:
                return await self.store.save(event)
            except Exception as exc:
                if attempt == attempts - 1:
                    logger.error("event_persist_failed", extra={"event_id": str(event.id), "attempts": attempts,
                                                                "error": type(exc).__name__})
                    return None
                await asyncio.sleep(backoff_delay(attempt, self.cfg.persist_retry_base_delay_seconds,
                                                  self.cfg.persist_retry_max_delay_seconds))
        return None

    async def _fan_out(self, event: Event) -> None:
        """Publish through Redis; every instance's subscriber forwards it to its own clients. Only if publishing
        fails do we deliver locally, so a healthy setup never sends an event twice."""
        message = event.to_message()
        try:
            await self.bus.publish(self.cfg.redis_channel, message)
        except Exception as exc:
            self.metrics.publish_failures += 1
            logger.warning("event_publish_failed", extra={"event_id": str(event.id), "error": type(exc).__name__})
            self.connections.broadcast(message)

    async def _subscribe_once(self) -> None:
        async for message in self.bus.messages(self.cfg.redis_channel):
            self.connections.broadcast(message)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.heartbeat_interval_seconds)
            self.connections.send_heartbeats()
            if self.cfg.client_pong_timeout_seconds > 0:
                await self.connections.reap_stale(self.cfg.client_pong_timeout_seconds)

    # ---- lifecycle ----------------------------------------------------------
    async def start(self) -> None:
        self._accepting = True
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self.cfg.worker_pool_size)]
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))
        self._tasks.append(asyncio.create_task(supervise(
            "redis_subscriber", self._subscribe_once, self._stop, base=self.cfg.reconnect_base_delay_seconds,
            cap=self.cfg.reconnect_max_delay_seconds, stable_seconds=self.cfg.reconnect_stable_seconds)))
        logger.info("stream_engine_started", extra={"workers": self.cfg.worker_pool_size})

    async def stop(self) -> None:
        """Stop accepting events, finish what is queued (up to the drain timeout), then shut down."""
        self._accepting = False
        try:
            await asyncio.wait_for(self.queue.join(), self.cfg.drain_timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning("drain_timed_out", extra={"events_left": self.queue.qsize()})
        await asyncio.sleep(self.cfg.shutdown_grace_seconds)
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.connections.close_all()
        logger.info("stream_engine_stopped")

    # ---- observability ------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        m = self.metrics
        return {"uptime_seconds": round(time.monotonic() - self._started, 2),
                "connected_websocket_clients": self.connections.count,
                "queue_depth": self.queue.qsize(), "queue_capacity": self.queue.maxsize,
                "events_accepted": m.accepted, "events_processed": m.processed, "events_duplicate": m.duplicates,
                "events_failed": m.failed, "events_rejected": m.rejected, "publish_failures": m.publish_failures,
                "invalid_messages": m.invalid_messages, "slow_clients_dropped": m.slow_clients_dropped,
                "clients_timed_out": m.clients_timed_out}

    async def readiness(self, timeout: float = 2.0) -> Dict[str, bool]:
        async def probe(fn: Callable[[], Awaitable[None]]) -> bool:
            try:
                await asyncio.wait_for(fn(), timeout)
                return True
            except Exception:
                return False
        database, redis = await asyncio.gather(probe(self.store.ping), probe(self.bus.ping))
        return {"database": database, "redis": redis}


# ============================================================================
# Upstream WebSocket feed client
# ============================================================================

class UpstreamFeed:
    """Reads events from an upstream WebSocket and submits them to the engine. Reconnects with capped
    exponential backoff after every disconnect, including a clean close. When the queue is full the feed waits,
    which stops it reading from the socket (backpressure) instead of dropping messages."""

    def __init__(self, url: str, engine: StreamEngine, cfg: Settings, connect: Optional[Callable[[str], Any]] = None,
                 rand: Callable[[], float] = random.random) -> None:
        self.url, self.engine, self.cfg = url, engine, cfg
        self._connect, self._rand = connect, rand
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(supervise(
            "upstream_feed", self._run_once, self._stop, base=self.cfg.reconnect_base_delay_seconds,
            cap=self.cfg.reconnect_max_delay_seconds, stable_seconds=self.cfg.reconnect_stable_seconds,
            rand=self._rand))

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def _open(self):
        if self._connect is not None:
            return self._connect(self.url)
        import websockets                      # imported only when a feed is configured

        return websockets.connect(self.url, ping_interval=self.cfg.heartbeat_interval_seconds)

    async def _run_once(self) -> None:
        async with self._open() as ws:
            logger.info("upstream_feed_connected", extra={"host": urlsplit(self.url).hostname})
            async for message in ws:
                await self._handle(message)

    async def _handle(self, raw: Any) -> None:
        try:
            event = parse_event_message(raw, self.cfg.max_payload_bytes)
        except InvalidEvent as exc:
            self.engine.metrics.invalid_messages += 1
            logger.warning("upstream_message_rejected", extra={"reason": str(exc)})
            return
        await self.engine.submit(event)         # waits when the queue is full


# ============================================================================
# API schemas
# ============================================================================

class EventIn(BaseModel):
    id: Optional[uuid.UUID] = Field(None, description="Optional client-generated id; makes retries idempotent")
    source: str = Field(..., min_length=1, max_length=100)
    event_type: str = Field(..., min_length=1, max_length=100)
    payload: Dict[str, Any] = Field(..., description="Non-empty JSON object")
    occurred_at: Optional[datetime] = Field(None, description="When it happened; naive times are taken as UTC")


class EventOut(BaseModel):
    id: uuid.UUID
    source: str
    event_type: str
    payload: Dict[str, Any]
    occurred_at: Optional[datetime] = None
    received_at: datetime
    processed_at: Optional[datetime] = None


class HealthStatus(BaseModel):
    status: str
    database: bool
    redis: bool
    uptime_seconds: float
    connected_websocket_clients: int
    queue_depth: int
    queue_capacity: int
    events_processed: int
    events_rejected: int
    events_failed: int


# ============================================================================
# HTTP and WebSocket layer
# ============================================================================

router = APIRouter()


def get_engine(request: Request) -> StreamEngine:
    return request.app.state.engine


def require_api_key(request: Request, x_api_key: Optional[str] = Header(None, alias="X-API-Key")) -> None:
    import hmac

    expected = request.app.state.settings.api_key
    if expected and not (x_api_key and hmac.compare_digest(x_api_key.encode(), expected.encode())):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


@router.post("/events", response_model=EventOut, status_code=202, tags=["events"],
             dependencies=[Depends(require_api_key)], summary="Submit one event")
async def ingest_event(payload: EventIn, request: Request, engine: StreamEngine = Depends(get_engine)):
    """Returns 202 once the event is queued; it is saved and streamed shortly after. Returns 503 with
    Retry-After when the engine is saturated or shutting down."""
    event = build_event(payload.source, payload.event_type, payload.payload, occurred_at=payload.occurred_at,
                        event_id=payload.id, max_payload_bytes=request.app.state.settings.max_payload_bytes)
    await engine.submit(event, timeout=request.app.state.settings.ingest_backpressure_timeout_seconds)
    return EventOut(**event.to_dict())


@router.get("/events/{event_id}", response_model=EventOut, tags=["events"], dependencies=[Depends(require_api_key)],
            summary="Read one saved event")
async def read_event(event_id: uuid.UUID, request: Request):
    """404 also covers events that are accepted but not saved yet."""
    event = await request.app.state.store.get(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return EventOut(**event.to_dict())


@router.get("/events", response_model=List[EventOut], tags=["events"], dependencies=[Depends(require_api_key)],
            summary="List saved events, newest first")
async def list_events(request: Request, source: Optional[str] = Query(None, max_length=100),
                      event_type: Optional[str] = Query(None, max_length=100),
                      before: Optional[datetime] = Query(None, description="Only events received before this time"),
                      limit: int = Query(50, ge=1, le=200)):
    events = await request.app.state.store.list(source=source, event_type=event_type,
                                                before=to_utc(before) if before else None, limit=limit)
    return [EventOut(**e.to_dict()) for e in events]


@router.get("/health", response_model=HealthStatus, tags=["monitoring"],
            summary="Readiness: checks PostgreSQL and Redis; 503 when either is unreachable")
async def health(engine: StreamEngine = Depends(get_engine)):
    ready = await engine.readiness()
    snap = engine.snapshot()
    body = HealthStatus(status="ok" if all(ready.values()) else "degraded", **ready,
                        **{k: snap[k] for k in ("uptime_seconds", "connected_websocket_clients", "queue_depth",
                                                "queue_capacity", "events_processed", "events_rejected",
                                                "events_failed")})
    return JSONResponse(status_code=200 if body.status == "ok" else 503, content=body.model_dump())


@router.get("/metrics", tags=["monitoring"], summary="Throughput and backpressure counters",
            dependencies=[Depends(require_api_key)])
async def metrics(engine: StreamEngine = Depends(get_engine)):
    return engine.snapshot()


def _ws_authorised(websocket: WebSocket, expected: Optional[str]) -> bool:
    import hmac

    if not expected:
        return True
    given = websocket.headers.get("x-api-key") or websocket.query_params.get("api_key") or ""
    return hmac.compare_digest(given.encode(), expected.encode())


async def stream_ws(websocket: WebSocket) -> None:
    """Real-time channel. Sends every saved event as {"type": "event", "data": ...} and periodic
    {"type": "heartbeat"} frames. Send any message (for example "pong") at least every
    CLIENT_PONG_TIMEOUT_SECONDS to stay connected. Pass the API key in X-API-Key or ?api_key=."""
    engine: StreamEngine = websocket.app.state.engine
    if not _ws_authorised(websocket, websocket.app.state.settings.api_key):
        await websocket.close(code=1008)
        return
    if not engine.connections.has_capacity():
        await websocket.close(code=1013)
        return
    await websocket.accept()
    client_id = str(uuid.uuid4())
    try:
        engine.connections.register(client_id, websocket)
    except TooManyClients:
        await websocket.close(code=1013)
        return
    try:
        async for _ in websocket.iter_text():
            engine.connections.touch(client_id)
    except Exception:                                    # includes disconnects and non-text frames
        pass
    finally:
        await engine.connections.unregister(client_id)


def create_app(settings: Optional[Settings] = None, *, store: Optional[EventStore] = None,
               bus: Optional[MessageBus] = None, feed_connect: Optional[Callable[[str], Any]] = None) -> FastAPI:
    """Application factory. `store`, `bus` and `feed_connect` can be replaced (the tests do this)."""
    cfg = settings or Settings.from_env()
    configure_logging(cfg.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        event_store = store or PostgresEventStore(cfg)
        message_bus = bus or RedisBus(cfg.redis_url)
        await event_store.init()
        metrics_ = Metrics()
        connections = ConnectionManager(cfg.client_queue_size, cfg.max_ws_clients, metrics_)
        engine = StreamEngine(cfg, event_store, message_bus, connections, metrics_)
        await engine.start()
        feed = UpstreamFeed(cfg.upstream_feed_url, engine, cfg, feed_connect) if cfg.upstream_feed_url else None
        if feed:
            feed.start()
        app.state.settings, app.state.store, app.state.bus, app.state.engine = cfg, event_store, message_bus, engine
        try:
            yield
        finally:
            if feed:
                await feed.stop()                  # stop the producer first, then drain
            await engine.stop()
            await message_bus.close()
            await event_store.close()

    app = FastAPI(
        title=cfg.app_name, version="2.0.0", lifespan=lifespan,
        description="Event ingestion, validation and persistence for high-velocity data feeds, with backpressure, "
                    "resilient reconnection and real-time WebSocket fan-out. Docs at /docs, spec at /openapi.json.")

    @app.exception_handler(InvalidEvent)
    async def invalid_event_handler(request: Request, exc: InvalidEvent):
        return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})

    @app.exception_handler(EngineUnavailable)
    async def unavailable_handler(request: Request, exc: EngineUnavailable):
        return JSONResponse(status_code=503, content={"detail": str(exc)}, headers={"Retry-After": "1"})

    @app.get("/live", tags=["monitoring"], summary="Liveness probe")
    async def live():
        return {"status": "alive"}

    @app.get("/", tags=["monitoring"], summary="Service banner")
    async def root():
        return {"service": cfg.app_name, "docs": "/docs", "openapi_spec": "/openapi.json",
                "websocket_stream": "/ws/stream"}

    app.include_router(router, prefix=cfg.api_v1_str)
    app.add_api_websocket_route("/ws/stream", stream_ws)
    return app


# ============================================================================
# Entry point
# ============================================================================

_app: Optional[FastAPI] = None


def __getattr__(name: str):
    """Build the default app lazily, so importing this module (for example from tests) has no side effects.
    `uvicorn main:app` triggers this."""
    global _app
    if name == "app":
        if _app is None:
            _app = create_app()
        return _app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
