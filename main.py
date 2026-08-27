"""
Real-Time Data Streaming & API Integration Engine
--------------------------------------------------
Single-file version for easy copy/paste into a repo.

Stack: Python, AsyncIO, WebSockets, PostgreSQL, Redis Pub/Sub, FastAPI

Features:
  - Event-driven ingestion via REST + an optional upstream WebSocket feed
  - Bounded async queue enforcing real backpressure (no unbounded buffering)
  - Async worker pool that validates, persists, and fans events out
  - Automatic reconnection with exponential backoff + jitter for upstream feeds
  - Heartbeat monitoring over WebSocket connections to downstream clients
  - Structured JSON logging
  - Interactive Swagger/OpenAPI docs, generated automatically by FastAPI

Run:
    pip install fastapi uvicorn asyncpg "redis>=5" pydantic pydantic-settings websockets
    uvicorn main:app --reload

Requires a running PostgreSQL instance and a running Redis instance.
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional

import asyncpg
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ============================================================================
# Settings
# ============================================================================

class Settings(BaseSettings):
    APP_NAME: str = "Real-Time Data Streaming & API Integration Engine"
    API_V1_STR: str = "/api/v1"

    POSTGRES_DSN: str = "postgresql://postgres:postgres@localhost:5432/streaming_engine"

    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CHANNEL: str = "events:stream"

    WORKER_POOL_SIZE: int = 4
    QUEUE_MAX_SIZE: int = 1000
    INGEST_BACKPRESSURE_TIMEOUT_SECONDS: float = 2.0

    HEARTBEAT_INTERVAL_SECONDS: float = 15.0

    # Optional upstream WebSocket feed to consume from (leave unset to disable)
    UPSTREAM_FEED_URL: Optional[str] = None
    RECONNECT_BASE_DELAY_SECONDS: float = 1.0
    RECONNECT_MAX_DELAY_SECONDS: float = 30.0

    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()

# ============================================================================
# Structured (JSON) logging
# ============================================================================

_STANDARD_RECORD_KEYS = set(vars(logging.makeLogRecord({})).keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
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


configure_logging(settings.LOG_LEVEL)
logger = logging.getLogger("streaming_engine")

# ============================================================================
# Schemas
# ============================================================================

class EventIn(BaseModel):
    source: str = Field(..., min_length=1, max_length=100)
    event_type: str = Field(..., min_length=1, max_length=100)
    payload: Dict[str, Any]
    occurred_at: Optional[datetime] = None

    @field_validator("payload")
    @classmethod
    def payload_must_not_be_empty(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        if not v:
            raise ValueError("payload must not be empty")
        return v


class EventRecord(EventIn):
    id: uuid.UUID
    received_at: datetime
    processed_at: Optional[datetime] = None


class HealthStatus(BaseModel):
    status: str
    uptime_seconds: float
    connected_websocket_clients: int
    queue_depth: int
    queue_capacity: int
    events_processed: int
    events_dropped: int


# ============================================================================
# Backpressure-aware bounded queue
# ============================================================================

class BackpressureQueue:
    """
    Wraps asyncio.Queue(maxsize=N) so producers genuinely block (or time out)
    once the queue is full, instead of buffering unboundedly in memory.
    Tracks enqueued/dropped counts for observability.
    """

    def __init__(self, maxsize: int):
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.enqueued = 0
        self.dropped = 0

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, item: Any) -> None:
        await self._queue.put(item)
        self.enqueued += 1

    async def get(self) -> Any:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()


# ============================================================================
# WebSocket connection manager (downstream clients + heartbeat)
# ============================================================================

class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, WebSocket] = {}
        self.last_pong: Dict[str, datetime] = {}

    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        await websocket.accept()
        self.active[client_id] = websocket
        self.last_pong[client_id] = datetime.utcnow()
        logger.info("client_connected", extra={"client_id": client_id})

    def disconnect(self, client_id: str) -> None:
        self.active.pop(client_id, None)
        self.last_pong.pop(client_id, None)
        logger.info("client_disconnected", extra={"client_id": client_id})

    async def broadcast(self, message: Dict[str, Any]) -> None:
        stale: List[str] = []
        payload = json.dumps(message, default=str)
        for client_id, ws in list(self.active.items()):
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(client_id)
        for client_id in stale:
            self.disconnect(client_id)

    async def send_heartbeat(self) -> None:
        stale: List[str] = []
        for client_id, ws in list(self.active.items()):
            try:
                await ws.send_json({"type": "heartbeat", "ts": datetime.utcnow().isoformat()})
            except Exception:
                stale.append(client_id)
        for client_id in stale:
            self.disconnect(client_id)


# ============================================================================
# Persistence (PostgreSQL via asyncpg)
# ============================================================================

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


async def init_db_pool(dsn: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)
    return pool


async def persist_event(pool: asyncpg.Pool, record: EventRecord) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (id, source, event_type, payload, occurred_at, received_at, processed_at)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
            """,
            record.id,
            record.source,
            record.event_type,
            json.dumps(record.payload),
            record.occurred_at,
            record.received_at,
            record.processed_at,
        )


async def publish_event(redis_client: "aioredis.Redis", channel: str, record: EventRecord) -> None:
    await redis_client.publish(channel, record.model_dump_json())


# ============================================================================
# Stream engine: queue + worker pool + heartbeat + metrics
# ============================================================================

class StreamEngine:
    """
    Central coordinator: accepts events (REST or upstream feed), enforces
    backpressure via a bounded queue, and runs a pool of async workers that
    validate, persist to Postgres, publish to Redis, and fan out to
    connected WebSocket clients.
    """

    def __init__(
        self,
        cfg: Settings,
        pool: asyncpg.Pool,
        redis_client: "aioredis.Redis",
        connections: ConnectionManager,
    ):
        self.cfg = cfg
        self.pool = pool
        self.redis = redis_client
        self.connections = connections
        self.queue = BackpressureQueue(cfg.QUEUE_MAX_SIZE)
        self.events_processed = 0
        self._start_time = time.monotonic()
        self._workers: List[asyncio.Task] = []
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()

    async def ingest(self, event: EventIn) -> EventRecord:
        record = EventRecord(
            id=uuid.uuid4(),
            received_at=datetime.utcnow(),
            source=event.source,
            event_type=event.event_type,
            payload=event.payload,
            occurred_at=event.occurred_at,
        )
        try:
            await asyncio.wait_for(
                self.queue.put(record), timeout=self.cfg.INGEST_BACKPRESSURE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            self.queue.dropped += 1
            logger.warning("queue_saturated_event_rejected", extra={"event_id": str(record.id)})
            raise HTTPException(
                status_code=503,
                detail="Stream engine backpressure limit reached; retry shortly",
            )
        return record

    async def _worker_loop(self, worker_id: int) -> None:
        logger.info("worker_started", extra={"worker_id": worker_id})
        while not self._shutdown.is_set():
            try:
                record = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                record.processed_at = datetime.utcnow()
                await persist_event(self.pool, record)
                await publish_event(self.redis, self.cfg.REDIS_CHANNEL, record)
                await self.connections.broadcast(
                    {"type": "event", "data": record.model_dump(mode="json")}
                )
                self.events_processed += 1
            except Exception:
                logger.exception(
                    "event_processing_failed",
                    extra={"event_id": str(record.id), "worker_id": worker_id},
                )
            finally:
                self.queue.task_done()
        logger.info("worker_stopped", extra={"worker_id": worker_id})

    async def _heartbeat_loop(self) -> None:
        while not self._shutdown.is_set():
            await self.connections.send_heartbeat()
            await asyncio.sleep(self.cfg.HEARTBEAT_INTERVAL_SECONDS)

    async def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._worker_loop(i)) for i in range(self.cfg.WORKER_POOL_SIZE)
        ]
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("stream_engine_started", extra={"workers": self.cfg.WORKER_POOL_SIZE})

    async def stop(self) -> None:
        self._shutdown.set()
        for task in self._workers:
            task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        logger.info("stream_engine_stopped")

    def health(self) -> HealthStatus:
        return HealthStatus(
            status="ok",
            uptime_seconds=round(time.monotonic() - self._start_time, 2),
            connected_websocket_clients=len(self.connections.active),
            queue_depth=self.queue.qsize(),
            queue_capacity=self.queue.maxsize,
            events_processed=self.events_processed,
            events_dropped=self.queue.dropped,
        )


# ============================================================================
# Resilient upstream feed client (auto-reconnect + backoff)
# ============================================================================

class ReconnectingFeedClient:
    """
    Connects to an upstream WebSocket feed and forwards each message into
    the stream engine's ingest queue. On any disconnect or error it
    reconnects automatically using capped exponential backoff with jitter,
    so a single upstream blip doesn't interrupt ingestion.
    """

    def __init__(self, url: str, engine: StreamEngine, cfg: Settings):
        self.url = url
        self.engine = engine
        self.cfg = cfg
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                import websockets  # imported lazily; only needed if a feed URL is set

                async with websockets.connect(
                    self.url, ping_interval=self.cfg.HEARTBEAT_INTERVAL_SECONDS
                ) as ws:
                    logger.info("upstream_feed_connected", extra={"url": self.url})
                    attempt = 0  # reset backoff after a clean connection
                    async for message in ws:
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = min(
                    self.cfg.RECONNECT_MAX_DELAY_SECONDS,
                    self.cfg.RECONNECT_BASE_DELAY_SECONDS * (2**attempt),
                )
                delay *= 0.5 + random.random()  # jitter avoids thundering herd
                logger.warning(
                    "upstream_feed_disconnected",
                    extra={"error": str(exc), "retry_in_seconds": round(delay, 2)},
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _handle_message(self, raw: str) -> None:
        try:
            data = json.loads(raw)
            event = EventIn(**data)
            await self.engine.ingest(event)
        except Exception:
            logger.exception("invalid_upstream_message")


# ============================================================================
# FastAPI app (REST + WebSocket + auto-generated Swagger/OpenAPI docs)
# ============================================================================

app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "Event-driven ingestion, validation, and persistence of high-velocity "
        "data feeds, with resilient reconnection, backpressure, and real-time "
        "WebSocket fan-out. Interactive docs available at /docs, machine-"
        "readable spec at /openapi.json for Postman import."
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def on_startup() -> None:
    pool = await init_db_pool(settings.POSTGRES_DSN)
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    connections = ConnectionManager()
    engine = StreamEngine(settings, pool, redis_client, connections)
    await engine.start()

    app.state.pool = pool
    app.state.redis = redis_client
    app.state.connections = connections
    app.state.engine = engine
    app.state.feed_client = None

    if settings.UPSTREAM_FEED_URL:
        feed_client = ReconnectingFeedClient(settings.UPSTREAM_FEED_URL, engine, settings)
        feed_client.start()
        app.state.feed_client = feed_client


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if app.state.feed_client is not None:
        await app.state.feed_client.stop()
    await app.state.engine.stop()
    await app.state.redis.close()
    await app.state.pool.close()


def get_engine() -> StreamEngine:
    return app.state.engine


@app.post(
    f"{settings.API_V1_STR}/events",
    response_model=EventRecord,
    status_code=202,
    tags=["events"],
    summary="Ingest a single event into the stream",
)
async def ingest_event(event: EventIn, engine: StreamEngine = Depends(get_engine)):
    return await engine.ingest(event)


@app.get(
    f"{settings.API_V1_STR}/health",
    response_model=HealthStatus,
    tags=["monitoring"],
    summary="Liveness/readiness probe with queue and connection stats",
)
async def get_health(engine: StreamEngine = Depends(get_engine)):
    return engine.health()


@app.get(
    f"{settings.API_V1_STR}/metrics",
    tags=["monitoring"],
    summary="Snapshot of throughput and backpressure metrics",
)
async def get_metrics(engine: StreamEngine = Depends(get_engine)):
    return engine.health().model_dump()


@app.websocket("/ws/stream")
async def stream_ws(websocket: WebSocket):
    """
    Real-time fan-out channel. Every persisted event is broadcast here, plus
    periodic {"type": "heartbeat"} frames. Clients may reply with the text
    "pong" to have their liveness timestamp refreshed server-side.
    """
    connections: ConnectionManager = app.state.connections
    client_id = str(uuid.uuid4())
    await connections.connect(websocket, client_id)
    try:
        while True:
            msg = await websocket.receive_text()
            if msg == "pong":
                connections.last_pong[client_id] = datetime.utcnow()
    except WebSocketDisconnect:
        connections.disconnect(client_id)


@app.get("/", tags=["monitoring"], summary="Service banner + docs pointer")
async def root():
    return {
        "service": settings.APP_NAME,
        "docs": "/docs",
        "openapi_spec": "/openapi.json",
        "websocket_stream": "/ws/stream",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)