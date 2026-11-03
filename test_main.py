"""Tests for main.py.

    pip install httpx            # plus the requirements listed at the top of main.py
    python -m unittest test_main -v

PostgreSQL, Redis and the upstream feed are replaced by in-memory fakes, so no services are needed.
The real asyncpg and redis adapters are not covered here.
"""
import asyncio
import importlib.util
import json
import unittest
import uuid
from unittest import mock

try:
    import main
except ImportError as exc:                      # the required libraries are not installed
    raise unittest.SkipTest(f"install the requirements listed in main.py ({exc})")

HAVE_API_DEPS = all(importlib.util.find_spec(m) for m in ("fastapi", "httpx", "websockets"))


def cfg(**overrides):
    base = dict(worker_pool_size=2, queue_max_size=5, ingest_backpressure_timeout_seconds=0.05, persist_max_attempts=3,
                persist_retry_base_delay_seconds=0.001, persist_retry_max_delay_seconds=0.002,
                drain_timeout_seconds=2.0, shutdown_grace_seconds=0.05, heartbeat_interval_seconds=60.0,
                client_pong_timeout_seconds=0.0, client_queue_size=4, max_ws_clients=3,
                reconnect_base_delay_seconds=0.001, reconnect_max_delay_seconds=0.002, reconnect_stable_seconds=10.0)
    base.update(overrides)
    return main.Settings(**base)


def ev(i=0, **kw):
    return main.build_event("sensor", "reading", {"n": i}, **kw)


async def eventually(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition not met in time")


class FakeSocket:
    def __init__(self):
        self.sent, self.closed, self.fail, self.gate = [], None, False, None

    async def send_text(self, data):
        if self.fail:
            raise RuntimeError("socket closed")
        if self.gate is not None:
            await self.gate.wait()
        self.sent.append(data)

    async def close(self, code=1000):
        self.closed = code

    def messages(self):
        return [json.loads(m) for m in self.sent]


class FakeStore:
    def __init__(self):
        self.events, self.fail_times, self.save_calls = {}, 0, 0
        self.healthy, self.delay, self.gate = True, 0.0, None

    async def init(self): pass

    async def save(self, event):
        self.save_calls += 1
        if self.gate is not None:
            await self.gate.wait()
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError("database unavailable")
        if event.id in self.events:
            return False
        self.events[event.id] = event
        return True

    async def get(self, event_id): return self.events.get(event_id)

    async def list(self, *, source, event_type, before, limit):
        rows = [e for e in self.events.values() if (not source or e.source == source)
                and (not event_type or e.event_type == event_type) and (before is None or e.received_at < before)]
        return sorted(rows, key=lambda e: e.received_at, reverse=True)[:limit]

    async def ping(self):
        if not self.healthy:
            raise ConnectionError("down")

    async def close(self): pass


class FakeBus:
    """Behaves like Redis Pub/Sub: published messages reach every subscriber."""
    def __init__(self):
        self.subscribers, self.published, self.fail_publish, self.healthy = [], [], False, True

    async def publish(self, channel, message):
        if self.fail_publish:
            raise ConnectionError("redis unavailable")
        self.published.append(message)
        for q in list(self.subscribers):
            q.put_nowait(message)

    async def messages(self, channel):
        q = asyncio.Queue()
        self.subscribers.append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self.subscribers.remove(q)

    async def ping(self):
        if not self.healthy:
            raise ConnectionError("down")

    async def close(self): pass


class FakeConn:
    """Async context manager and async iterator, like a websockets connection."""
    def __init__(self, messages, hang=False):
        self.messages, self.hang = messages, hang

    async def __aenter__(self): return self
    async def __aexit__(self, *exc): return False
    def __aiter__(self): return self._gen()

    async def _gen(self):
        for m in self.messages:
            yield m
        if self.hang:
            await asyncio.Event().wait()


# ============================================================================
class SettingsTests(unittest.TestCase):
    def test_validation(self):
        for kwargs in ({"worker_pool_size": 0}, {"queue_max_size": -1}, {"db_pool_min": 5, "db_pool_max": 2},
                       {"client_pong_timeout_seconds": -1.0}, {"max_payload_bytes": 0}):
            with self.assertRaises(ValueError):
                main.Settings(**kwargs)

    def test_from_env(self):
        import os
        names = ("WORKER_POOL_SIZE", "HEARTBEAT_INTERVAL_SECONDS", "UPSTREAM_FEED_URL", "LOG_LEVEL", "API_KEY")
        old = {k: os.environ.get(k) for k in names}
        try:
            os.environ.update(WORKER_POOL_SIZE="8", HEARTBEAT_INTERVAL_SECONDS="2.5",
                              UPSTREAM_FEED_URL="wss://feed.example/stream", LOG_LEVEL="debug", API_KEY="k")
            s = main.Settings.from_env()
            self.assertEqual((s.worker_pool_size, s.heartbeat_interval_seconds, s.upstream_feed_url, s.log_level, s.api_key),
                             (8, 2.5, "wss://feed.example/stream", "DEBUG", "k"))
            os.environ["WORKER_POOL_SIZE"] = "many"
            with self.assertRaises(ValueError):
                main.Settings.from_env()
        finally:
            for k, v in old.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)


class ValidationTests(unittest.TestCase):
    def test_valid_event_is_normalised(self):
        e = main.build_event("  sensor ", "reading", {"t": 1}, occurred_at="2026-01-01T10:00:00Z")
        self.assertEqual((e.source, e.event_type), ("sensor", "reading"))
        self.assertEqual(e.occurred_at.utcoffset().total_seconds(), 0)
        self.assertIsNotNone(e.received_at.tzinfo)

    def test_naive_timestamps_are_utc_and_offsets_are_converted(self):
        from datetime import datetime, timedelta, timezone
        naive = main.build_event("s", "t", {"a": 1}, occurred_at=datetime(2026, 1, 1, 12, 0))
        self.assertEqual(naive.occurred_at.isoformat(), "2026-01-01T12:00:00+00:00")
        offset = main.build_event("s", "t", {"a": 1}, occurred_at="2026-01-01T12:00:00+02:00")
        self.assertEqual(offset.occurred_at, datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc))

    def test_client_supplied_id_is_kept(self):
        i = uuid.uuid4()
        self.assertEqual(main.build_event("s", "t", {"a": 1}, event_id=str(i)).id, i)

    def test_rejects_bad_input(self):
        cases = [("", "t", {"a": 1}), ("s", "  ", {"a": 1}), ("s", "t", {}), ("s", "t", [1]), ("s", "t", None),
                 ("x" * 101, "t", {"a": 1}), (None, "t", {"a": 1}), ("s", "t", {"a": float("nan")}),
                 ("s", "t", {"a": float("inf")}), ("s", "t", {"a": object()})]
        for source, etype, payload in cases:
            with self.assertRaises(main.InvalidEvent, msg=repr((source, etype, payload))):
                main.build_event(source, etype, payload)
        with self.assertRaises(main.InvalidEvent):
            main.build_event("s", "t", {"a": 1}, event_id="not-a-uuid")
        with self.assertRaises(main.InvalidEvent):
            main.build_event("s", "t", {"a": 1}, occurred_at="yesterday")

    def test_oversized_payload_has_its_own_status(self):
        with self.assertRaises(main.PayloadTooLarge) as ctx:
            main.build_event("s", "t", {"blob": "x" * 200}, max_payload_bytes=100)
        self.assertEqual(ctx.exception.status_code, 413)
        self.assertEqual(main.InvalidEvent.status_code, 422)

    def test_error_messages_do_not_echo_payload_content(self):
        try:
            main.build_event("s", "t", {"secret": float("nan")})
        except main.InvalidEvent as exc:
            self.assertNotIn("secret", str(exc))

    def test_parse_feed_message(self):
        raw = json.dumps({"source": "s", "event_type": "t", "payload": {"a": 1}, "id": str(uuid.uuid4())})
        self.assertEqual(main.parse_event_message(raw, 1000).source, "s")
        for bad in ("not json", "[1, 2]", "42", json.dumps({"source": "s"}), None, b"\xff"):
            with self.assertRaises(main.InvalidEvent):
                main.parse_event_message(bad, 1000)

    def test_message_envelope(self):
        e = ev(7)
        body = json.loads(e.to_message())
        self.assertEqual((body["type"], body["data"]["id"], body["data"]["payload"]), ("event", str(e.id), {"n": 7}))


class BackoffTests(unittest.TestCase):
    def test_growth_cap_and_jitter(self):
        top = lambda a: main.backoff_delay(a, 1.0, 30.0, rand=lambda: 1.0)
        self.assertEqual([top(0), top(1), top(2), top(3)], [1.0, 2.0, 4.0, 8.0])
        self.assertEqual(top(10), 30.0)                                     # capped
        self.assertEqual(main.backoff_delay(2, 1.0, 30.0, rand=lambda: 0.0), 2.0)   # jitter floor is 50%


class SuperviseTests(unittest.IsolatedAsyncioTestCase):
    async def test_restarts_after_failures(self):
        stop, calls = asyncio.Event(), []

        async def run_once():
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("boom")
            stop.set()
        await main.supervise("t", run_once, stop, base=0.001, cap=0.002, stable_seconds=10, rand=lambda: 1.0)
        self.assertEqual(len(calls), 3)

    async def test_clean_exit_still_backs_off(self):
        """A connection that closes cleanly must not cause a tight reconnect loop."""
        stop, calls = asyncio.Event(), []

        async def run_once():
            calls.append(1)
        task = asyncio.create_task(main.supervise("t", run_once, stop, base=0.02, cap=0.02, stable_seconds=10,
                                                  rand=lambda: 1.0))
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, 1)
        self.assertTrue(2 <= len(calls) <= 9, len(calls))

    async def test_stop_interrupts_the_wait(self):
        stop = asyncio.Event()

        async def run_once():
            raise RuntimeError("boom")
        task = asyncio.create_task(main.supervise("t", run_once, stop, base=30, cap=30, stable_seconds=10))
        await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(task, 1)           # would take 30 s if the wait were not interruptible

    async def test_backoff_resets_only_after_a_stable_run(self):
        async def attempts_seen(stable_seconds):
            seen, stop, runs = [], asyncio.Event(), []

            async def run_once():
                runs.append(1)
                if len(runs) == 4:
                    stop.set()
            with mock.patch.object(main, "backoff_delay", side_effect=lambda a, b, c, r=None: seen.append(a) or 0.001):
                await main.supervise("t", run_once, stop, base=1, cap=1, stable_seconds=stable_seconds)
            return seen
        self.assertEqual(await attempts_seen(stable_seconds=100), [0, 1, 2])       # never stable: keeps growing
        self.assertEqual(await attempts_seen(stable_seconds=0), [0, 0, 0])         # always stable: resets


class ConnectionManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.metrics = main.Metrics()
        self.manager = main.ConnectionManager(queue_size=2, max_clients=3, metrics=self.metrics)

    async def asyncTearDown(self):
        await self.manager.close_all()

    async def test_broadcast_reaches_every_client(self):
        a, b = FakeSocket(), FakeSocket()
        self.manager.register("a", a)
        self.manager.register("b", b)
        self.manager.broadcast("hello")
        await eventually(lambda: a.sent == ["hello"] and b.sent == ["hello"])

    async def test_slow_client_is_dropped_and_others_are_unaffected(self):
        fast, slow = FakeSocket(), FakeSocket()
        slow.gate = asyncio.Event()                         # never set: this client cannot keep up
        self.manager.register("fast", fast)
        self.manager.register("slow", slow)
        for m in ("m0", "m1", "m2", "m3"):                  # paced, so the fast client keeps up
            self.manager.broadcast(m)
            await asyncio.sleep(0.01)                       # slow: m0 is in flight, m1 and m2 fill the outbox, m3 overflows
        await eventually(lambda: slow.closed == 1013)
        await eventually(lambda: fast.sent == ["m0", "m1", "m2", "m3"])
        self.assertEqual((self.manager.count, self.metrics.slow_clients_dropped), (1, 1))

    async def test_client_limit(self):
        for i in range(3):
            self.manager.register(str(i), FakeSocket())
        self.assertFalse(self.manager.has_capacity())
        with self.assertRaises(main.TooManyClients):
            self.manager.register("extra", FakeSocket())

    async def test_heartbeat_frames(self):
        s = FakeSocket()
        self.manager.register("a", s)
        self.manager.send_heartbeats()
        await eventually(lambda: len(s.sent) == 1)
        self.assertEqual(s.messages()[0]["type"], "heartbeat")

    async def test_silent_clients_are_disconnected_and_active_ones_kept(self):
        active, silent = FakeSocket(), FakeSocket()
        self.manager.register("active", active)
        self.manager.register("silent", silent)
        await asyncio.sleep(0.07)
        self.manager.touch("active")
        self.assertEqual(await self.manager.reap_stale(0.05), ["silent"])
        self.assertEqual((silent.closed, active.closed, self.manager.count, self.metrics.clients_timed_out),
                         (1001, None, 1, 1))

    async def test_client_whose_socket_fails_is_removed(self):
        s = FakeSocket()
        s.fail = True
        self.manager.register("a", s)
        self.manager.broadcast("x")
        await eventually(lambda: self.manager.count == 0)

    async def test_close_all(self):
        a, b = FakeSocket(), FakeSocket()
        self.manager.register("a", a)
        self.manager.register("b", b)
        await self.manager.close_all()
        self.assertEqual((a.closed, b.closed, self.manager.count), (1001, 1001, 0))


class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def make(self, **overrides):
        self.cfg = cfg(**overrides)
        self.store, self.bus, self.metrics = FakeStore(), FakeBus(), main.Metrics()
        self.connections = main.ConnectionManager(self.cfg.client_queue_size, self.cfg.max_ws_clients, self.metrics)
        self.engine = main.StreamEngine(self.cfg, self.store, self.bus, self.connections, self.metrics)
        await self.engine.start()
        self.addAsyncCleanup(self.engine.stop)
        self.client = FakeSocket()
        self.connections.register("c1", self.client)
        await asyncio.sleep(0.01)                           # let the subscriber attach to the bus

    async def test_event_is_saved_published_and_delivered_exactly_once(self):
        await self.make()
        e = ev(1)
        await self.engine.submit(e)
        await eventually(lambda: e.id in self.store.events and len(self.client.sent) == 1)
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual((self.client.messages()[0]["data"]["id"], len(self.bus.published)), (str(e.id), 1))
        self.assertIsNotNone(self.store.events[e.id].processed_at)
        self.assertEqual((self.metrics.accepted, self.metrics.processed), (1, 1))

    async def test_duplicate_ids_are_saved_and_announced_once(self):
        await self.make()
        shared = uuid.uuid4()
        await self.engine.submit(ev(1, event_id=shared))
        await self.engine.submit(ev(1, event_id=shared))
        await eventually(lambda: self.metrics.processed + self.metrics.duplicates == 2)
        await asyncio.sleep(0.03)
        self.assertEqual((self.metrics.processed, self.metrics.duplicates, len(self.client.sent)), (1, 1, 1))

    async def test_transient_database_errors_are_retried(self):
        await self.make()
        self.store.fail_times = 2
        e = ev(1)
        await self.engine.submit(e)
        await eventually(lambda: e.id in self.store.events)
        self.assertEqual((self.store.save_calls, self.metrics.processed, self.metrics.failed), (3, 1, 0))

    async def test_exhausted_retries_are_counted_and_the_engine_keeps_working(self):
        await self.make()
        self.store.fail_times = 99
        await self.engine.submit(ev(1))
        await eventually(lambda: self.metrics.failed == 1)
        self.assertEqual(self.client.sent, [])                  # not announced: it was never stored
        self.store.fail_times = 0
        good = ev(2)
        await self.engine.submit(good)
        await eventually(lambda: good.id in self.store.events)

    async def test_publish_failure_falls_back_to_local_delivery(self):
        await self.make()
        self.bus.fail_publish = True
        e = ev(1)
        await self.engine.submit(e)
        await eventually(lambda: len(self.client.sent) == 1)
        await asyncio.sleep(0.03)
        self.assertEqual((len(self.client.sent), self.metrics.publish_failures, e.id in self.store.events),
                         (1, 1, True))

    async def test_events_published_by_other_instances_reach_local_clients(self):
        await self.make()
        await self.bus.publish(self.cfg.redis_channel, "from-another-instance")
        await eventually(lambda: self.client.sent == ["from-another-instance"])

    async def test_backpressure_rejects_after_the_timeout_and_recovers(self):
        await self.make()
        self.store.gate = asyncio.Event()                       # workers block inside save()
        for i in range(2):
            await self.engine.submit(ev(i))
        await asyncio.sleep(0.02)                               # both workers are now busy
        for i in range(5):                                      # fills the queue (capacity 5)
            await self.engine.submit(ev(10 + i))
        with self.assertRaises(main.BackpressureError):
            await self.engine.submit(ev(99), timeout=0.05)
        self.assertEqual((self.metrics.rejected, self.metrics.accepted), (1, 7))
        self.store.gate.set()
        await eventually(lambda: self.metrics.processed == 7)

    async def test_waiting_without_a_timeout_never_drops(self):
        await self.make(queue_max_size=1, worker_pool_size=1)
        self.store.delay = 0.005
        await asyncio.wait_for(asyncio.gather(*(self.engine.submit(ev(i)) for i in range(10))), 5)
        await eventually(lambda: self.metrics.processed == 10)
        self.assertEqual(self.metrics.rejected, 0)

    async def test_shutdown_drains_queued_events_then_refuses_new_ones(self):
        await self.make()
        self.store.delay = 0.01
        events = [ev(i) for i in range(5)]
        for e in events:
            await self.engine.submit(e)
        await self.engine.stop()
        self.assertTrue(all(e.id in self.store.events for e in events))
        with self.assertRaises(main.EngineStopped):
            await self.engine.submit(ev(9))
        self.assertEqual(self.client.closed, 1001)

    async def test_readiness_and_snapshot(self):
        await self.make()
        self.assertEqual(await self.engine.readiness(), {"database": True, "redis": True})
        self.store.healthy = False
        self.assertEqual(await self.engine.readiness(), {"database": False, "redis": True})
        snap = self.engine.snapshot()
        self.assertEqual((snap["queue_capacity"], snap["connected_websocket_clients"]), (5, 1))
        self.assertTrue({"events_accepted", "events_processed", "events_failed", "events_rejected"} <= set(snap))


class UpstreamFeedTests(unittest.IsolatedAsyncioTestCase):
    async def start(self, *scripts, **overrides):
        self.cfg = cfg(**overrides)
        self.store, self.bus, self.metrics = FakeStore(), FakeBus(), main.Metrics()
        connections = main.ConnectionManager(4, 3, self.metrics)
        self.engine = main.StreamEngine(self.cfg, self.store, self.bus, connections, self.metrics)
        await self.engine.start()
        self.addAsyncCleanup(self.engine.stop)
        pending, self.connects = list(scripts), 0

        def connect(url):
            self.connects += 1
            if isinstance(pending[0] if pending else None, Exception):
                raise pending.pop(0)
            return FakeConn(pending.pop(0)) if pending else FakeConn([], hang=True)
        self.feed = main.UpstreamFeed("wss://feed.example/stream?token=secret", self.engine, self.cfg, connect,
                                      rand=lambda: 1.0)
        self.feed.start()
        self.addAsyncCleanup(self.feed.stop)

    @staticmethod
    def msg(i, **extra):
        return json.dumps({"source": "feed", "event_type": "tick", "payload": {"i": i}, **extra})

    async def test_valid_messages_are_ingested_and_invalid_ones_skipped(self):
        await self.start([self.msg(1), "not json", self.msg(2), json.dumps({"source": "x"})])
        await eventually(lambda: self.metrics.processed == 2)
        self.assertEqual(self.metrics.invalid_messages, 2)

    async def test_reconnects_after_clean_closes_and_errors(self):
        await self.start([], ConnectionError("refused"), [], [self.msg(1)])
        await eventually(lambda: self.metrics.processed == 1)
        self.assertGreaterEqual(self.connects, 4)

    async def test_full_queue_makes_the_feed_wait_instead_of_dropping(self):
        await self.start([self.msg(i) for i in range(25)], queue_max_size=2, worker_pool_size=1)
        self.store.delay = 0.003
        await eventually(lambda: self.metrics.processed == 25, timeout=5)
        self.assertEqual(self.metrics.rejected, 0)

    async def test_idempotent_redelivery_after_reconnect(self):
        same = self.msg(1, id=str(uuid.uuid4()))
        await self.start([same], [same])
        await eventually(lambda: self.metrics.processed + self.metrics.duplicates == 2)
        self.assertEqual((self.metrics.processed, self.metrics.duplicates), (1, 1))


@unittest.skipUnless(HAVE_API_DEPS, "fastapi, httpx and websockets are required")
class ApiTests(unittest.TestCase):
    def client(self, **overrides):
        from fastapi.testclient import TestClient
        self.store, self.bus = FakeStore(), FakeBus()
        app = main.create_app(cfg(**overrides), store=self.store, bus=self.bus)
        return TestClient(app)

    BODY = {"source": "sensor", "event_type": "reading", "payload": {"t": 21.5}}

    def wait_for(self, predicate, timeout=3.0):
        import time
        end = time.time() + timeout
        while time.time() < end:
            if predicate():
                return
            time.sleep(0.01)
        self.fail("condition not met in time")

    def test_post_then_read_back(self):
        with self.client() as c:
            r = c.post("/api/v1/events", json={**self.BODY, "id": str(uuid.uuid4())})
            self.assertEqual(r.status_code, 202)
            eid = r.json()["id"]
            self.wait_for(lambda: c.get(f"/api/v1/events/{eid}").status_code == 200)
            self.assertEqual(c.get(f"/api/v1/events/{eid}").json()["payload"], {"t": 21.5})
            self.assertEqual(len(c.get("/api/v1/events?source=sensor").json()), 1)
            self.assertEqual(c.get("/api/v1/events?source=other").json(), [])
            self.assertEqual(c.get(f"/api/v1/events/{uuid.uuid4()}").status_code, 404)

    def test_validation_errors(self):
        with self.client(max_payload_bytes=100) as c:
            self.assertEqual(c.post("/api/v1/events", json={**self.BODY, "payload": {}}).status_code, 422)
            self.assertEqual(c.post("/api/v1/events", json={**self.BODY, "source": ""}).status_code, 422)
            self.assertEqual(c.post("/api/v1/events", json={**self.BODY, "payload": {"x": "y" * 500}}).status_code, 413)
            self.assertEqual(c.get("/api/v1/events?limit=0").status_code, 422)

    def test_api_key(self):
        with self.client(api_key="secret") as c:
            self.assertEqual(c.post("/api/v1/events", json=self.BODY).status_code, 401)
            self.assertEqual(c.post("/api/v1/events", json=self.BODY, headers={"X-API-Key": "secret"}).status_code, 202)
            self.assertEqual(c.get("/live").status_code, 200)
            self.assertEqual(c.get("/api/v1/health").status_code, 200)          # health stays open

    def test_health_reports_unreachable_dependencies(self):
        with self.client() as c:
            self.assertEqual(c.get("/api/v1/health").json()["status"], "ok")
            self.bus.healthy = False
            r = c.get("/api/v1/health")
            self.assertEqual((r.status_code, r.json()["status"], r.json()["redis"]), (503, "degraded", False))

    def test_websocket_receives_events_and_enforces_the_key(self):
        from starlette.websockets import WebSocketDisconnect
        with self.client(api_key="secret") as c:
            with self.assertRaises(WebSocketDisconnect):
                with c.websocket_connect("/ws/stream"):
                    pass
            with c.websocket_connect("/ws/stream?api_key=secret") as ws:
                c.post("/api/v1/events", json=self.BODY, headers={"X-API-Key": "secret"})
                for _ in range(20):
                    frame = ws.receive_json()
                    if frame["type"] == "event":
                        self.assertEqual(frame["data"]["payload"], {"t": 21.5})
                        break
                else:
                    self.fail("no event frame received")


if __name__ == "__main__":
    unittest.main()
