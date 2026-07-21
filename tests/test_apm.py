import asyncio
import contextvars
import http.client
import http.server
import json
import logging
import os
import socket
import threading
import time
import types
import unittest
from unittest import mock

try:
    import requests
except ImportError:  # the end-to-end urllib3 test is optional
    requests = None

try:
    import httpx
except ImportError:  # the httpx tests are optional
    httpx = None

try:
    import aiohttp
except ImportError:  # the aiohttp tests are optional
    aiohttp = None

try:
    import pymongo  # noqa: F401  (presence check only)
except ImportError:  # the mongodb listener tests are optional
    pymongo = None

try:
    import motor.frameworks.asyncio as motor_asyncio_framework
except ImportError:  # the motor context tests are optional
    motor_asyncio_framework = None

try:
    import elasticsearch
except ImportError:  # the elasticsearch patch test is optional
    elasticsearch = None

import roottrace_apm as apm_mod
from roottrace_apm import (
    MAX_ENTRIES, MAX_ERRORS, MAX_LOG_ENTRIES, MAX_LOG_MESSAGE_LENGTH,
    MAX_NAME_LENGTH, MAX_TAG_KEYS, MAX_TX_GROUPS, MAX_USER_ENTRIES,
    Apm, AsgiMiddleware, RootTraceLogHandler, WsgiMiddleware,
    _HttpStatusError, _UnserializableError,
)

VALID_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def _local_http_server(received):
    """One-shot local server recording each request's headers (lowercased)."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            received.append({k.lower(): v for k, v in self.headers.items()})
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


class ApmTestCase(unittest.TestCase):
    def setUp(self):
        self.apm = Apm(
            service="svc",
            token="rtc_test",
            api_url="https://api.example/api",
            interval_seconds=5,
            runtime_metrics=False,
        )
        apm_mod._instance = self.apm  # bind module-level instruments; no thread started
        self.sent = []
        self.apm._send = self.sent.append

    def tearDown(self):
        apm_mod._instance = None


class ApmTest(ApmTestCase):

    def test_payload_shape(self):
        orders = apm_mod.counter("orders.processed")
        orders.add()
        orders.add(2)
        queue = apm_mod.gauge("queue.depth")
        queue.set(12)
        queue.set(7)
        latency = apm_mod.timer("http.request.duration", tags={"endpoint": "/checkout"})
        latency.record(3.0)
        latency.record(250.0)
        latency.record(47.0)

        self.apm.flush()

        self.assertEqual(len(self.sent), 1)
        payload = self.sent[0]
        json.dumps(payload)  # must be serializable as-is
        self.assertEqual(
            set(payload),
            {"service", "language", "hostname", "runtime", "interval_seconds", "metrics"},
        )
        self.assertEqual(payload["service"], "svc")
        self.assertEqual(payload["language"], "python")
        self.assertEqual(payload["hostname"], socket.gethostname())
        self.assertEqual(payload["interval_seconds"], 5)
        self.assertEqual(
            set(payload["runtime"]), {"language_version", "pid", "wrapper_version"}
        )
        self.assertEqual(payload["runtime"]["pid"], os.getpid())
        self.assertEqual(payload["runtime"]["wrapper_version"], apm_mod.VERSION)

        metrics = {m["name"]: m for m in payload["metrics"]}
        self.assertEqual(
            metrics["orders.processed"],
            {"name": "orders.processed", "kind": "counter", "count": 2, "sum": 3.0},
        )
        self.assertEqual(
            metrics["queue.depth"],
            {"name": "queue.depth", "kind": "gauge", "value": 7.0},
        )
        self.assertEqual(
            metrics["http.request.duration"],
            {
                "name": "http.request.duration",
                "kind": "timer",
                "unit": "ms",
                "tags": {"endpoint": "/checkout"},
                "count": 3,
                "sum": 300.0,
                "min": 3.0,
                "max": 250.0,
                "buckets": {"46": 1, "62": 1, "71": 1},
            },
        )

    def test_counter_resets_after_flush(self):
        jobs = apm_mod.counter("jobs")
        jobs.add()
        self.apm.flush()
        jobs.add(3)
        self.apm.flush()

        self.assertEqual(len(self.sent), 2)
        self.assertEqual(
            self.sent[1]["metrics"],
            [{"name": "jobs", "kind": "counter", "count": 1, "sum": 3.0}],
        )

    def test_timer_math(self):
        t = apm_mod.timer("latency")
        for duration in (5, 1.0, 9):
            t.record(duration)

        entry = self.apm._buffer[("latency", "")]
        self.assertEqual(entry["count"], 3)
        self.assertEqual(entry["sum"], 15.0)
        self.assertEqual(entry["min"], 1.0)
        self.assertEqual(entry["max"], 9.0)

    def test_merge_back_after_send_failure(self):
        jobs = apm_mod.counter("jobs")
        for _ in range(5):
            jobs.add()
        latency = apm_mod.timer("latency")
        latency.record(10)
        latency.record(30)
        depth = apm_mod.gauge("depth")
        depth.set(1)

        def failing_send(payload):
            # simulate a recording landing while the send is in flight
            self.apm._record("depth", "gauge", None, None, 99)
            raise RuntimeError("boom")

        self.apm._send = failing_send
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        jobs.add(2)
        latency.record(5)
        self.apm._send = self.sent.append
        self.apm.flush()

        metrics = {m["name"]: m for m in self.sent[0]["metrics"]}
        self.assertEqual(metrics["jobs"]["count"], 6)
        self.assertEqual(metrics["jobs"]["sum"], 7.0)
        self.assertEqual(metrics["latency"]["count"], 3)
        self.assertEqual(metrics["latency"]["sum"], 45.0)
        self.assertEqual(metrics["latency"]["min"], 5.0)
        self.assertEqual(metrics["latency"]["max"], 30.0)
        # gauge keeps the newer, in-flight value
        self.assertEqual(metrics["depth"]["value"], 99.0)

    def test_non_finite_values_dropped(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            apm_mod.gauge("g").set(float("nan"))
            apm_mod.gauge("g").set(float("inf"))
            apm_mod.counter("c").add(float("-inf"))

        self.assertEqual(self.apm._buffer, {})
        # throttled: one warning per metric name, not per recording
        self.assertEqual(len(cm.records), 2)

    def test_send_rejects_non_finite_payload(self):
        # the real _send must refuse NaN before it reaches the wire
        with self.assertRaises(ValueError):
            Apm(
                service="svc", token="rtc_test", api_url="https://api.example/api",
                interval_seconds=5, runtime_metrics=False,
            )._send({"metrics": [{"name": "g", "kind": "gauge", "value": float("nan")}]})

    def test_unserializable_snapshot_dropped_not_merged_back(self):
        apm_mod.counter("jobs").add()

        def poison_send(payload):
            raise _UnserializableError("Out of range float values are not JSON compliant")

        self.apm._send = poison_send
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            self.apm.flush()

        self.assertEqual(self.apm._buffer, {})
        self.assertTrue(any("dropping" in r.getMessage() for r in cm.records))

    def test_merge_back_kind_change_does_not_raise(self):
        apm_mod.counter("m").add()

        def failing_send(payload):
            # the metric flips kind while the send is in flight
            self.apm._record("m", "gauge", None, None, 5)
            raise RuntimeError("boom")

        self.apm._send = failing_send
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            self.apm.flush()

        entry = self.apm._buffer[("m", "")]
        self.assertEqual(entry["kind"], "gauge")
        self.assertEqual(entry["value"], 5.0)
        self.assertTrue(any("changed kind" in r.getMessage() for r in cm.records))

    def test_runtime_metrics_fit_under_wire_cap(self):
        self.apm.runtime_metrics = True
        self.apm._cpu_sample = (0.0, self.apm._started - 1)  # force a cpu sample
        with self.assertLogs("roottrace_apm", level="WARNING"):
            for i in range(MAX_ENTRIES):
                self.apm._record(f"m{i}", "counter", None, None, 1)

        self.apm.flush()

        names = {m["name"] for m in self.sent[0]["metrics"]}
        runtime_names = {
            "process.memory.rss_bytes", "process.cpu.percent", "process.threads",
            "process.gc.collections", "process.uptime_seconds",
        }
        self.assertTrue(runtime_names <= names)
        # user series stop at the soft cap so the payload never exceeds MAX_ENTRIES
        self.assertEqual(len(names), MAX_USER_ENTRIES + len(runtime_names))
        self.assertLessEqual(len(self.sent[0]["metrics"]), MAX_ENTRIES)

    def test_tag_key_cap(self):
        tags = {f"k{i}": str(i) for i in range(12)}
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            self.apm._record("m", "counter", tags, None, 1)

        (key,) = self.apm._buffer
        entry = self.apm._buffer[key]
        self.assertEqual(len(entry["tags"]), MAX_TAG_KEYS)
        self.assertEqual(sorted(entry["tags"]), sorted(tags)[:MAX_TAG_KEYS])
        self.assertTrue(any("tag keys" in r.getMessage() for r in cm.records))

    def test_name_truncated(self):
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm._record("n" * 250, "counter", None, None, 1)

        (key,) = self.apm._buffer
        self.assertEqual(len(key[0]), MAX_NAME_LENGTH)

    def test_4xx_drops_snapshot(self):
        apm_mod.counter("jobs").add()

        def send_422(payload):
            raise _HttpStatusError(422, "malformed payload")

        self.apm._send = send_422
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            self.apm.flush()

        self.assertEqual(self.apm._buffer, {})
        self.assertTrue(any("rejected" in r.getMessage() for r in cm.records))

    def test_5xx_merges_back(self):
        apm_mod.counter("jobs").add()

        def send_500(payload):
            raise _HttpStatusError(500, "oops")

        self.apm._send = send_500
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        self.assertEqual(self.apm._buffer[("jobs", "")]["count"], 1)

    def test_429_pauses_flushes_until_retry_after(self):
        apm_mod.counter("jobs").add()

        def send_429(payload):
            raise _HttpStatusError(429, "slow down", retry_after="60")

        self.apm._send = send_429
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        # merged back, and no sends until the deadline passes
        self.assertEqual(self.apm._buffer[("jobs", "")]["count"], 1)
        self.assertGreater(self.apm._retry_at, time.monotonic())
        self.apm._send = self.sent.append
        self.apm.flush()
        self.assertEqual(self.sent, [])

        self.apm._retry_at = 0.0
        self.apm.flush()
        self.assertEqual(len(self.sent), 1)

    def test_nested_timer_context(self):
        t = apm_mod.timer("nested")
        with t:
            with t:
                pass

        entry = self.apm._buffer[("nested", "")]
        self.assertEqual(entry["count"], 2)

    def test_entry_cap(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            for i in range(MAX_ENTRIES + 5):
                self.apm._record(f"m{i}", "counter", None, None, 1)

        self.assertEqual(len(self.apm._buffer), MAX_USER_ENTRIES)
        cap_warnings = [r for r in cm.records if "buffer full" in r.getMessage()]
        self.assertEqual(len(cap_warnings), 1)

    def test_empty_name_dropped(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            apm_mod.counter("").add()
            apm_mod.counter("").add()
            apm_mod.gauge("").set(1)

        self.assertEqual(self.apm._buffer, {})
        self.assertEqual(len(cm.records), 1)  # throttled

    def test_metric_name_and_unit_coerced_to_str(self):
        # a bytes name used to make json.dumps drop the whole payload
        self.apm._record(b"bytes.name", "timer", None, b"ms", 1.5)

        (key,) = self.apm._buffer
        self.assertIsInstance(key[0], str)
        entry = self.apm._buffer[key]
        self.assertIsInstance(entry["name"], str)
        self.assertIsInstance(entry["unit"], str)
        self.apm.flush()
        json.dumps(self.sent[0])  # the payload stays serializable

    def test_errors_count_metric_name_reserved(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            apm_mod.counter("errors.count").add()
            apm_mod.counter("errors.count").add()

        self.assertEqual(self.apm._buffer, {})
        self.assertEqual(len(cm.records), 1)  # throttled

    def test_instance_tags_trimmed_once(self):
        with self.assertLogs("roottrace_apm", level="WARNING"):
            apm = Apm(
                service="svc", token="rtc_test", api_url="https://api.example/api",
                interval_seconds=5, runtime_metrics=False,
                tags={f"k{i}": str(i) for i in range(12)},
            )
        self.assertEqual(len(apm.tags), MAX_TAG_KEYS)

    def test_endpoint_url_properties_read_only(self):
        self.assertEqual(self.apm.api_url, "https://api.example/api")
        self.assertEqual(self.apm.ingest_url, "https://api.example/api/apm/ingest")
        with self.assertRaises(AttributeError):
            self.apm.api_url = "https://elsewhere.example"
        with self.assertRaises(AttributeError):
            self.apm.ingest_url = "https://elsewhere.example"

    def test_tags_key_canonicalization(self):
        self.apm._record("hits", "counter", {"b": "2", "a": "1"}, None, 1)
        self.apm._record("hits", "counter", {"a": "1", "b": "2"}, None, 1)

        self.assertEqual(list(self.apm._buffer), [("hits", "a=1,b=2")])
        self.assertEqual(self.apm._buffer[("hits", "a=1,b=2")]["count"], 2)

    def test_tags_key_escaping(self):
        # distinct tag maps must never collide in k=v,k=v form
        self.apm._record("m", "counter", {"a": "1,b=2"}, None, 1)
        self.apm._record("m", "counter", {"a": "1", "b": "2"}, None, 1)
        # '%' escapes first, so a literal "%2C" stays distinct from ","
        self.apm._record("m", "counter", {"a": "%2C"}, None, 1)
        self.apm._record("m", "counter", {"a": ","}, None, 1)

        self.assertEqual(
            {key for _, key in self.apm._buffer},
            {"a=1%2Cb%3D2", "a=1,b=2", "a=%252C", "a=%2C"},
        )
        # tags go on the wire unescaped; only the dedup key is encoded
        self.assertEqual(self.apm._buffer[("m", "a=1%2Cb%3D2")]["tags"], {"a": "1,b=2"})

    def test_timed_decorator(self):
        @apm_mod.timed("fn.duration")
        def work():
            return 7

        self.assertEqual(work(), 7)
        entry = self.apm._buffer[("fn.duration", "")]
        self.assertEqual(entry["kind"], "timer")
        self.assertEqual(entry["count"], 1)
        self.assertGreaterEqual(entry["sum"], 0.0)

    def test_bad_input_does_not_raise(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            apm_mod.counter("c").add("nope")
            apm_mod.gauge("g").set(None)
            apm_mod.timer("t").record(object())

        self.assertEqual(self.apm._buffer, {})
        self.assertEqual(len(cm.records), 3)

        # recording before init() drops with a warning instead of raising
        apm_mod._instance = None
        apm_mod._warned_no_init = False
        with self.assertLogs("roottrace_apm", level="WARNING"):
            apm_mod.counter("c").add()


class InitTest(unittest.TestCase):
    def tearDown(self):
        inst = apm_mod._instance
        if inst is not None:
            inst._send = self._drop  # the final flush must not hit the network
            apm_mod.shutdown()
        apm_mod._instance = None

    @staticmethod
    def _drop(payload):
        pass

    def test_init_fails_fast_on_bad_api_url(self):
        with self.assertRaises(ValueError):
            apm_mod.init(service="svc", token="rtc_t", api_url="ftp://api.example",
                         http_instrumentation=False)
        self.assertIsNone(apm_mod._instance)

    def test_init_truncates_service_version_and_exposes_urls(self):
        with self.assertLogs("roottrace_apm", level="WARNING"):
            inst = apm_mod.init(
                service="svc", token="rtc_t", api_url="http://api.example/api/",
                service_version="v" * 100, http_instrumentation=False,
            )
        inst._send = self._drop
        self.assertEqual(inst.service_version, "v" * 64)
        self.assertEqual(inst.api_url, "http://api.example/api")
        self.assertEqual(inst.ingest_url, "http://api.example/api/apm/ingest")


class TransactionTest(ApmTestCase):
    def test_transaction_aggregation(self):
        for _ in range(2):
            with apm_mod.transaction("GET /checkout"):
                with apm_mod.span("q", type="db", subtype="postgresql"):
                    pass
        with self.assertRaises(ValueError):
            with apm_mod.transaction("GET /checkout"):
                raise ValueError("invalid order id")

        group = self.apm._tx_buffer[("GET /checkout", "request")]
        self.assertEqual(group["count"], 3)
        self.assertEqual(group["success"], 2)
        self.assertEqual(group["failed"], 1)
        self.assertLessEqual(group["min"], group["max"])
        self.assertLessEqual(group["max"], group["sum"])
        breakdown = group["spans"][("db", "postgresql")]
        self.assertEqual(breakdown["count"], 2)
        self.assertGreaterEqual(breakdown["sum"], 0.0)

        # the escaping exception was captured against the transaction
        (error,) = self.apm._error_buffer.values()
        self.assertEqual(error["type"], "ValueError")
        self.assertEqual(error["transaction_name"], "GET /checkout")

    def test_transaction_decorator(self):
        @apm_mod.transaction("nightly", type="task")
        def job(x):
            return x * 2

        @apm_mod.transaction("nightly", type="task")
        def bad():
            raise KeyError("k")

        self.assertEqual(job(3), 6)
        self.assertEqual(job.__name__, "job")
        with self.assertRaises(KeyError):
            bad()

        group = self.apm._tx_buffer[("nightly", "task")]
        self.assertEqual(group["count"], 2)
        self.assertEqual(group["success"], 1)
        self.assertEqual(group["failed"], 1)

    def test_exit_closes_only_the_transaction_it_opened(self):
        def boom(*args, **kwargs):
            raise RuntimeError("cannot start")

        with apm_mod.transaction("outer"):
            original = apm_mod._Transaction
            apm_mod._Transaction = boom
            try:
                with self.assertLogs("roottrace_apm", level="ERROR"):
                    with apm_mod.transaction("inner") as inner:
                        self.assertIsNone(inner)
            finally:
                apm_mod._Transaction = original
            # the outer transaction must not have been recorded early
            self.assertEqual(self.apm._tx_buffer, {})
            with apm_mod.span("q", type="db"):
                pass

        group = self.apm._tx_buffer[("outer", "request")]
        self.assertEqual(group["count"], 1)
        self.assertEqual(group["spans"][("db", None)]["count"], 1)

    def test_async_transaction_decorator(self):
        @apm_mod.transaction("async-job", type="task")
        async def job():
            await asyncio.sleep(0.02)
            with apm_mod.span("q", type="db"):
                pass
            return 7

        self.assertEqual(asyncio.run(job()), 7)
        self.assertEqual(job.__name__, "job")

        group = self.apm._tx_buffer[("async-job", "task")]
        self.assertEqual(group["count"], 1)
        self.assertEqual(group["success"], 1)
        self.assertGreaterEqual(group["sum"], 15.0)  # timed across the await, not ~0ms
        self.assertEqual(group["spans"][("db", None)]["count"], 1)  # attached, not orphaned

    def test_async_transaction_decorator_captures_failure(self):
        @apm_mod.transaction("async-bad", type="task")
        async def bad():
            raise KeyError("k")

        with self.assertRaises(KeyError):
            asyncio.run(bad())

        group = self.apm._tx_buffer[("async-bad", "task")]
        self.assertEqual(group["failed"], 1)
        (error,) = self.apm._error_buffer.values()
        self.assertEqual(error["type"], "KeyError")

    def test_unhandled_capture_marks_transaction_failed(self):
        with apm_mod.transaction("risky"):
            try:
                raise ValueError("boom")
            except ValueError as exc:
                apm_mod.capture_exception(exc, handled=False)

        group = self.apm._tx_buffer[("risky", "request")]
        self.assertEqual(group["failed"], 1)
        self.assertEqual(group["success"], 0)
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["outcome"], "failed")

        with apm_mod.transaction("fine"):
            try:
                raise ValueError("boom")
            except ValueError as exc:
                apm_mod.capture_exception(exc)  # handled: outcome unchanged

        self.assertEqual(self.apm._tx_buffer[("fine", "request")]["success"], 1)

    def test_late_span_after_transaction_closed_is_noop(self):
        with apm_mod.transaction("stream") as tx:
            late = apm_mod.span("late-chunk")
            late.__enter__()
        late.__exit__(None, None, None)  # WSGI streaming edge: already recorded

        self.assertTrue(tx.closed)
        group = self.apm._tx_buffer[("stream", "request")]
        self.assertEqual(group["spans"], {})
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["spans"], [])

    def test_closed_active_transaction_treated_absent(self):
        with apm_mod.transaction("first") as tx:
            pass
        # simulate a context where the closed transaction never got reset
        token = apm_mod._active_transaction.set(tx)
        try:
            with apm_mod.span("orphan", type="db"):
                pass
            with apm_mod.transaction("second"):
                pass
        finally:
            apm_mod._active_transaction.reset(token)

        self.assertEqual(self.apm._tx_buffer[("first", "request")]["spans"], {})
        self.assertEqual(self.apm._tx_buffer[("second", "request")]["count"], 1)

    def test_interleaved_async_tasks_keep_their_own_spans(self):
        shared = apm_mod.span("op", type="db")

        async def work(name, delay):
            with apm_mod.transaction(name, type="task"):
                shared.__enter__()
                await asyncio.sleep(delay)
                shared.__exit__(None, None, None)

        async def main():
            await asyncio.gather(work("quick", 0.005), work("slow", 0.05))

        asyncio.run(main())

        quick = self.apm._tx_buffer[("quick", "task")]["spans"][("db", None)]
        slow = self.apm._tx_buffer[("slow", "task")]["spans"][("db", None)]
        self.assertEqual(quick["count"], 1)
        self.assertEqual(slow["count"], 1)
        # with a thread-local stack, quick's exit would pop slow's start
        self.assertLess(quick["sum"], slow["sum"])

    def test_transaction_and_span_names_sanitized(self):
        with apm_mod.transaction("", type="") as tx:
            with apm_mod.span("", type="", subtype=""):
                pass
            with apm_mod.span("q", type="x" * 60):
                pass

        self.assertEqual(tx.name, "unnamed")
        self.assertEqual(tx.type, "request")
        group = self.apm._tx_buffer[("unnamed", "request")]
        self.assertEqual(group["spans"][("custom", None)]["count"], 1)
        self.assertIn(("x" * 40, None), group["spans"])
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["spans"][0]["name"], "unnamed")

    def test_non_string_transaction_names_coerced(self):
        with apm_mod.transaction(123, type=456):
            pass

        self.assertIn(("123", "456"), self.apm._tx_buffer)

    def test_nested_spans_attach_to_active_transaction(self):
        with apm_mod.transaction("job", type="task"):
            with apm_mod.span("outer", type="db", subtype="postgresql"):
                with apm_mod.span("inner", type="cache", subtype="redis"):
                    pass

        group = self.apm._tx_buffer[("job", "task")]
        self.assertEqual(group["spans"][("db", "postgresql")]["count"], 1)
        self.assertEqual(group["spans"][("cache", "redis")]["count"], 1)

        (sample,) = self.apm._trace_samples
        spans = {s["name"]: s for s in sample["spans"]}
        self.assertEqual(set(spans), {"outer", "inner"})
        self.assertGreaterEqual(spans["inner"]["start_offset_ms"],
                                spans["outer"]["start_offset_ms"])
        self.assertLessEqual(spans["inner"]["duration_ms"], spans["outer"]["duration_ms"])
        self.assertEqual(sample["spans_dropped"], 0)

    def test_span_without_transaction_is_noop(self):
        with apm_mod.span("orphan", type="db"):
            pass

        self.assertEqual(self.apm._buffer, {})
        self.assertEqual(self.apm._tx_buffer, {})

    def test_trace_samples_keep_two_slowest(self):
        for name, duration in (("a", 10.0), ("b", 50.0), ("c", 30.0), ("d", 20.0)):
            tx = apm_mod._Transaction(name, "request", os.urandom(16).hex())
            self.apm._record_transaction(tx, duration, "success")

        durations = sorted(s["duration_ms"] for s in self.apm._trace_samples)
        self.assertEqual(durations, [30.0, 50.0])

    def test_transaction_trace_id(self):
        with apm_mod.transaction("t") as tx:
            pass
        self.assertRegex(tx.trace_id, "^[0-9a-f]{32}$")

        with apm_mod.transaction("t", traceparent=VALID_TRACEPARENT) as adopted:
            pass
        self.assertEqual(adopted.trace_id, "0af7651916cd43dd8448eb211c80319c")

    def test_transaction_group_cap(self):
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            for i in range(MAX_TX_GROUPS + 5):
                tx = apm_mod._Transaction(f"t{i}", "request", "ab" * 16)
                self.apm._record_transaction(tx, 1.0, "success")

        self.assertEqual(len(self.apm._tx_buffer), MAX_TX_GROUPS)
        cap_warnings = [r for r in cm.records if "transaction buffer full" in r.getMessage()]
        self.assertEqual(len(cap_warnings), 1)  # throttled

    def test_parse_traceparent(self):
        trace_id = "0af7651916cd43dd8448eb211c80319c"
        self.assertEqual(apm_mod._parse_traceparent(VALID_TRACEPARENT), trace_id)
        # case-insensitive, normalized to lowercase
        self.assertEqual(apm_mod._parse_traceparent(VALID_TRACEPARENT.upper()), trace_id)
        # future versions parse, with or without extra fields (W3C SHOULD)
        self.assertEqual(
            apm_mod._parse_traceparent(f"01-{trace_id}-b7ad6b7169203331-01"), trace_id)
        self.assertEqual(
            apm_mod._parse_traceparent(f"cc-{trace_id}-b7ad6b7169203331-01-extra-state"),
            trace_id)
        for bad in (
            None,
            "",
            "nonsense",
            "00-short-b7ad6b7169203331-01",
            f"ff-{trace_id}-b7ad6b7169203331-01",  # version ff is forbidden
            f"00-{trace_id}-b7ad6b7169203331-01-extra",  # version 00 has no suffix
            f"00-{'0' * 32}-b7ad6b7169203331-01",  # all-zero trace id
            f"00-{trace_id}-{'0' * 16}-01",  # all-zero parent id
        ):
            self.assertIsNone(apm_mod._parse_traceparent(bad), bad)


class ErrorTest(ApmTestCase):
    def _capture(self, exc_type=ValueError, message="boom"):
        try:
            raise exc_type(message)
        except Exception as exc:
            apm_mod.capture_exception(exc)

    def test_fingerprint_stable_across_captures(self):
        self._capture()
        self._capture()

        (error,) = self.apm._error_buffer.values()
        self.assertRegex(error["fingerprint"], "^[0-9a-f]{16}$")
        self.assertEqual(error["count"], 2)
        self.assertEqual(error["type"], "ValueError")
        self.assertEqual(error["message"], "boom")
        self.assertTrue(error["culprit"].endswith("._capture"))
        self.assertEqual(error["stack"][-1]["function"], "_capture")

    def test_message_truncated(self):
        self._capture(message="x" * 5000)

        (error,) = self.apm._error_buffer.values()
        self.assertEqual(len(error["message"]), 1000)

    def test_error_fields_clipped(self):
        namespace = {}
        exec("def {name}():\n    raise ValueError('x')".format(name="f" * 350), namespace)
        try:
            namespace["f" * 350]()
        except ValueError as exc:
            apm_mod.capture_exception(exc)

        long_type = type("E" * 300, (Exception,), {})
        try:
            raise long_type("boom")
        except Exception as exc:
            apm_mod.capture_exception(exc)

        errors = {e["type"]: e for e in self.apm._error_buffer.values()}
        self.assertIn("E" * 200, errors)  # type name clipped to 200
        clipped = errors["ValueError"]
        self.assertTrue(any(len(f["function"]) == 300 for f in clipped["stack"]))
        self.assertLessEqual(len(clipped["culprit"]), 300)
        for error in errors.values():
            for frame in error["stack"]:
                self.assertLessEqual(len(frame["function"]), 300)
                self.assertLessEqual(len(frame["file"]), 1024)

    def test_error_cap(self):
        types = [type(f"Err{i}", (Exception,), {}) for i in range(MAX_ERRORS + 5)]
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            for exc_type in types:
                self._capture(exc_type=exc_type)
            self._capture(exc_type=types[0])  # repeats on kept errors still count

        self.assertEqual(len(self.apm._error_buffer), MAX_ERRORS)
        cap_warnings = [r for r in cm.records if "error buffer full" in r.getMessage()]
        self.assertEqual(len(cap_warnings), 1)  # throttled
        first = next(iter(self.apm._error_buffer.values()))
        self.assertEqual(first["count"], 2)


class WsgiTest(ApmTestCase):
    def _app(self, status="200 OK"):
        def app(environ, start_response):
            start_response(status, [("Content-Type", "text/plain")])
            return [b"ok"]
        return app

    def _run(self, mw, path="/", **environ_extra):
        environ = {"REQUEST_METHOD": "GET", "PATH_INFO": path}
        environ.update(environ_extra)
        return b"".join(mw(environ, lambda status, headers, exc_info=None: None))

    def test_creates_named_transaction_and_keeps_metrics(self):
        mw = WsgiMiddleware(self._app())
        body = self._run(mw, "/orders/123/items/550e8400-e29b-41d4-a716-446655440000")

        self.assertEqual(body, b"ok")
        group = self.apm._tx_buffer[("GET /orders/:id/items/:id", "request")]
        self.assertEqual(group["count"], 1)
        self.assertEqual(group["success"], 1)
        # the pre-existing metrics keep working unchanged
        tags_key = "method=GET,status=2xx"
        self.assertEqual(self.apm._buffer[("http.request.duration", tags_key)]["count"], 1)
        self.assertEqual(self.apm._buffer[("http.requests", tags_key)]["count"], 1)

    def test_normalizes_dashless_hex_ids(self):
        mw = WsgiMiddleware(self._app())
        self._run(mw, "/orders/665f1c2b9a8d4e0012345678/report")

        self.assertIn(("GET /orders/:id/report", "request"), self.apm._tx_buffer)

    def test_name_callback_override(self):
        mw = WsgiMiddleware(self._app(), name_callback=lambda environ: "custom-name")
        self._run(mw, "/whatever/123")

        self.assertIn(("custom-name", "request"), self.apm._tx_buffer)

    def test_adopts_valid_traceparent(self):
        mw = WsgiMiddleware(self._app())
        self._run(mw, "/", HTTP_TRACEPARENT=VALID_TRACEPARENT.upper())

        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["trace_id"], "0af7651916cd43dd8448eb211c80319c")

    def test_rejects_malformed_traceparent(self):
        mw = WsgiMiddleware(self._app())
        self._run(mw, "/", HTTP_TRACEPARENT="00-not-a-trace-id-01")

        (sample,) = self.apm._trace_samples
        self.assertRegex(sample["trace_id"], "^[0-9a-f]{32}$")

    def test_captures_http_context_on_trace_sample(self):
        mw = WsgiMiddleware(self._app())
        self._run(mw, "/orders/7", QUERY_STRING="page=2",
                  REMOTE_ADDR="10.0.0.9",
                  HTTP_X_FORWARDED_FOR="203.0.113.7, 10.0.0.1",
                  HTTP_USER_AGENT="pytest-agent")

        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["http"], {
            "method": "GET",
            "path": "/orders/7?page=2",
            "client_ip": "203.0.113.7",  # first X-Forwarded-For hop
            "remote_ip": "10.0.0.9",     # the socket peer, kept alongside
            "user_agent": "pytest-agent",
            "status_code": 200,
        })

    def test_http_context_without_proxy_uses_peer_address(self):
        mw = WsgiMiddleware(self._app("404 Not Found"))
        self._run(mw, "/missing", REMOTE_ADDR="10.0.0.9")

        (sample,) = self.apm._trace_samples
        http = sample["http"]
        self.assertEqual(http["client_ip"], "10.0.0.9")
        self.assertEqual(http["remote_ip"], "10.0.0.9")  # always the socket peer
        self.assertNotIn("user_agent", http)
        self.assertEqual(http["status_code"], 404)
        self.assertEqual(http["path"], "/missing")

    def test_malformed_status_never_raises(self):
        # "²00" passes isdigit() but int() rejects it; the middleware must
        # swallow it, keep serving, and just omit the status code.
        mw = WsgiMiddleware(self._app("²00 Weird"))
        body = self._run(mw, "/odd", REMOTE_ADDR="10.0.0.9")

        self.assertEqual(body, b"ok")
        (sample,) = self.apm._trace_samples
        self.assertNotIn("status_code", sample["http"])

    def test_5xx_marks_failed(self):
        mw = WsgiMiddleware(self._app("500 Internal Server Error"))
        self._run(mw, "/boom")

        group = self.apm._tx_buffer[("GET /boom", "request")]
        self.assertEqual(group["failed"], 1)
        self.assertEqual(group["success"], 0)

    def test_exception_captured_and_reraised(self):
        def app(environ, start_response):
            raise RuntimeError("kaboom")

        mw = WsgiMiddleware(app)
        with self.assertRaises(RuntimeError):
            self._run(mw, "/explode")

        group = self.apm._tx_buffer[("GET /explode", "request")]
        self.assertEqual(group["failed"], 1)
        (error,) = self.apm._error_buffer.values()
        self.assertEqual(error["type"], "RuntimeError")
        self.assertEqual(error["transaction_name"], "GET /explode")


class HttpClientInstrumentationTest(ApmTestCase):
    def test_outbound_call_instrumented_end_to_end(self):
        received = []
        server = _local_http_server(received)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        apm_mod._instrument_http_client()
        patched = http.client.HTTPConnection.request
        apm_mod._instrument_http_client()  # idempotent: no double patch
        self.assertIs(http.client.HTTPConnection.request, patched)

        port = server.server_address[1]
        with apm_mod.transaction("GET /outbound") as tx:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/data")
            response = conn.getresponse()
            self.assertEqual(response.read(), b"ok")
            conn.close()

        # the server saw a well-formed traceparent carrying the trace id
        (headers,) = received
        self.assertRegex(headers.get("traceparent", ""),
                         f"^00-{tx.trace_id}-[0-9a-f]{{16}}-01$")

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        timer_entry = self.apm._buffer[("http.client.duration", tags_key)]
        self.assertEqual(timer_entry["kind"], "timer")
        self.assertEqual(timer_entry["count"], 1)
        self.assertEqual(timer_entry["tags"], {"destination": destination, "status": "2xx"})
        counter_entry = self.apm._buffer[("http.client.requests", tags_key)]
        self.assertEqual(counter_entry["count"], 1)

        # and the call became an http span on the active transaction
        group = self.apm._tx_buffer[("GET /outbound", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)
        (sample,) = self.apm._trace_samples
        (http_span,) = sample["spans"]
        self.assertEqual(http_span["name"], f"GET {destination}")
        self.assertEqual(http_span["subtype"], destination)

    def test_failed_outbound_call_records_error_status(self):
        apm_mod._instrument_http_client()
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()  # nothing listens here anymore

        with apm_mod.transaction("GET /down"):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            try:
                with self.assertRaises(OSError):
                    conn.request("GET", "/")
            finally:
                conn.close()

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=error"
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)
        self.assertEqual(self.apm._buffer[("http.client.duration", tags_key)]["count"], 1)
        group = self.apm._tx_buffer[("GET /down", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)


@unittest.skipUnless(requests is not None, "requests is not installed")
class RequestsInstrumentationTest(ApmTestCase):
    def test_requests_get_instrumented_end_to_end(self):
        # requests rides urllib3; urllib3 2.x overrides request() without
        # calling the stdlib one, so this exercises the putrequest/endheaders
        # layer of the patch.
        received = []
        server = _local_http_server(received)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]

        apm_mod._instrument_http_client()
        with apm_mod.transaction("GET /via-requests") as tx:
            session = requests.Session()
            session.trust_env = False  # no proxy env vars in the way
            self.addCleanup(session.close)
            response = session.get(f"http://127.0.0.1:{port}/data", timeout=5)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "ok")

        # the server saw a well-formed traceparent carrying the trace id
        (headers,) = received
        self.assertRegex(headers.get("traceparent", ""),
                         f"^00-{tx.trace_id}-[0-9a-f]{{16}}-01$")

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        timer_entry = self.apm._buffer[("http.client.duration", tags_key)]
        self.assertEqual(timer_entry["kind"], "timer")
        self.assertEqual(timer_entry["count"], 1)
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)

        group = self.apm._tx_buffer[("GET /via-requests", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)
        (sample,) = self.apm._trace_samples
        (http_span,) = sample["spans"]
        self.assertEqual(http_span["name"], f"GET {destination}")


@unittest.skipUnless(httpx is not None, "httpx is not installed")
class HttpxInstrumentationTest(ApmTestCase):
    def test_sync_client_instrumented_end_to_end(self):
        received = []
        server = _local_http_server(received)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]

        apm_mod._instrument_httpx()
        patched = httpx.Client.send
        apm_mod._instrument_httpx()  # idempotent: no double patch
        self.assertIs(httpx.Client.send, patched)

        with apm_mod.transaction("GET /via-httpx") as tx:
            with httpx.Client(trust_env=False) as client:
                response = client.get(f"http://127.0.0.1:{port}/data", timeout=5)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, "ok")

        # the server saw a well-formed traceparent carrying the trace id
        (headers,) = received
        self.assertRegex(headers.get("traceparent", ""),
                         f"^00-{tx.trace_id}-[0-9a-f]{{16}}-01$")

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        timer_entry = self.apm._buffer[("http.client.duration", tags_key)]
        self.assertEqual(timer_entry["kind"], "timer")
        self.assertEqual(timer_entry["count"], 1)
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)

        group = self.apm._tx_buffer[("GET /via-httpx", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)
        (sample,) = self.apm._trace_samples
        (http_span,) = sample["spans"]
        self.assertEqual(http_span["name"], f"GET {destination}")
        self.assertEqual(http_span["subtype"], destination)

    def test_async_client_instrumented_end_to_end(self):
        received = []
        server = _local_http_server(received)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]

        apm_mod._instrument_httpx()

        async def call():
            async with httpx.AsyncClient(trust_env=False) as client:
                return await client.get(f"http://127.0.0.1:{port}/data", timeout=5)

        with apm_mod.transaction("GET /via-httpx-async") as tx:
            response = asyncio.run(call())
        self.assertEqual(response.status_code, 200)

        (headers,) = received
        self.assertRegex(headers.get("traceparent", ""),
                         f"^00-{tx.trace_id}-[0-9a-f]{{16}}-01$")

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        self.assertEqual(self.apm._buffer[("http.client.duration", tags_key)]["count"], 1)
        group = self.apm._tx_buffer[("GET /via-httpx-async", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)

    def test_failed_call_records_error_status(self):
        apm_mod._instrument_httpx()
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()  # nothing listens here anymore

        with apm_mod.transaction("GET /down-httpx"):
            with httpx.Client(trust_env=False) as client:
                with self.assertRaises(httpx.ConnectError):
                    client.get(f"http://127.0.0.1:{port}/", timeout=1)

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=error"
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)
        self.assertEqual(self.apm._buffer[("http.client.duration", tags_key)]["count"], 1)
        group = self.apm._tx_buffer[("GET /down-httpx", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)


class PayloadAndMergeBackTest(ApmTestCase):
    def _capture_value_error(self):
        try:
            raise ValueError("bad")
        except ValueError as exc:
            apm_mod.capture_exception(exc)

    def test_payload_shape_matches_protocol(self):
        self.apm.service_version = "1.2.3"
        with apm_mod.transaction("GET /checkout"):
            with apm_mod.span("q", type="db", subtype="postgresql"):
                pass
            self._capture_value_error()
        apm_mod.counter("jobs").add()

        self.apm.flush()

        payload = self.sent[0]
        json.dumps(payload)  # must be serializable as-is
        self.assertEqual(
            set(payload),
            {"service", "language", "hostname", "runtime", "interval_seconds",
             "metrics", "service_version", "transactions", "errors", "trace_samples"},
        )
        self.assertEqual(payload["service_version"], "1.2.3")

        (tx_entry,) = payload["transactions"]
        self.assertEqual(
            set(tx_entry),
            {"name", "type", "count", "sum", "min", "max", "success", "failed",
             "spans", "buckets"},
        )
        self.assertEqual(tx_entry["name"], "GET /checkout")
        self.assertEqual(tx_entry["type"], "request")
        self.assertEqual(tx_entry["count"], 1)
        self.assertEqual(tx_entry["success"], 1)
        self.assertEqual(tx_entry["failed"], 0)
        (span_row,) = tx_entry["spans"]
        self.assertEqual(set(span_row), {"type", "subtype", "count", "sum"})
        self.assertEqual(span_row["type"], "db")
        self.assertEqual(span_row["subtype"], "postgresql")

        (error,) = payload["errors"]
        self.assertEqual(
            set(error),
            {"fingerprint", "type", "message", "culprit", "count",
             "transaction_name", "stack"},
        )
        self.assertEqual(error["transaction_name"], "GET /checkout")
        for frame in error["stack"]:
            self.assertEqual(set(frame), {"function", "file", "line"})

        (sample,) = payload["trace_samples"]
        self.assertEqual(
            set(sample),
            {"trace_id", "transaction_name", "transaction_type", "duration_ms",
             "started_at", "outcome", "spans_dropped", "spans"},
        )
        self.assertRegex(sample["trace_id"], "^[0-9a-f]{32}$")
        self.assertEqual(sample["outcome"], "success")
        (sample_span,) = sample["spans"]
        self.assertEqual(
            set(sample_span),
            {"name", "type", "subtype", "start_offset_ms", "duration_ms"},
        )

    def test_merge_back_of_all_buffers_after_5xx(self):
        for name, duration in (("a", 100.0), ("a", 50.0)):
            tx = apm_mod._Transaction(name, "request", os.urandom(16).hex())
            self.apm._record_transaction(tx, duration, "success")
        self._capture_value_error()

        def send_500(payload):
            raise _HttpStatusError(500, "oops")

        self.apm._send = send_500
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        tx = apm_mod._Transaction("a", "request", os.urandom(16).hex())
        self.apm._record_transaction(tx, 300.0, "failed")
        self._capture_value_error()
        self.apm._send = self.sent.append
        self.apm.flush()

        payload = self.sent[0]
        (group,) = payload["transactions"]
        self.assertEqual(group["count"], 3)
        self.assertEqual(group["sum"], 450.0)
        self.assertEqual(group["min"], 50.0)
        self.assertEqual(group["max"], 300.0)
        self.assertEqual(group["success"], 2)
        self.assertEqual(group["failed"], 1)

        (error,) = payload["errors"]
        self.assertEqual(error["count"], 2)  # merged additively by fingerprint

        # the two slowest of both sample sets survive, slowest first
        durations = [s["duration_ms"] for s in payload["trace_samples"]]
        self.assertEqual(durations, [300.0, 100.0])


class KubernetesContextTest(unittest.TestCase):
    """Kubernetes context detection and payload shape (PROTOCOL.md)."""

    POD = "checkout-api-7d9f8b6c5d-x2k4p"

    def _apm(self, hostname="web-1", env_vars=None, **kwargs):
        # Blank defaults neutralize any real values leaking in from the host;
        # env() strips, so "" behaves like unset. Both patches end before the
        # Apm is returned: detection must have run once, at init.
        overrides = {
            "KUBERNETES_SERVICE_HOST": "",
            "ROOTTRACE_APM_DEPLOYMENT": "",
            "ROOTTRACE_APM_NAMESPACE": "",
        }
        overrides.update(env_vars or {})
        with mock.patch.dict(os.environ, overrides), \
                mock.patch("socket.gethostname", return_value=hostname):
            return Apm(service="svc", token="rtc_test",
                       api_url="https://api.example/api", interval_seconds=5,
                       runtime_metrics=False, **kwargs)

    def _payload(self, apm):
        return apm._build_payload({}, {}, {}, [])

    def test_explicit_env_vars_win_over_detection(self):
        apm = self._apm(
            hostname=self.POD,
            env_vars={"KUBERNETES_SERVICE_HOST": "10.0.0.1",
                      "ROOTTRACE_APM_DEPLOYMENT": " checkout ",
                      "ROOTTRACE_APM_NAMESPACE": "prod"},
        )
        self.assertEqual(apm.deployment, "checkout")  # trimmed, not derived
        self.assertEqual(apm.namespace, "prod")
        self.assertEqual(
            self._payload(apm)["kubernetes"],
            {"deployment": "checkout", "namespace": "prod", "pod": self.POD},
        )

    def test_init_arguments_win_over_env_vars(self):
        apm = self._apm(
            env_vars={"ROOTTRACE_APM_DEPLOYMENT": "from-env"},
            deployment="from-arg", namespace=123,  # str()-coerced
        )
        self.assertEqual(apm.deployment, "from-arg")
        self.assertEqual(apm.namespace, "123")

    def test_deployment_derived_from_replicaset_pod_name(self):
        apm = self._apm(hostname=self.POD,
                        env_vars={"KUBERNETES_SERVICE_HOST": "10.0.0.1"})
        self.assertEqual(apm.deployment, "checkout-api")
        kubernetes = self._payload(apm)["kubernetes"]
        self.assertEqual(kubernetes["deployment"], "checkout-api")
        self.assertEqual(kubernetes["pod"], self.POD)
        # the serviceaccount namespace file is unreadable here
        self.assertNotIn("namespace", kubernetes)

    def test_deployment_derived_from_statefulset_pod_name(self):
        apm = self._apm(hostname="kafka-2",
                        env_vars={"KUBERNETES_SERVICE_HOST": "10.0.0.1"})
        self.assertEqual(apm.deployment, "kafka")

    def test_unrecognized_pod_name_sent_as_is(self):
        apm = self._apm(hostname="oddly.named.host",
                        env_vars={"KUBERNETES_SERVICE_HOST": "10.0.0.1"})
        self.assertEqual(apm.deployment, "oddly.named.host")

    def test_outside_kubernetes_payload_has_no_kubernetes_key(self):
        apm = self._apm()
        self.assertIsNone(apm.deployment)
        self.assertIsNone(apm.namespace)
        self.assertNotIn("kubernetes", self._payload(apm))

    def test_names_truncated_at_253_chars(self):
        apm = self._apm(deployment="d" * 300, namespace="n" * 300)
        self.assertEqual(apm.deployment, "d" * 253)
        self.assertEqual(apm.namespace, "n" * 253)


class SqlSpanNameTest(unittest.TestCase):
    def test_operation_and_table(self):
        self.assertEqual(apm_mod._sql_span_name("SELECT * FROM users WHERE id = 1"),
                         "SELECT users")
        self.assertEqual(apm_mod._sql_span_name("insert into orders values (1)"),
                         "INSERT orders")
        self.assertEqual(apm_mod._sql_span_name("UPDATE public.accounts SET x=1"),
                         "UPDATE public.accounts")
        self.assertEqual(apm_mod._sql_span_name("BEGIN"), "BEGIN")
        self.assertEqual(apm_mod._sql_span_name(""), "SQL")


@unittest.skipUnless(aiohttp is not None, "aiohttp is not installed")
class AiohttpInstrumentationTest(ApmTestCase):
    def test_client_session_instrumented_end_to_end(self):
        received = []
        server = _local_http_server(received)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]

        apm_mod._instrument_aiohttp()
        patched = aiohttp.ClientSession._request
        apm_mod._instrument_aiohttp()  # idempotent: no double patch
        self.assertIs(aiohttp.ClientSession._request, patched)

        async def call():
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/data") as response:
                    await response.read()
                    return response.status

        with apm_mod.transaction("GET /via-aiohttp") as tx:
            status = asyncio.run(call())
        self.assertEqual(status, 200)

        (headers,) = received
        self.assertRegex(headers.get("traceparent", ""),
                         f"^00-{tx.trace_id}-[0-9a-f]{{16}}-01$")

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        self.assertEqual(self.apm._buffer[("http.client.duration", tags_key)]["count"], 1)
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)
        group = self.apm._tx_buffer[("GET /via-aiohttp", "request")]
        self.assertEqual(group["spans"][("http", destination)]["count"], 1)
        (sample,) = self.apm._trace_samples
        (span,) = sample["spans"]
        self.assertEqual(span["name"], f"GET {destination}")


@unittest.skipUnless(httpx is not None, "httpx is not installed")
class SpanSuppressionTest(ApmTestCase):
    def test_suppressed_call_keeps_metrics_but_not_span(self):
        # The db instrumentation (e.g. Elasticsearch) sets suppression while
        # its transport runs: metrics still count, the waterfall shows one
        # logical operation instead of two.
        server = _local_http_server([])
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_address[1]
        apm_mod._instrument_httpx()

        with apm_mod.transaction("GET /suppressed"):
            token = apm_mod._span_suppressed.set(True)
            try:
                with httpx.Client(trust_env=False) as client:
                    client.get(f"http://127.0.0.1:{port}/data", timeout=5)
            finally:
                apm_mod._span_suppressed.reset(token)

        destination = f"127.0.0.1:{port}"
        tags_key = f"destination={destination},status=2xx"
        self.assertEqual(self.apm._buffer[("http.client.requests", tags_key)]["count"], 1)
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["spans"], [])


def _mongo_event(command_name, request_id, duration_micros=1500,
                 database_name="app", command=None):
    return types.SimpleNamespace(
        command_name=command_name,
        request_id=request_id,
        duration_micros=duration_micros,
        database_name=database_name,
        command=command if command is not None else {command_name: "users"},
    )


@unittest.skipUnless(pymongo is not None, "pymongo is not installed")
class MongoListenerTest(ApmTestCase):
    def test_command_becomes_a_db_span(self):
        listener = apm_mod._make_mongo_listener()
        with apm_mod.transaction("GET /mongo"):
            listener.started(_mongo_event("find", request_id=7))
            listener.succeeded(_mongo_event("find", request_id=7))

        group = self.apm._tx_buffer[("GET /mongo", "request")]
        self.assertEqual(group["spans"][("db", "mongodb")]["count"], 1)
        (sample,) = self.apm._trace_samples
        (span,) = sample["spans"]
        self.assertEqual(span["name"], "find app.users")
        self.assertEqual(span["duration_ms"], 1.5)

    def test_failed_command_still_recorded(self):
        listener = apm_mod._make_mongo_listener()
        with apm_mod.transaction("GET /mongo-fail"):
            listener.started(_mongo_event("update", request_id=9))
            listener.failed(_mongo_event("update", request_id=9))
        (sample,) = self.apm._trace_samples
        (span,) = sample["spans"]
        self.assertEqual(span["name"], "update app.users")

    def test_handshake_commands_are_skipped(self):
        listener = apm_mod._make_mongo_listener()
        with apm_mod.transaction("GET /mongo-noise"):
            listener.started(_mongo_event("hello", request_id=1))
            listener.succeeded(_mongo_event("hello", request_id=1))
            listener.started(_mongo_event("ping", request_id=2, command={}))
            listener.succeeded(_mongo_event("ping", request_id=2, command={}))
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["spans"], [])

    def test_no_transaction_is_a_noop(self):
        listener = apm_mod._make_mongo_listener()
        listener.started(_mongo_event("find", request_id=3))
        listener.succeeded(_mongo_event("find", request_id=3))
        self.assertEqual(self.apm._trace_samples, [])


@unittest.skipUnless(motor_asyncio_framework is not None, "motor is not installed")
class MotorContextTest(unittest.TestCase):
    def test_executor_calls_see_the_callers_context(self):
        apm_mod._patch_motor_context()
        self.assertTrue(motor_asyncio_framework._roottrace_apm_context)
        before = motor_asyncio_framework.run_on_executor
        apm_mod._patch_motor_context()  # idempotent
        self.assertIs(motor_asyncio_framework.run_on_executor, before)

        var = contextvars.ContextVar("motor-test", default=None)

        async def run():
            loop = asyncio.get_running_loop()
            var.set("visible")
            return await motor_asyncio_framework.run_on_executor(loop, var.get)

        self.assertEqual(asyncio.run(run()), "visible")


@unittest.skipUnless(elasticsearch is not None, "elasticsearch is not installed")
class ElasticsearchPatchTest(unittest.TestCase):
    def test_perform_request_is_wrapped(self):
        apm_mod._instrument_elasticsearch()
        self.assertTrue(hasattr(elasticsearch.Elasticsearch.perform_request,
                                "__wrapped__"))
        before = elasticsearch.Elasticsearch.perform_request
        apm_mod._instrument_elasticsearch()  # idempotent
        self.assertIs(elasticsearch.Elasticsearch.perform_request, before)


class HistogramBucketTest(ApmTestCase):
    def test_bucket_index_exact_values(self):
        self.assertEqual(apm_mod._bucket_index(1.0), 40)
        self.assertEqual(apm_mod._bucket_index(1000.0), 79)
        self.assertEqual(apm_mod._bucket_index(0.5), 36)
        # sub-ms clamps toward 0; huge durations clamp at 127
        self.assertEqual(apm_mod._bucket_index(0.0), 0)
        self.assertEqual(apm_mod._bucket_index(-5.0), 0)
        self.assertEqual(apm_mod._bucket_index(1e12), 127)

    def test_timer_buckets_accumulate_and_ride_the_payload(self):
        t = apm_mod.timer("latency")
        t.record(1.0)
        t.record(1.0)
        t.record(1000.0)

        entry = self.apm._buffer[("latency", "")]
        self.assertEqual(entry["buckets"], {40: 2, 79: 1})

        self.apm.flush()
        (metric,) = self.sent[0]["metrics"]
        # stringified indexes on the wire, and still JSON-serializable
        self.assertEqual(metric["buckets"], {"40": 2, "79": 1})
        json.dumps(self.sent[0])

    def test_transaction_group_buckets(self):
        for duration in (1.0, 1.0, 1000.0):
            tx = apm_mod._Transaction("t", "request", os.urandom(16).hex())
            self.apm._record_transaction(tx, duration, "success")

        group = self.apm._tx_buffer[("t", "request")]
        self.assertEqual(group["buckets"], {40: 2, 79: 1})

        self.apm.flush()
        (tx_entry,) = self.sent[0]["transactions"]
        self.assertEqual(tx_entry["buckets"], {"40": 2, "79": 1})

    def test_merge_back_merges_buckets(self):
        apm_mod.timer("latency").record(1.0)
        tx = apm_mod._Transaction("t", "request", os.urandom(16).hex())
        self.apm._record_transaction(tx, 1.0, "success")

        def failing_send(payload):
            raise RuntimeError("boom")

        self.apm._send = failing_send
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        apm_mod.timer("latency").record(1.0)
        apm_mod.timer("latency").record(1000.0)
        tx = apm_mod._Transaction("t", "request", os.urandom(16).hex())
        self.apm._record_transaction(tx, 1000.0, "success")
        self.apm._send = self.sent.append
        self.apm.flush()

        metrics = {m["name"]: m for m in self.sent[0]["metrics"]}
        self.assertEqual(metrics["latency"]["buckets"], {"40": 2, "79": 1})
        (group,) = self.sent[0]["transactions"]
        self.assertEqual(group["buckets"], {"40": 1, "79": 1})


class LogHandlerTest(ApmTestCase):
    def _logger(self):
        log = logging.Logger("app.orders")  # standalone: no global registry
        log.addHandler(RootTraceLogHandler(self.apm))
        return log

    def test_records_batched_with_fields(self):
        log = self._logger()
        log.info("hello %s", "world", extra={"order_id": 7, "api_key": "sk-1234"})

        (entry,) = self.apm._log_buffer
        self.assertEqual(entry["service"], "svc")
        self.assertEqual(entry["level"], "INFO")
        self.assertEqual(entry["message"], "hello world")
        self.assertEqual(entry["logger"], "app.orders")
        self.assertIn("T", entry["timestamp"])  # ISO 8601
        self.assertNotIn("trace_id", entry)
        # extras ride as attrs; secret-looking keys are redacted
        self.assertEqual(entry["attrs"], {"order_id": 7, "api_key": "[REDACTED]"})

    def test_trace_id_from_active_transaction(self):
        log = self._logger()
        with apm_mod.transaction("job", type="task") as tx:
            log.warning("inside")
        log.warning("outside")

        inside, outside = self.apm._log_buffer
        self.assertEqual(inside["trace_id"], tx.trace_id)
        self.assertNotIn("trace_id", outside)

    def test_message_truncated_at_8kb(self):
        log = self._logger()
        log.error("x" * (MAX_LOG_MESSAGE_LENGTH + 500))

        (entry,) = self.apm._log_buffer
        self.assertEqual(len(entry["message"]), MAX_LOG_MESSAGE_LENGTH)

    def test_cap_drops_oldest_with_counted_warning(self):
        log = self._logger()
        with self.assertLogs("roottrace_apm", level="WARNING") as cm:
            for i in range(MAX_LOG_ENTRIES + 5):
                log.info("m%d", i)

        self.assertEqual(len(self.apm._log_buffer), MAX_LOG_ENTRIES)
        self.assertEqual(self.apm._logs_dropped, 5)
        self.assertEqual(self.apm._log_buffer[0]["message"], "m5")  # oldest gone
        cap_warnings = [r for r in cm.records if "log buffer full" in r.getMessage()]
        self.assertEqual(len(cap_warnings), 1)  # throttled

    def test_flush_posts_logs_and_resets_buffer(self):
        posted = []
        self.apm._send_logs = posted.append
        log = self._logger()
        log.info("one")
        log.info("two")

        self.apm.flush()

        (payload,) = posted
        json.dumps(payload)
        self.assertEqual(payload["service"], "svc")
        self.assertEqual([e["message"] for e in payload["logs"]], ["one", "two"])
        self.assertEqual(self.apm._log_buffer, [])
        self.assertEqual(self.sent, [])  # no metrics, so no metric flush

    def test_failed_log_flush_merges_back(self):
        log = self._logger()
        log.info("kept")

        def failing_send(payload):
            raise RuntimeError("boom")

        self.apm._send_logs = failing_send
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        log.info("newer")
        # unsent records precede whatever arrived meanwhile
        self.assertEqual([e["message"] for e in self.apm._log_buffer],
                         ["kept", "newer"])

    def test_4xx_drops_log_batch(self):
        log = self._logger()
        log.info("rejected")

        def send_422(payload):
            raise _HttpStatusError(422, "malformed")

        self.apm._send_logs = send_422
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.flush()

        self.assertEqual(self.apm._log_buffer, [])

    def test_emit_never_raises(self):
        log = self._logger()
        old = logging.raiseExceptions
        logging.raiseExceptions = False  # keep handleError quiet
        try:
            log.info("%d", "not-a-number")  # getMessage() raises inside emit
        finally:
            logging.raiseExceptions = old
        self.assertEqual(self.apm._log_buffer, [])

    def test_own_logger_records_skipped(self):
        handler = RootTraceLogHandler(self.apm)
        apm_logger = logging.Logger("roottrace_apm")
        apm_logger.addHandler(handler)
        apm_logger.warning("internal")
        self.assertEqual(self.apm._log_buffer, [])


class AsgiTest(ApmTestCase):
    def _scope(self, path="/", method="GET", headers=None, **extra):
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": headers or [],
            "client": ("10.0.0.9", 51000),
            "server": ("127.0.0.1", 80),
        }
        scope.update(extra)
        return scope

    def _app(self, status=200, route_path=None):
        async def app(scope, receive, send):
            if route_path is not None:  # what Starlette does after routing
                scope["route"] = types.SimpleNamespace(path=route_path)
            await send({"type": "http.response.start", "status": status,
                        "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"ok"})
        return app

    def _run(self, mw, scope):
        sent = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        asyncio.run(mw(scope, receive, send))
        return sent

    def test_raw_path_fallback_and_metrics(self):
        mw = AsgiMiddleware(self._app())
        sent = self._run(mw, self._scope("/orders/123"))

        self.assertEqual(sent[-1]["body"], b"ok")
        # no route on the scope: the raw path, parameters left as-is
        group = self.apm._tx_buffer[("GET /orders/123", "request")]
        self.assertEqual(group["count"], 1)
        self.assertEqual(group["success"], 1)
        tags_key = "method=GET,status=2xx"
        self.assertEqual(self.apm._buffer[("http.request.duration", tags_key)]["count"], 1)
        self.assertEqual(self.apm._buffer[("http.requests", tags_key)]["count"], 1)

    def test_route_template_names_transaction(self):
        mw = AsgiMiddleware(self._app(route_path="/orders/{order_id}"))
        self._run(mw, self._scope("/orders/123"))

        self.assertIn(("GET /orders/{order_id}", "request"), self.apm._tx_buffer)
        # the raw path still rides the trace sample's http context
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["http"]["path"], "/orders/123")

    def test_non_http_scopes_pass_through(self):
        seen = []

        async def app(scope, receive, send):
            seen.append(scope["type"])

        mw = AsgiMiddleware(app)
        for scope_type in ("lifespan", "websocket"):
            asyncio.run(mw({"type": scope_type}, None, None))

        self.assertEqual(seen, ["lifespan", "websocket"])
        self.assertEqual(self.apm._tx_buffer, {})
        self.assertEqual(self.apm._buffer, {})

    def test_adopts_valid_traceparent(self):
        mw = AsgiMiddleware(self._app())
        headers = [(b"traceparent", VALID_TRACEPARENT.upper().encode())]
        self._run(mw, self._scope(headers=headers))

        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["trace_id"], "0af7651916cd43dd8448eb211c80319c")

    def test_captures_http_context(self):
        mw = AsgiMiddleware(self._app(status=404))
        headers = [(b"x-forwarded-for", b"203.0.113.7, 10.0.0.1"),
                   (b"user-agent", b"pytest-agent")]
        self._run(mw, self._scope("/orders/7", headers=headers,
                                  query_string=b"page=2"))

        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["http"], {
            "method": "GET",
            "path": "/orders/7?page=2",
            "client_ip": "203.0.113.7",  # first X-Forwarded-For hop
            "remote_ip": "10.0.0.9",     # the socket peer, kept alongside
            "user_agent": "pytest-agent",
            "status_code": 404,
        })

    def test_5xx_marks_failed(self):
        mw = AsgiMiddleware(self._app(status=500))
        self._run(mw, self._scope("/boom"))

        group = self.apm._tx_buffer[("GET /boom", "request")]
        self.assertEqual(group["failed"], 1)
        self.assertEqual(group["success"], 0)

    def test_exception_captured_and_reraised(self):
        async def app(scope, receive, send):
            raise RuntimeError("kaboom")

        mw = AsgiMiddleware(app)
        with self.assertRaises(RuntimeError):
            self._run(mw, self._scope("/explode"))

        group = self.apm._tx_buffer[("GET /explode", "request")]
        self.assertEqual(group["failed"], 1)
        (error,) = self.apm._error_buffer.values()
        self.assertEqual(error["type"], "RuntimeError")
        self.assertEqual(error["transaction_name"], "GET /explode")
        # no http.response.start was sent, so the status is unknown
        tags_key = "method=GET,status=unknown"
        self.assertEqual(self.apm._buffer[("http.requests", tags_key)]["count"], 1)

    def test_starts_event_loop_monitor(self):
        self.assertIsNone(self.apm._loop_monitor)
        mw = AsgiMiddleware(self._app())
        self._run(mw, self._scope())
        self.assertIsNotNone(self.apm._loop_monitor)

    def test_spans_attach_to_the_request_transaction(self):
        async def app(scope, receive, send):
            with apm_mod.span("q", type="db", subtype="postgresql"):
                pass
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        mw = AsgiMiddleware(app)
        self._run(mw, self._scope("/db"))

        group = self.apm._tx_buffer[("GET /db", "request")]
        self.assertEqual(group["spans"][("db", "postgresql")]["count"], 1)

    def test_starlette_falls_back_to_the_raw_path(self):
        # Plain Starlette does NOT publish scope["route"] — it only sets
        # "endpoint" and "path_params" — so there is no route template to
        # read and the raw path stands, ids and all. Documented in the
        # README: name these transactions yourself.
        try:
            import pytest
        except ImportError:
            self.skipTest("pytest is not installed")
        pytest.importorskip("starlette")
        from starlette.applications import Starlette
        from starlette.responses import PlainTextResponse
        from starlette.routing import Route

        async def order(request):
            return PlainTextResponse("ok")

        app = Starlette(routes=[Route("/orders/{order_id}", order)])
        mw = AsgiMiddleware(app)
        self._run(mw, self._scope("/orders/123"))

        self.assertIn(("GET /orders/123", "request"), self.apm._tx_buffer)

    def test_fastapi_route_template(self):
        # FastAPI's APIRoute.matches does child_scope["route"] = self, and
        # APIRoute.path is the template: this is the framework the route
        # naming actually works on.
        try:
            import pytest
        except ImportError:
            self.skipTest("pytest is not installed")
        pytest.importorskip("fastapi")
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/orders/{order_id}")
        async def order(order_id: str):
            return {"ok": order_id}

        mw = AsgiMiddleware(app)
        self._run(mw, self._scope("/orders/123"))

        self.assertIn(("GET /orders/{order_id}", "request"), self.apm._tx_buffer)
        # the raw path still rides the trace sample's http context
        (sample,) = self.apm._trace_samples
        self.assertEqual(sample["http"]["path"], "/orders/123")


class EventLoopLagTest(ApmTestCase):
    def test_watch_outside_a_loop_warns_and_does_not_start(self):
        with self.assertLogs("roottrace_apm", level="WARNING"):
            self.apm.watch_event_loop()
        self.assertIsNone(self.apm._loop_monitor)

    def test_watch_is_idempotent_while_alive(self):
        async def run():
            self.apm.watch_event_loop()
            first = self.apm._loop_monitor
            self.apm.watch_event_loop()
            self.assertIs(self.apm._loop_monitor, first)
            first.cancel()

        asyncio.run(run())

    def test_mean_lag_reported_and_reset_on_flush(self):
        self.apm._loop_lag = (30.0, 3)
        self.apm._gil_lag = (5.0, 2)
        self.apm.flush()

        metrics = {m["name"]: m for m in self.sent[0]["metrics"]}
        self.assertEqual(metrics["python.eventloop.lag_ms"]["value"], 10.0)
        self.assertEqual(metrics["python.eventloop.lag_ms"]["kind"], "gauge")
        self.assertEqual(metrics["process.gil.lag_ms"]["value"], 2.5)
        self.assertEqual(self.apm._loop_lag, (0.0, 0))
        self.assertEqual(self.apm._gil_lag, (0.0, 0))

    def test_no_samples_no_gauges(self):
        apm_mod.counter("jobs").add()
        self.apm.flush()
        names = {m["name"] for m in self.sent[0]["metrics"]}
        self.assertNotIn("python.eventloop.lag_ms", names)
        self.assertNotIn("process.gil.lag_ms", names)


if __name__ == "__main__":
    unittest.main()
