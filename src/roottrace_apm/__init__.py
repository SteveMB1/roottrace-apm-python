"""RootTrace APM wrapper for Python.

Aggregates counters, gauges, and timers in memory and posts one ingest
payload per flush interval to the RootTrace API. Stdlib only.
"""

from __future__ import annotations

import atexit
import contextvars
import datetime
import functools
import gc
import hashlib
import http.client
import inspect
import json
import logging
import math
import os
import platform
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import resource
except ImportError:  # not available on Windows
    resource = None

VERSION = "0.1.0"
DEFAULT_API_URL = "https://api.roottrace.io/api"
MAX_ENTRIES = 200  # wire cap per ingest request
MAX_USER_ENTRIES = MAX_ENTRIES - 8  # headroom so runtime metrics fit under the cap
MAX_TAG_KEYS = 8
MAX_NAME_LENGTH = 200
MAX_TX_GROUPS = 100  # transaction groups per flush (PROTOCOL.md)
MAX_TX_BREAKDOWN_ROWS = 20  # span-breakdown rows per transaction group
MAX_TX_TYPE_LENGTH = 40
MAX_SUBTYPE_LENGTH = 200
MAX_ERRORS = 25  # distinct error fingerprints per flush
MAX_MESSAGE_LENGTH = 1000
MAX_CULPRIT_LENGTH = 300
MAX_STACK_FRAMES = 50
MAX_ERROR_TYPE_LENGTH = 200
MAX_FRAME_FUNCTION_LENGTH = 300
MAX_FRAME_FILE_LENGTH = 1024
MAX_SERVICE_VERSION_LENGTH = 64
RESERVED_METRIC_NAME = "errors.count"  # the server folds error rollups into this name
MAX_TRACE_SAMPLES = 2  # slowest transactions kept per flush
MAX_SAMPLE_SPANS = 100  # spans kept on one trace sample

logger = logging.getLogger("roottrace_apm")

__all__ = [
    "Apm", "Counter", "Gauge", "Timer", "Span", "WsgiMiddleware",
    "init", "counter", "gauge", "timer", "timed", "transaction", "span",
    "capture_exception", "flush", "shutdown",
    "VERSION",
]

# The active transaction. Contextvars isolate threads and asyncio tasks, so
# spans started while a transaction is open attach to the right one.
_active_transaction = contextvars.ContextVar("roottrace_apm_transaction", default=None)

# Open Timer/Span starts, per context. Tuples, never mutated in place: each
# push/pop set()s a new tuple, so interleaved asyncio tasks on one thread
# can't pop each other's starts (a task's set() only affects its own context).
_timer_starts = contextvars.ContextVar("roottrace_apm_timer_starts", default=())
_span_starts = contextvars.ContextVar("roottrace_apm_span_starts", default=())


def _stack_push(var, entry):
    var.set(var.get() + (entry,))


def _stack_pop(var, owner):
    """Remove and return the newest entry owned by `owner`, else None."""
    stack = var.get()
    for i in range(len(stack) - 1, -1, -1):
        if stack[i][0] is owner:
            var.set(stack[:i] + stack[i + 1:])
            return stack[i]
    return None


def _current_transaction():
    """The active transaction, treating an already-closed one as absent."""
    tx = _active_transaction.get()
    if tx is None or tx.closed:
        return None
    return tx


# Set while _send posts to the API, so the http.client instrumentation
# never measures the wrapper's own flush traffic.
_self_calls = threading.local()

# W3C traceparent: 2-hex version, 32-hex trace id, 16-hex parent id, 2-hex
# flags. Future versions (anything but ff) may append "-suffix" fields.
_TRACEPARENT_RE = re.compile(
    r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}(-.*)?$", re.IGNORECASE
)


def _parse_traceparent(value):
    """Return the trace id from a valid traceparent header, else None."""
    if not isinstance(value, str):
        return None
    match = _TRACEPARENT_RE.match(value.strip())
    if match is None:
        return None
    version, trace_id, parent_id, suffix = match.groups()
    version = version.lower()
    if version == "ff":  # forbidden per W3C
        return None
    if version == "00" and suffix:  # version 00 has exactly four fields
        return None
    trace_id = trace_id.lower()
    if trace_id == "0" * 32 or parent_id.lower() == "0" * 16:  # all-zero ids are invalid
        return None
    return trace_id


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _tags_key(tags):
    # Canonical k=v,k=v form (PROTOCOL.md): percent-encode %, =, and , in
    # keys and values so distinct tag maps never collide. '%' goes first.
    def esc(s):
        return s.replace("%", "%25").replace("=", "%3D").replace(",", "%2C")
    return ",".join(f"{esc(k)}={esc(v)}" for k, v in sorted(tags.items()))


class _HttpStatusError(RuntimeError):
    """HTTP error response from the ingest endpoint, with its status code."""

    def __init__(self, code, detail, retry_after=None):
        super().__init__(f"RootTrace API returned HTTP {code}: {detail}")
        self.code = code
        self.retry_after = retry_after


class _UnserializableError(ValueError):
    """json.dumps refused the payload; retrying it can never succeed."""


class Apm:
    """Aggregation buffer plus the background flush loop."""

    def __init__(self, service, token, api_url=DEFAULT_API_URL, interval_seconds=30,
                 tags=None, runtime_metrics=True, service_version=None):
        self.service = service
        self.token = token
        self._api_url = api_url.rstrip("/")
        self.interval_seconds = interval_seconds
        self.tags = {str(k): str(v) for k, v in (tags or {}).items()}
        if len(self.tags) > MAX_TAG_KEYS:
            # trimmed once here: runtime metrics bypass the per-record trim
            logger.warning("instance tags have %d keys; keeping the first %d in sorted order",
                           len(self.tags), MAX_TAG_KEYS)
            self.tags = {k: self.tags[k] for k in sorted(self.tags)[:MAX_TAG_KEYS]}
        self.runtime_metrics = runtime_metrics
        self.service_version = service_version
        self.hostname = socket.gethostname()
        self._buffer = {}
        self._tx_buffer = {}  # (name, type) -> transaction group
        self._error_buffer = {}  # fingerprint -> error entry
        self._trace_samples = []  # up to MAX_TRACE_SAMPLES, slowest win
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._warned = set()
        self._retry_at = 0.0  # monotonic deadline set by HTTP 429 Retry-After
        self._ssl_context = ssl.create_default_context()
        self._started = time.monotonic()
        t = os.times()
        self._cpu_sample = (t.user + t.system, self._started)
        self._gc_total = sum(s.get("collections", 0) for s in gc.get_stats())

    @property
    def api_url(self):
        """The resolved RootTrace API base URL. Read-only."""
        return self._api_url

    @property
    def ingest_url(self):
        """The full flush target, <api_url>/apm/ingest. Read-only."""
        return self._api_url + "/apm/ingest"

    def _start(self):
        self._thread = threading.Thread(
            target=self._loop, name="roottrace-apm-flush", daemon=True
        )
        self._thread.start()

    def _loop(self):
        while not self._stop.wait(self.interval_seconds):
            try:
                self.flush()
            except Exception:
                # flush handles send errors itself; this keeps the loop alive
                logger.exception("background flush failed")

    def _warn_throttled(self, key, msg, *args):
        # At most one warning per key per flush interval; flush clears the set.
        if key in self._warned:
            return
        self._warned.add(key)
        logger.warning(msg, *args)

    def _record(self, name, kind, tags, unit, value):
        # Guarded end to end: instrumentation must never raise into the app.
        try:
            try:
                value = float(value)
            except (TypeError, ValueError):
                logger.warning("ignoring non-numeric value %r for %s %r", value, kind, name)
                return
            if not math.isfinite(value):
                self._warn_throttled(("non-finite", name),
                                     "ignoring non-finite value %r for %s %r", value, kind, name)
                return
            if not name:
                self._warn_throttled("name-empty",
                                     "ignoring %s recording with empty metric name", kind)
                return
            if not isinstance(name, str):  # a bytes name would poison the whole payload
                name = str(name)
            if unit is not None and not isinstance(unit, str):
                unit = str(unit)
            if name == RESERVED_METRIC_NAME:
                self._warn_throttled("name-reserved",
                                     "metric name %r is reserved for server-side error rollups; "
                                     "dropping", name)
                return
            if len(name) > MAX_NAME_LENGTH:
                self._warn_throttled(("name-length", name[:MAX_NAME_LENGTH]),
                                     "metric name %r longer than %d chars; truncating",
                                     name, MAX_NAME_LENGTH)
                name = name[:MAX_NAME_LENGTH]
            merged = dict(self.tags)
            for k, v in (tags or {}).items():
                merged[str(k)] = str(v)
            if len(merged) > MAX_TAG_KEYS:
                self._warn_throttled(("tag-keys", name),
                                     "metric %r has %d tag keys; keeping the first %d in sorted order",
                                     name, len(merged), MAX_TAG_KEYS)
                merged = {k: merged[k] for k in sorted(merged)[:MAX_TAG_KEYS]}
            key = (name, _tags_key(merged))
            with self._lock:
                entry = self._buffer.get(key)
                if entry is None:
                    if len(self._buffer) >= MAX_USER_ENTRIES:
                        self._warn_throttled(
                            "entry-cap",
                            "metric buffer full at %d entries; dropping new series until next flush",
                            MAX_USER_ENTRIES,
                        )
                        return
                    entry = {"name": name, "kind": kind, "unit": unit, "tags": merged}
                    if kind == "counter":
                        entry.update(count=0, sum=0.0)
                    elif kind == "timer":
                        entry.update(count=0, sum=0.0, min=value, max=value)
                    self._buffer[key] = entry
                if entry["kind"] != kind:
                    logger.warning("metric %r is a %s; ignoring %s recording",
                                   name, entry["kind"], kind)
                    return
                if kind == "counter":
                    entry["count"] += 1
                    entry["sum"] += value
                elif kind == "timer":
                    entry["count"] += 1
                    entry["sum"] += value
                    entry["min"] = min(entry["min"], value)
                    entry["max"] = max(entry["max"], value)
                else:
                    entry["value"] = value
        except Exception:
            logger.exception("failed to record metric %r", name)

    def _record_transaction(self, tx, duration_ms, outcome):
        # Guarded end to end: instrumentation must never raise into the app.
        try:
            started_at = datetime.datetime.fromtimestamp(
                tx.started_at, datetime.timezone.utc
            ).isoformat()
            with self._lock:
                # Trace-sample competition first: the slowest transactions win
                # a slot even when their group is dropped at the cap below.
                samples = self._trace_samples
                if (len(samples) < MAX_TRACE_SAMPLES
                        or duration_ms > min(s["duration_ms"] for s in samples)):
                    if len(samples) >= MAX_TRACE_SAMPLES:
                        samples.remove(min(samples, key=lambda s: s["duration_ms"]))
                    samples.append({
                        "trace_id": tx.trace_id,
                        "transaction_name": tx.name,
                        "transaction_type": tx.type,
                        "duration_ms": duration_ms,
                        "started_at": started_at,
                        "outcome": outcome,
                        "spans_dropped": tx.spans_dropped,
                        "spans": list(tx.spans),
                    })
                key = (tx.name, tx.type)
                group = self._tx_buffer.get(key)
                if group is None:
                    if len(self._tx_buffer) >= MAX_TX_GROUPS:
                        self._warn_throttled(
                            "tx-cap",
                            "transaction buffer full at %d groups; dropping new ones until next flush",
                            MAX_TX_GROUPS,
                        )
                        return
                    group = {"name": tx.name, "type": tx.type,
                             "count": 0, "sum": 0.0,
                             "min": duration_ms, "max": duration_ms,
                             "success": 0, "failed": 0, "spans": {}}
                    self._tx_buffer[key] = group
                group["count"] += 1
                group["sum"] += duration_ms
                group["min"] = min(group["min"], duration_ms)
                group["max"] = max(group["max"], duration_ms)
                group[outcome] += 1
                for span_key, row in tx.breakdown.items():
                    existing = group["spans"].get(span_key)
                    if existing is None:
                        if len(group["spans"]) >= MAX_TX_BREAKDOWN_ROWS:
                            self._warn_throttled(
                                ("tx-breakdown", tx.name),
                                "transaction %r has more than %d span breakdown rows; dropping the rest",
                                tx.name, MAX_TX_BREAKDOWN_ROWS,
                            )
                            continue
                        group["spans"][span_key] = existing = {"count": 0, "sum": 0.0}
                    existing["count"] += row["count"]
                    existing["sum"] += row["sum"]
        except Exception:
            logger.exception("failed to record transaction %r", tx.name)

    def _record_error(self, error):
        # Guarded end to end: error capture must never raise into the app.
        try:
            with self._lock:
                entry = self._error_buffer.get(error["fingerprint"])
                if entry is None:
                    if len(self._error_buffer) >= MAX_ERRORS:
                        self._warn_throttled(
                            "error-cap",
                            "error buffer full at %d distinct errors; dropping new ones until next flush",
                            MAX_ERRORS,
                        )
                        return
                    self._error_buffer[error["fingerprint"]] = error
                else:
                    entry["count"] += error["count"]
        except Exception:
            logger.exception("failed to record error %r", error.get("type"))

    def _read_rss(self):
        if sys.platform.startswith("linux"):
            try:
                with open("/proc/self/status") as status:
                    for line in status:
                        if line.startswith("VmRSS:"):
                            return int(line.split()[1]) * 1024  # value is in kB
            except (OSError, ValueError, IndexError) as exc:
                logger.debug("could not read VmRSS from /proc/self/status: %s", exc)
        if resource is not None:
            # Fallback: ru_maxrss is a lifetime high-water mark, not current RSS.
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != "darwin":  # ru_maxrss is KB on Linux, bytes on macOS
                rss *= 1024
            return rss
        return None

    def _snapshot_record(self, snapshot, name, kind, unit, value):
        # Direct write into the just-drained snapshot, bypassing the entry cap.
        key = (name, _tags_key(self.tags))
        entry = {"name": name, "kind": kind, "unit": unit, "tags": dict(self.tags)}
        if kind == "counter":
            entry.update(count=1, sum=float(value))
            old = snapshot.get(key)
            if old is not None and old["kind"] == "counter":
                entry["count"] += old["count"]
                entry["sum"] += old["sum"]
        else:
            entry["value"] = float(value)
        snapshot[key] = entry

    def _record_runtime(self, snapshot):
        # Written straight into the drained snapshot: a full live buffer can
        # never starve these, and they don't count against the entry cap.
        try:
            rss = self._read_rss()
            if rss is not None:
                self._snapshot_record(snapshot, "process.memory.rss_bytes", "gauge", "bytes", rss)
            wall = time.monotonic()
            t = os.times()
            cpu = t.user + t.system
            last_cpu, last_wall = self._cpu_sample
            if wall > last_wall:
                self._snapshot_record(snapshot, "process.cpu.percent", "gauge", "%",
                                      100.0 * (cpu - last_cpu) / (wall - last_wall))
                self._cpu_sample = (cpu, wall)  # baseline advances only when recorded
            self._snapshot_record(snapshot, "process.threads", "gauge", None,
                                  threading.active_count())
            collections = sum(s.get("collections", 0) for s in gc.get_stats())
            self._snapshot_record(snapshot, "process.gc.collections", "counter", None,
                                  collections - self._gc_total)
            self._gc_total = collections
            self._snapshot_record(snapshot, "process.uptime_seconds", "gauge", "s",
                                  wall - self._started)
        except Exception:
            logger.exception("runtime metrics collection failed")

    def flush(self):
        """Send everything aggregated so far. Synchronous."""
        # One flush at a time: the background loop and public flush() would
        # otherwise interleave and race the cpu/gc baselines.
        with self._flush_lock:
            if time.monotonic() < self._retry_at:
                return
            with self._lock:
                snapshot = self._buffer
                self._buffer = {}
                tx_snapshot = self._tx_buffer
                self._tx_buffer = {}
                err_snapshot = self._error_buffer
                self._error_buffer = {}
                trace_snapshot = self._trace_samples
                self._trace_samples = []
                self._warned.clear()
            if self.runtime_metrics:
                self._record_runtime(snapshot)
            buffers = (snapshot, tx_snapshot, err_snapshot, trace_snapshot)
            if not any(buffers):
                return
            payload = self._build_payload(*buffers)
            try:
                self._send(payload)
            except _UnserializableError as exc:
                logger.warning("dropping %d unserializable metric entries: %s",
                               len(payload["metrics"]), exc)
            except _HttpStatusError as exc:
                self._handle_http_error(buffers, payload, exc)
            except Exception as exc:
                logger.warning("flush of %d metric entries failed: %s",
                               len(payload["metrics"]), exc)
                self._merge_back(*buffers)

    def _handle_http_error(self, buffers, payload, exc):
        if exc.code == 429:
            delay = self.interval_seconds
            if exc.retry_after:
                try:
                    delay = max(int(exc.retry_after), 1)
                except ValueError:
                    logger.warning("unparseable Retry-After %r; retrying in %ds",
                                   exc.retry_after, delay)
            self._retry_at = time.monotonic() + delay
            logger.warning("rate limited (HTTP 429); pausing flushes for %ds", delay)
            self._merge_back(*buffers)
        elif 400 <= exc.code < 500:
            # the API rejected the payload; resending it would fail forever
            logger.warning("dropping %d metric entries rejected by the API: %s",
                           len(payload["metrics"]), exc)
        else:
            logger.warning("flush of %d metric entries failed: %s",
                           len(payload["metrics"]), exc)
            self._merge_back(*buffers)

    def _build_payload(self, snapshot, tx_snapshot, err_snapshot, trace_snapshot):
        metrics = []
        for entry in snapshot.values():
            metric = {"name": entry["name"], "kind": entry["kind"]}
            if entry["unit"]:
                metric["unit"] = entry["unit"]
            if entry["tags"]:
                metric["tags"] = entry["tags"]
            for field in ("count", "sum", "min", "max", "value"):
                if field in entry:
                    metric[field] = entry[field]
            metrics.append(metric)
        payload = {
            "service": self.service,
            "language": "python",
            "hostname": self.hostname,
            "runtime": {
                "language_version": platform.python_version(),
                "pid": os.getpid(),
                "wrapper_version": VERSION,
            },
            "interval_seconds": self.interval_seconds,
            "metrics": metrics,
        }
        if self.service_version:
            payload["service_version"] = self.service_version
        if tx_snapshot:
            transactions = []
            for group in tx_snapshot.values():
                entry = {"name": group["name"], "type": group["type"],
                         "count": group["count"], "sum": group["sum"],
                         "min": group["min"], "max": group["max"],
                         "success": group["success"], "failed": group["failed"]}
                if group["spans"]:
                    rows = []
                    for (span_type, subtype), row in group["spans"].items():
                        span_row = {"type": span_type}
                        if subtype:
                            span_row["subtype"] = subtype
                        span_row["count"] = row["count"]
                        span_row["sum"] = row["sum"]
                        rows.append(span_row)
                    entry["spans"] = rows
                transactions.append(entry)
            payload["transactions"] = transactions
        if err_snapshot:
            payload["errors"] = list(err_snapshot.values())
        if trace_snapshot:
            payload["trace_samples"] = sorted(
                trace_snapshot, key=lambda s: s["duration_ms"], reverse=True
            )
        return payload

    def _merge_back(self, snapshot, tx_snapshot=None, err_snapshot=None,
                    trace_snapshot=None):
        # Unsent aggregates fold into whatever accumulated meanwhile; the live
        # buffer is newer, so gauges keep the live value and the snapshot loses
        # ties for space at the cap. Guarded: flush's failure path must not raise.
        dropped = 0
        try:
            with self._lock:
                for key, old in snapshot.items():
                    entry = self._buffer.get(key)
                    if entry is None:
                        if len(self._buffer) >= MAX_USER_ENTRIES:
                            dropped += 1
                            continue
                        self._buffer[key] = old
                    elif entry["kind"] != old["kind"]:
                        self._warn_throttled(("merge-kind", key),
                                             "metric %r changed kind from %s to %s mid-flush; "
                                             "dropping unsent %s data",
                                             old["name"], old["kind"], entry["kind"], old["kind"])
                    elif old["kind"] in ("counter", "timer"):
                        entry["count"] += old["count"]
                        entry["sum"] += old["sum"]
                        if old["kind"] == "timer":
                            entry["min"] = min(entry["min"], old["min"])
                            entry["max"] = max(entry["max"], old["max"])
                for key, old in (tx_snapshot or {}).items():
                    group = self._tx_buffer.get(key)
                    if group is None:
                        if len(self._tx_buffer) >= MAX_TX_GROUPS:
                            dropped += 1
                            continue
                        self._tx_buffer[key] = old
                        continue
                    group["count"] += old["count"]
                    group["sum"] += old["sum"]
                    group["min"] = min(group["min"], old["min"])
                    group["max"] = max(group["max"], old["max"])
                    group["success"] += old["success"]
                    group["failed"] += old["failed"]
                    for span_key, row in old["spans"].items():
                        existing = group["spans"].get(span_key)
                        if existing is None:
                            if len(group["spans"]) >= MAX_TX_BREAKDOWN_ROWS:
                                continue
                            group["spans"][span_key] = row
                        else:
                            existing["count"] += row["count"]
                            existing["sum"] += row["sum"]
                for fingerprint, old in (err_snapshot or {}).items():
                    entry = self._error_buffer.get(fingerprint)
                    if entry is None:
                        if len(self._error_buffer) >= MAX_ERRORS:
                            dropped += 1
                            continue
                        self._error_buffer[fingerprint] = old
                    else:
                        # the live entry is newer; keep its message and stack
                        entry["count"] += old["count"]
                if trace_snapshot:
                    combined = self._trace_samples + trace_snapshot
                    combined.sort(key=lambda s: s["duration_ms"], reverse=True)
                    self._trace_samples = combined[:MAX_TRACE_SAMPLES]
        except Exception:
            logger.exception("merge-back of unsent entries failed; data lost")
        if dropped:
            logger.warning("dropped %d unsent entries: buffers at their caps", dropped)

    def _send(self, payload):
        # Serialization gets its own try: a ValueError from the request below
        # (e.g. a bad URL) must not masquerade as a poison payload.
        try:
            body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise _UnserializableError(exc) from exc
        request = urllib.request.Request(
            self.ingest_url,
            data=body,
            headers={
                "Authorization": f"Collector {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": f"roottrace-apm-python/{VERSION}",
                "Connection": "close",
            },
            method="POST",
        )
        _self_calls.active = True
        try:
            with urllib.request.urlopen(request, timeout=10, context=self._ssl_context) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise _HttpStatusError(exc.code, detail, exc.headers.get("Retry-After")) from exc
        finally:
            _self_calls.active = False

    def shutdown(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds)
        self.flush()
        if time.monotonic() < self._retry_at:
            # the final flush was a no-op behind the 429 deadline
            with self._lock:
                abandoned = len(self._buffer)
            if abandoned:
                logger.warning(
                    "shutting down while rate limited; abandoning %d unsent metric entries",
                    abandoned)


_instance = None
_init_lock = threading.Lock()
_warned_no_init = False


def init(service=None, token=None, api_url=None, interval_seconds=None,
         tags=None, runtime_metrics=True, service_version=None,
         http_instrumentation=True) -> Apm:
    """Configure the singleton and start the background flush thread."""
    global _instance
    with _init_lock:
        if _instance is not None:
            logger.warning("roottrace_apm.init() called again; returning the existing instance")
            return _instance
        service = service or env("ROOTTRACE_APM_SERVICE")
        token = token or env("ROOTTRACE_APM_TOKEN") or env("ROOTTRACE_COLLECTOR_TOKEN")
        api_url = api_url or env("ROOTTRACE_API_URL", DEFAULT_API_URL)
        if interval_seconds is None:
            raw = env("ROOTTRACE_APM_INTERVAL_SECONDS") or "30"
            try:
                interval_seconds = int(float(raw))
            except (ValueError, OverflowError):  # OverflowError: int(float("inf"))
                logger.warning("invalid ROOTTRACE_APM_INTERVAL_SECONDS %r; using 30", raw)
                interval_seconds = 30
        if not service:
            raise ValueError(
                "RootTrace APM needs a service name: pass service= or set ROOTTRACE_APM_SERVICE"
            )
        if not token:
            raise ValueError(
                "RootTrace APM needs a collector token: pass token= or set "
                "ROOTTRACE_APM_TOKEN (or ROOTTRACE_COLLECTOR_TOKEN)"
            )
        if urllib.parse.urlsplit(api_url).scheme not in ("http", "https"):
            raise ValueError(
                f"RootTrace APM needs an http(s) api_url, got {api_url!r}: "
                "pass api_url= or set ROOTTRACE_API_URL"
            )
        try:
            interval_seconds = int(interval_seconds)
        except (ValueError, OverflowError) as exc:
            raise ValueError(f"invalid interval_seconds {interval_seconds!r}") from exc
        clamped = min(max(interval_seconds, 5), 3600)
        if clamped != interval_seconds:
            logger.warning("interval_seconds %d outside the server's 5-3600 range; using %d",
                           interval_seconds, clamped)
        if service_version is not None:
            service_version = str(service_version)
            if len(service_version) > MAX_SERVICE_VERSION_LENGTH:
                logger.warning("service_version longer than %d chars; truncating",
                               MAX_SERVICE_VERSION_LENGTH)
                service_version = service_version[:MAX_SERVICE_VERSION_LENGTH]
        _instance = Apm(
            service=service,
            token=token,
            api_url=api_url,
            interval_seconds=clamped,
            tags=tags,
            runtime_metrics=runtime_metrics,
            service_version=service_version,
        )
        if http_instrumentation:
            _instrument_http_client()
        _instance._start()
        atexit.register(shutdown)
        return _instance


def _record(name, kind, tags, unit, value):
    global _warned_no_init
    apm = _instance
    if apm is None:
        if not _warned_no_init:
            _warned_no_init = True
            logger.warning("metric recorded before roottrace_apm.init(); dropping")
        return
    apm._record(name, kind, tags, unit, value)


_module_warned = set()  # throttle for warnings raised while no instance exists


def _warn_throttled(key, msg, *args):
    apm = _instance
    if apm is not None:
        apm._warn_throttled(key, msg, *args)
    elif key not in _module_warned:
        _module_warned.add(key)
        logger.warning(msg, *args)


class Counter:
    def __init__(self, name, tags=None, unit=None):
        self.name = name
        self.tags = tags
        self.unit = unit

    def add(self, amount=1):
        _record(self.name, "counter", self.tags, self.unit, amount)


class Gauge:
    def __init__(self, name, tags=None, unit=None):
        self.name = name
        self.tags = tags
        self.unit = unit

    def set(self, value):
        _record(self.name, "gauge", self.tags, self.unit, value)


class Timer:
    def __init__(self, name, tags=None, unit="ms"):
        self.name = name
        self.tags = tags
        self.unit = unit

    def record(self, duration_ms):
        _record(self.name, "timer", self.tags, self.unit, duration_ms)

    def __enter__(self):
        # A per-context stack, so nested `with` blocks on one Timer work and
        # concurrent threads or asyncio tasks never see each other's starts.
        _stack_push(_timer_starts, (self, time.perf_counter()))
        return self

    def __exit__(self, exc_type, exc, tb):
        entry = _stack_pop(_timer_starts, self)
        if entry is not None:
            self.record((time.perf_counter() - entry[1]) * 1000.0)
        return False


def counter(name, tags=None, unit=None) -> Counter:
    return Counter(name, tags, unit)


def gauge(name, tags=None, unit=None) -> Gauge:
    return Gauge(name, tags, unit)


def timer(name, tags=None, unit="ms") -> Timer:
    return Timer(name, tags, unit)


def timed(name, tags=None, unit="ms"):
    """Decorator: record the wrapped call's duration as a timer metric."""
    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                _record(name, "timer", tags, unit, (time.perf_counter() - start) * 1000.0)
        return wrapper
    return decorate


class _Transaction:
    """One in-flight unit of work: timing, spans, and the trace id."""

    def __init__(self, name, type, trace_id):
        self.name = str(name)[:MAX_NAME_LENGTH] or "unnamed"
        self.type = str(type)[:MAX_TX_TYPE_LENGTH] or "request"
        self.trace_id = trace_id
        self.started_at = time.time()
        self.spans = []  # kept for the trace sample, capped
        self.spans_dropped = 0
        self.closed = False  # set once recorded; late spans then no-op
        self.failed = False  # forced by capture_exception(handled=False)
        self.breakdown = {}  # (type, subtype) -> {"count", "sum"}
        self._start = time.perf_counter()
        self._token = None  # contextvar reset token, set by whoever activates us

    def _add_span(self, name, type, subtype, start_offset_ms, duration_ms):
        name = str(name)[:MAX_NAME_LENGTH] or "unnamed"
        type = str(type)[:MAX_TX_TYPE_LENGTH] or "custom"
        subtype = str(subtype)[:MAX_SUBTYPE_LENGTH] if subtype else None
        row = self.breakdown.get((type, subtype))
        if row is None:
            self.breakdown[(type, subtype)] = row = {"count": 0, "sum": 0.0}
        row["count"] += 1
        row["sum"] += duration_ms
        if len(self.spans) >= MAX_SAMPLE_SPANS:
            self.spans_dropped += 1
            return
        span = {"name": name, "type": type}
        if subtype:
            span["subtype"] = subtype
        span["start_offset_ms"] = start_offset_ms
        span["duration_ms"] = duration_ms
        self.spans.append(span)


def _finish_transaction(tx, outcome):
    global _warned_no_init
    tx.closed = True  # late spans (e.g. WSGI streaming) must not attach anymore
    if tx.failed:
        outcome = "failed"
    apm = _instance
    if apm is None:
        if not _warned_no_init:
            _warned_no_init = True
            logger.warning("transaction finished before roottrace_apm.init(); dropping")
        return
    apm._record_transaction(tx, (time.perf_counter() - tx._start) * 1000.0, outcome)


class _TransactionContext:
    """Context manager and decorator wrapping work in a transaction."""

    def __init__(self, name, type, traceparent=None):
        self.name = name
        self.type = type
        self.traceparent = traceparent
        self._tx = None  # the transaction this context opened, nothing else

    def __enter__(self):
        # Guarded: starting a transaction must never raise into the app.
        try:
            trace_id = _parse_traceparent(self.traceparent) or os.urandom(16).hex()
            tx = _Transaction(self.name, self.type, trace_id)
            tx._token = _active_transaction.set(tx)
            self._tx = tx
            return tx
        except Exception:
            logger.exception("failed to start transaction %r", self.name)
            return None

    def __exit__(self, exc_type, exc, tb):
        try:
            # Only close what __enter__ opened: if it failed inside an outer
            # transaction, the outer one must not be recorded early.
            tx, self._tx = self._tx, None
            if tx is None:
                return False
            outcome = "success"
            if exc is not None:
                outcome = "failed"
                if isinstance(exc, Exception):
                    _capture_error(exc, tx.name)
            try:
                _active_transaction.reset(tx._token)
            except ValueError:  # exiting in a different context than we entered
                _active_transaction.set(None)
            _finish_transaction(tx, outcome)
        except Exception:
            logger.exception("failed to finish transaction")
        return False  # an exception from the body re-raises unchanged

    def __call__(self, fn):
        # A fresh context per call: concurrent calls must not share _tx. For
        # coroutine functions the transaction must span the await, not just
        # the (instant) coroutine creation.
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                with _TransactionContext(self.name, self.type, self.traceparent):
                    return await fn(*args, **kwargs)
            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with _TransactionContext(self.name, self.type, self.traceparent):
                return fn(*args, **kwargs)
        return wrapper


def transaction(name, type="request", traceparent=None) -> _TransactionContext:
    """Track a unit of work. Use as a context manager or a decorator.

    Pass a W3C ``traceparent`` header value to adopt an incoming trace id.
    """
    return _TransactionContext(name, type, traceparent)


class Span:
    """Times an operation inside the active transaction; no-op without one."""

    def __init__(self, name, type="custom", subtype=None):
        self.name = name
        self.type = type
        self.subtype = subtype

    def __enter__(self):
        try:
            # A per-context stack, so nested `with` blocks on one Span work
            # and interleaved asyncio tasks never pop each other's starts.
            _stack_push(_span_starts, (self, _current_transaction(), time.perf_counter()))
        except Exception:
            logger.exception("failed to start span %r", self.name)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            entry = _stack_pop(_span_starts, self)
            if entry is not None:
                _, tx, start = entry
                # No active transaction, or it closed while the span was
                # open (WSGI streaming): the span is a no-op.
                if tx is not None and not tx.closed:
                    tx._add_span(self.name, self.type, self.subtype,
                                 (start - tx._start) * 1000.0,
                                 (time.perf_counter() - start) * 1000.0)
        except Exception:
            logger.exception("failed to finish span %r", self.name)
        return False


def span(name, type="custom", subtype=None) -> Span:
    return Span(name, type, subtype)


# Files under these directories are stdlib or third-party, not the app.
_NON_APP_DIRS = (os.path.dirname(os.__file__), os.path.dirname(os.path.abspath(__file__)))


def _is_app_file(path):
    if not path or path.startswith("<"):
        return False
    if "site-packages" in path or "dist-packages" in path:
        return False
    return not path.startswith(_NON_APP_DIRS)


def _build_error(exc):
    frames = []
    tb = exc.__traceback__
    while tb is not None:
        code = tb.tb_frame.f_code
        frames.append({  # clipped to the server schema; fingerprints use these values
            "function": code.co_name[:MAX_FRAME_FUNCTION_LENGTH],
            "file": code.co_filename[:MAX_FRAME_FILE_LENGTH],
            "line": tb.tb_lineno,
            "module": tb.tb_frame.f_globals.get("__name__", ""),
        })
        tb = tb.tb_next
    culprit_frame = None
    for frame in reversed(frames):  # deepest app frame; else deepest frame
        if _is_app_file(frame["file"]):
            culprit_frame = frame
            break
    if culprit_frame is None and frames:
        culprit_frame = frames[-1]
    if culprit_frame is None:
        culprit = "<unknown>"
    elif culprit_frame["module"]:
        culprit = f"{culprit_frame['module']}.{culprit_frame['function']}"
    else:
        culprit = culprit_frame["function"]
    culprit = culprit[:MAX_CULPRIT_LENGTH]
    # keep the innermost frames when the stack is deep; outermost-first order
    frames = frames[-MAX_STACK_FRAMES:]
    stack = [{"function": f["function"], "file": f["file"], "line": f["line"]}
             for f in frames]
    type_name = type(exc).__name__[:MAX_ERROR_TYPE_LENGTH]
    digest = hashlib.sha256()
    digest.update(type_name.encode("utf-8", errors="replace"))
    digest.update(culprit.encode("utf-8", errors="replace"))
    for frame in stack[-5:]:  # the frames closest to the raise identify it
        digest.update(f"{frame['file']}:{frame['function']}".encode("utf-8", errors="replace"))
    return {
        "fingerprint": digest.hexdigest()[:16],
        "type": type_name,
        "message": str(exc)[:MAX_MESSAGE_LENGTH],
        "culprit": culprit,
        "count": 1,
        "stack": stack,
    }


def _capture_error(exc, transaction_name):
    # Guarded end to end: error capture must never raise into the app.
    global _warned_no_init
    try:
        apm = _instance
        if apm is None:
            if not _warned_no_init:
                _warned_no_init = True
                logger.warning("exception captured before roottrace_apm.init(); dropping")
            return
        if not isinstance(exc, BaseException):
            _warn_throttled("capture-not-exception",
                            "capture_exception() got %r, not an exception; ignoring", exc)
            return
        error = _build_error(exc)
        if transaction_name:
            error["transaction_name"] = transaction_name
        apm._record_error(error)
    except Exception:
        logger.exception("failed to capture exception")


def capture_exception(exc, handled=True):
    """Record an exception in the errors buffer. Never raises.

    ``handled=False`` also marks the active transaction failed.
    """
    tx = _current_transaction()
    if not handled and tx is not None:
        tx.failed = True
    _capture_error(exc, tx.name if tx is not None else None)


def flush():
    if _instance is not None:
        _instance.flush()


def shutdown():
    global _instance
    with _init_lock:
        apm = _instance
        if apm is None:
            return
        try:
            apm.shutdown()
        finally:
            _instance = None  # a later init() builds a fresh instance


_http_client_patched = False


def _instrument_http_client():
    """Patch http.client so outbound calls report metrics, spans, and
    propagate traceparent. Idempotent; every path falls back to the
    original methods on any internal error.

    request() alone is not enough: urllib3 2.x overrides it without calling
    the stdlib one, but still funnels through putrequest/endheaders (via
    super()) and getresponse, so those carry the timing and header injection
    for that path. putrequest only starts timing when request() hasn't
    already (request() calls putrequest internally)."""
    global _http_client_patched
    if _http_client_patched:
        return
    original_request = http.client.HTTPConnection.request
    original_putrequest = http.client.HTTPConnection.putrequest
    original_endheaders = http.client.HTTPConnection.endheaders
    original_getresponse = http.client.HTTPConnection.getresponse

    def pop_pending(conn):
        # Cleared unconditionally, no _instance check: pending state must not
        # leak across shutdown/re-init on a keep-alive connection.
        pending = getattr(conn, "_roottrace_apm", None)
        if pending is not None:
            conn._roottrace_apm = None
        return pending

    def finish(conn, pending, status):
        start, method = pending
        duration = (time.perf_counter() - start) * 1000.0
        destination = f"{conn.host}:{conn.port}"
        tags = {"destination": destination, "status": status}
        _record("http.client.duration", "timer", tags, "ms", duration)
        _record("http.client.requests", "counter", tags, None, 1)
        tx = _current_transaction()
        if tx is not None:
            tx._add_span(f"{method} {destination}", "http", destination,
                         (start - tx._start) * 1000.0, duration)

    def fail(conn):
        # Guarded: recording the failure must never mask the original exception.
        try:
            pending = pop_pending(conn)
            if pending is not None and _instance is not None:
                finish(conn, pending, "error")
        except Exception as exc:
            _warn_throttled("http-failure-instrumentation",
                            "outbound HTTP failure instrumentation failed (%r)", exc)

    def request(conn, method, url, body=None, headers=None, **kwargs):
        try:
            if _instance is not None and not getattr(_self_calls, "active", False):
                conn._roottrace_apm = (time.perf_counter(), str(method))
                tx = _current_transaction()
                if tx is not None:
                    headers = dict(headers) if headers else {}
                    if not any(str(k).lower() == "traceparent" for k in headers):
                        headers["traceparent"] = (
                            f"00-{tx.trace_id}-{os.urandom(8).hex()}-01"
                        )
        except Exception as exc:
            _warn_throttled("http-request-instrumentation",
                            "outbound HTTP instrumentation failed (%r); "
                            "sending the request uninstrumented", exc)
        try:
            return original_request(conn, method, url, body, headers or {}, **kwargs)
        except Exception:
            fail(conn)
            raise

    def putrequest(conn, method, url, *args, **kwargs):
        try:
            if (_instance is not None and not getattr(_self_calls, "active", False)
                    and getattr(conn, "_roottrace_apm", None) is None):
                conn._roottrace_apm = (time.perf_counter(), str(method))
        except Exception as exc:
            _warn_throttled("http-putrequest-instrumentation",
                            "outbound HTTP instrumentation failed (%r); "
                            "sending the request uninstrumented", exc)
        try:
            return original_putrequest(conn, method, url, *args, **kwargs)
        except Exception:
            fail(conn)
            raise

    def endheaders(conn, *args, **kwargs):
        try:
            if getattr(conn, "_roottrace_apm", None) is not None:
                tx = _current_transaction()
                # _buffer holds the headers already put; don't inject a second
                # traceparent when the caller (or request() above) set one.
                if tx is not None and not any(
                    line.lower().startswith(b"traceparent:")
                    for line in getattr(conn, "_buffer", ())
                ):
                    conn.putheader("traceparent",
                                   f"00-{tx.trace_id}-{os.urandom(8).hex()}-01")
        except Exception as exc:
            _warn_throttled("http-endheaders-instrumentation",
                            "outbound HTTP instrumentation failed (%r); "
                            "sending the request uninstrumented", exc)
        try:
            return original_endheaders(conn, *args, **kwargs)
        except Exception:
            fail(conn)  # connect errors (refused, DNS) surface here
            raise

    def getresponse(conn, *args, **kwargs):
        try:
            response = original_getresponse(conn, *args, **kwargs)
        except Exception:
            fail(conn)  # timeouts waiting on the response surface here
            raise
        pending = pop_pending(conn)
        try:
            if pending is not None and _instance is not None:
                status = response.status
                bucket = (f"{status // 100}xx"
                          if isinstance(status, int) and 100 <= status <= 599
                          else "unknown")
                finish(conn, pending, bucket)
        except Exception as exc:
            _warn_throttled("http-response-instrumentation",
                            "outbound HTTP instrumentation failed (%r); "
                            "returning the response uninstrumented", exc)
        return response

    http.client.HTTPConnection.request = request
    http.client.HTTPConnection.putrequest = putrequest
    http.client.HTTPConnection.endheaders = endheaders
    http.client.HTTPConnection.getresponse = getresponse
    _http_client_patched = True


# Numeric, UUID-shaped, or long-hex (>=16 chars: Mongo ObjectIds, hashes)
# path segments become ":id" in transaction names.
_ID_SEGMENT_RE = re.compile(
    r"^(?:\d+|[0-9a-fA-F]{16,}"
    r"|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$"
)


def _normalize_path(path):
    if not path:
        return "/"
    return "/".join(
        ":id" if _ID_SEGMENT_RE.match(segment) else segment
        for segment in path.split("/")
    ) or "/"


class WsgiMiddleware:
    """Wraps each request in a transaction and records the http.request.duration
    timer and http.requests counter, tagged by method and status class."""

    def __init__(self, app, name="http.request.duration", name_callback=None):
        self.app = app
        self.name = name
        self.name_callback = name_callback

    def _start_transaction(self, environ):
        # Guarded: the middleware must serve the request even if APM breaks.
        try:
            name = None
            if self.name_callback is not None:
                try:
                    name = self.name_callback(environ)
                except Exception:
                    _warn_throttled("wsgi-name-callback",
                                    "name_callback failed; using the default transaction name")
            if name is None:
                name = (f"{environ.get('REQUEST_METHOD', 'GET')} "
                        f"{_normalize_path(environ.get('PATH_INFO', ''))}")
            trace_id = (_parse_traceparent(environ.get("HTTP_TRACEPARENT"))
                        or os.urandom(16).hex())
            tx = _Transaction(name, "request", trace_id)
            tx._token = _active_transaction.set(tx)
            return tx
        except Exception:
            logger.exception("failed to start request transaction")
            return None

    def _finish_request(self, tx, outcome, exc):
        if tx is None:
            return
        try:
            if exc is not None:
                _capture_error(exc, tx.name)
            try:
                _active_transaction.reset(tx._token)
            except ValueError:  # streaming finished in a different context
                _active_transaction.set(None)
            _finish_transaction(tx, outcome)
        except Exception:
            logger.exception("failed to finish request transaction")

    def __call__(self, environ, start_response):
        tx = self._start_transaction(environ)
        start = time.perf_counter()
        captured = {}

        def capture(status, headers, exc_info=None):
            captured["status"] = status
            return start_response(status, headers, exc_info)

        try:
            result = self.app(environ, capture)
        except Exception as exc:
            self._finish_request(tx, "failed", exc)
            raise

        def stream(result):
            error = None
            try:
                yield from result
            except Exception as exc:
                error = exc
                raise
            finally:
                if hasattr(result, "close"):
                    result.close()
                status = captured.get("status", "")
                bucket = f"{status[:1]}xx" if status[:1].isdigit() else "unknown"
                tags = {"method": environ.get("REQUEST_METHOD", "GET"), "status": bucket}
                _record(self.name, "timer", tags, "ms",
                        (time.perf_counter() - start) * 1000.0)
                _record("http.requests", "counter", tags, None, 1)
                outcome = "failed" if (error is not None or bucket == "5xx") else "success"
                self._finish_request(tx, outcome, error)

        return stream(result)
