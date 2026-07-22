# Changelog

## 0.3.2 - 2026-07-22

- Flush-failure warnings now name the endpoint they were sending to
  (`flush of N metric entries to <url> failed: ...`), so a DNS or
  connectivity failure points at the host that needs fixing instead of
  leaving the target implicit.

## 0.3.1 - 2026-07-21

- `RootTraceLogHandler` now includes the formatted traceback in the shipped
  message when a record carries `exc_info` — so `logging.exception(...)`
  ships what it prints, inside the same 8KB message cap. Previously the
  traceback was silently dropped and an error log arrived as its first line
  only.

## 0.3.0 - 2026-07-15

- Latency histograms: every timer metric and transaction group now carries
  an optional `buckets` object — a log2 histogram (4 buckets per octave,
  index `min(127, max(0, floor(log2(max(d, 0.001)) * 4) + 40))`, so 1ms is
  bucket 40 and 1000ms is bucket 79) of the durations since the last flush,
  so the dashboard can compute real percentiles instead of inferring them
  from an average. Buckets merge back with everything else on a failed
  flush. The field is additive and optional: servers that ignore it read
  the payload exactly as before.
- `AsgiMiddleware`: ASGI 3 middleware for FastAPI/Starlette with the same
  metrics, transactions, `traceparent` adoption, request context, and error
  capture as the WSGI middleware. Transactions are named from the resolved
  route template (`GET /orders/{order_id}`) when the framework publishes
  one, falling back to the raw path. Lifespan and websocket scopes pass
  through untouched.
- `python.eventloop.lag_ms` gauge: mean asyncio scheduling drift since the
  last flush, sampled by a 500ms monitor coroutine. Started automatically
  by `AsgiMiddleware`, or manually with `apm.watch_event_loop()`.
- `process.gil.lag_ms` gauge: mean oversleep of a 100ms daemon sampler
  thread. This measures thread scheduling delay, of which GIL contention is
  the dominant cause in CPython — not a direct GIL instrumentation. Rides
  `runtime_metrics`.
- `RootTraceLogHandler`: a stdlib `logging.Handler` batching records
  (service, level, message, logger, timestamp, active `trace_id`, and
  redacted record extras) and POSTing them to `<api_url>/logs/ingest` on
  the existing flush cadence, from the existing background thread. Caps at
  500 buffered entries (drop-oldest, counted warning) and 8KB messages;
  `emit()` never raises. The resolved endpoint is readable as
  `inst.logs_url`.
- `ROOTTRACE_APM_SERVICE_VERSION` environment fallback for
  `service_version`, so deploy pipelines can report versions (deploy
  markers, regression detection) without code changes.

## 0.2.0 - 2026-07-11

- Automatic database spans (`db_instrumentation=True`, the default):
  MongoDB via a pymongo command listener (motor supported through executor
  context propagation — call `init()` before creating the client),
  Elasticsearch `perform_request`, redis-py (sync and asyncio), asyncpg,
  and SQLAlchemy engine events. Spans are named by operation and target
  (`find app.users`, `SELECT users`, `GET`), never by payload.
- Outbound `aiohttp` instrumentation: the same `http.client.duration`/
  `http.client.requests` metrics, `http` spans, and `traceparent`
  propagation as the stdlib and httpx hooks.
- Database clients that ride an instrumented HTTP transport (async
  Elasticsearch over aiohttp) suppress the inner HTTP span so one query is
  one waterfall row; HTTP metrics still record.
- Kubernetes context reporting: `deployment`/`namespace` init arguments with
  `ROOTTRACE_APM_DEPLOYMENT`/`ROOTTRACE_APM_NAMESPACE` fallbacks, plus
  in-cluster auto-detection from the pod name and the mounted serviceaccount
  namespace.
- Outbound `httpx` instrumentation (sync and async clients), applied when
  httpx is installed: the same `http.client.duration`/`http.client.requests`
  metrics, `http` spans, and `traceparent` propagation as the stdlib hook.
- Request context on trace samples: the WSGI middleware records the origin
  IP (first `X-Forwarded-For` hop, socket peer as `remote_ip`), user agent,
  method, path with query string, and status code of sampled requests, and
  `Transaction.set_http()` lets custom instrumentation attach the same.

## 0.1.0

Initial release.

- Counters, gauges, and timers with tags, aggregated in-process and
  flushed to the RootTrace API on an interval.
- Transactions and spans with per-span-type breakdown metrics.
- Error capture with fingerprinting and stack traces.
- Sampled slow-transaction traces with span waterfalls.
- Outbound HTTP monitoring via stdlib `http.client` instrumentation
  (covers `requests`/`urllib3`/`urllib`), with W3C `traceparent`
  propagation.
- WSGI middleware for inbound request transactions.
- Automatic process runtime metrics (RSS, CPU, GC, threads, uptime).
- Zero dependencies; Python 3.9+.
