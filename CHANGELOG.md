# Changelog

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
