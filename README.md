# roottrace-apm

[![PyPI](https://img.shields.io/pypi/v/roottrace-apm)](https://pypi.org/project/roottrace-apm/)
[![CI](https://github.com/SteveMB1/roottrace-apm/actions/workflows/ci.yml/badge.svg)](https://github.com/SteveMB1/roottrace-apm/actions/workflows/ci.yml)

RootTrace APM agent for Python. Aggregates counters, gauges, timers,
transactions, spans, and errors in memory and posts one payload per flush
interval to the RootTrace API. Zero dependencies, Python 3.9+.

## Install

```
pip install roottrace-apm
```

## Quickstart

```python
import roottrace_apm as apm

apm.init(
    service="checkout-api",
    token="rtc_...",                        # RootTrace collector token
    api_url="https://api.roottrace.io/api", # the default; point at your own deployment
)
```

Every argument can come from an environment variable instead (see
[Configuration](#configuration)). `init()` fails fast with a `ValueError` on
a missing service/token or a non-http(s) `api_url`; everything after a
successful `init()` never raises into your application.

### Metrics

```python
orders = apm.counter("orders.processed")
queue = apm.gauge("queue.depth")
checkout = apm.timer("checkout.duration", tags={"endpoint": "/checkout"})

orders.add()
queue.set(12)

with checkout:
    process_order()

@apm.timed("reports.render.duration")
def render_report():
    ...
```

Metrics flush automatically on a background thread and once more at process
exit. `apm.flush()` forces a synchronous send; `apm.shutdown()` stops the
thread and flushes a final time. The metric name `errors.count` is reserved
for server-side error rollups; recordings under it are dropped with a
warning.

### Transactions and spans

A transaction is one unit of work (a request, a job run); spans time the
operations inside it. `transaction()` works as a context manager or a
decorator, on both sync and async functions:

```python
with apm.transaction("process-order", type="task"):
    with apm.span("SELECT orders", type="db", subtype="postgresql"):
        rows = fetch_order()
    charge_card()  # outbound HTTP inside a transaction becomes an http span

@apm.transaction("nightly-report", type="task")
async def nightly_report():   # async functions are timed across their awaits
    ...
```

Completed transactions aggregate per `(name, type)` with success/failed
outcome counts and a span breakdown by `(type, subtype)`. An exception
escaping the body marks the transaction failed, captures it as an error, and
re-raises it unchanged. Each flush also carries the two slowest transactions
as full trace samples for the dashboard's waterfall view. Spans opened
without an active transaction are a silent no-op.

### Errors

Escaping exceptions are captured automatically; use `capture_exception` for
ones you handle yourself:

```python
try:
    risky()
except ValueError as exc:
    apm.capture_exception(exc)                 # record, transaction outcome unchanged
    apm.capture_exception(exc, handled=False)  # record and mark the transaction failed
```

Errors group by a stable fingerprint (type + culprit + top stack frames),
at most 25 distinct per flush.

### Outbound HTTP (requests, urllib3, urllib)

`init()` instruments the stdlib `http.client` unless you pass
`http_instrumentation=False`. That covers everything built on it —
`requests`, `urllib3` (both 1.x and 2.x), and `urllib` — with no extra
setup. Clients with their own HTTP stacks (`httpx`, `aiohttp`) are not
covered. Every outbound call records a `http.client.duration` timer and a
`http.client.requests` counter tagged
`{"destination": "host:port", "status": "2xx"}`; calls that fail without a
response (connection refused, timeout) are recorded with status `error`.
Inside a transaction the call also becomes an `http` span, and a W3C
`traceparent` header is injected so traces connect across services.

### WSGI

```python
from roottrace_apm import WsgiMiddleware

application = WsgiMiddleware(application)
```

Each request records a `http.request.duration` timer and increments a
`http.requests` counter, both tagged `{"method": "GET", "status": "2xx"}`,
and runs inside a transaction named `"<METHOD> <path>"` with id-like path
segments (numbers, UUIDs, hex ids of 16+ chars) collapsed to `:id` (pass
`name_callback=lambda environ: ...` to name it yourself). An incoming
`traceparent` header is adopted, so distributed requests share one trace id.
5xx responses and raised exceptions mark the transaction failed; exceptions
are captured and re-raised.

## Configuration

Explicit `init()` arguments win over environment variables, which win over
defaults.

| `init()` argument  | Environment variable                                             | Default                       |
| ------------------ | ---------------------------------------------------------------- | ----------------------------- |
| `service`          | `ROOTTRACE_APM_SERVICE`                                          | required                      |
| `token`            | `ROOTTRACE_APM_TOKEN`, falls back to `ROOTTRACE_COLLECTOR_TOKEN` | required                      |
| `api_url`          | `ROOTTRACE_API_URL`                                              | `https://api.roottrace.io/api` |
| `interval_seconds` | `ROOTTRACE_APM_INTERVAL_SECONDS`                                 | 30 (clamped to 5–3600)        |
| `tags`             | —                                                                | none (merged into every metric) |
| `runtime_metrics`  | —                                                                | enabled                       |
| `service_version`  | —                                                                | none (reported when set)      |
| `http_instrumentation` | —                                                            | enabled                       |

The token is the same environment-scoped collector token (`rtc_...`) minted
during collector onboarding.

The instance `init()` returns exposes the resolved endpoint as read-only
properties, for diagnostics and logging:

```python
inst = apm.init(...)
inst.api_url     # e.g. "https://api.roottrace.io/api"
inst.ingest_url  # api_url + "/apm/ingest", the flush target
```

Runtime metrics reported each flush: `process.memory.rss_bytes`,
`process.cpu.percent`, `process.threads`, `process.gc.collections`,
`process.uptime_seconds`.

## Development

```
git clone https://github.com/SteveMB1/roottrace-apm
cd roottrace-apm
python3 -m unittest discover     # tests (no install needed)
python3 -m ruff check .          # lint
```

The package lives under `src/`. Run the examples against the checkout with
`PYTHONPATH=src python3 examples/demo.py`, or `pip install -e .` first.

## License

Apache-2.0. See [LICENSE](LICENSE).
