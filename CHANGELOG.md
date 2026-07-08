# Changelog

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
