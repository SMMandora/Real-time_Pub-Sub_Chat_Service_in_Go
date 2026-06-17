# Load test

`ws-load.js` is a [k6](https://k6.io) WebSocket load test for the gateway,
targeting the spec's goal: **10k concurrent connections per gateway replica,
p99 fan-out latency < 50ms**.

## Prerequisites

- k6 ≥ v0.46 (the built-in `k6/experimental/websockets` module — no `xk6`
  custom build required).
- A running gateway (and its Redis/Postgres) reachable at `WS_URL`. For 10k
  connections, raise the client's open-file limit (`ulimit -n 65535`).

## Run

```bash
# Full run: ramp to 10k VUs over ~7 minutes.
WS_URL=ws://localhost:8080/ws k6 run loadtest/ws-load.js

# Smaller smoke run:
PEAK=500 HOLD_MS=20000 WS_URL=ws://localhost:8080/ws k6 run loadtest/ws-load.js
```

Env vars: `WS_URL`, `ROOM`, `PEAK` (target VUs), `HOLD_MS` (per-VU connection
hold), `SEND_EVERY_MS`.

## What it measures

- `fanout_latency_ms` — round trip of a VU's own message: send → publish →
  Redis → delivered back to the sender. The `p(99)<50` threshold encodes the SLO;
  k6 exits non-zero if it's breached.
- `ws_errors` — connection/protocol errors (threshold: zero).
- Standard k6 WS metrics (sessions, msgs sent/received).

## Capturing the report

Run the test against the full stack (`docker compose up` or the K8s manifests)
so Prometheus/Grafana are scraping. The report consists of:

1. **k6 summary** — copy the end-of-run table (`fanout_latency_ms` p95/p99,
   `ws_errors`, throughput).
2. **Grafana screenshots** — the "Chat Service" dashboard during the run:
   active connections, messages/sec, fan-out latency p99, queue depth.

Paste both into `loadtest/REPORT.md` (create it after a run). A template:

```markdown
# Load Test Report — <date>

- Gateway replicas: N · peak VUs: 10000 · duration: ~7m
- fanout_latency p99: __ ms (SLO < 50ms): PASS/FAIL
- ws_errors: __
- messages/sec (peak): __

## Grafana
![active connections](...)
![fan-out p99](...)
```
