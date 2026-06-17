# Design — F5: Admin/Observability Dashboard (frontend)

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Frontend program (slice F5 of F1–F5)

## Context

The reference includes an Admin/Observability dashboard (metric cards, trend
charts, a health table). F5 adds it to the SPA, querying the gateway's Prometheus
proxy (F3) so it works same-origin.

## Goals

- An in-app admin view with live metric cards, trend line charts, and a
  service-health table, polling Prometheus via `/api/metrics/query[_range]`.

## Data source

- `GET /api/metrics/query?query=<promql>` (instant) and
  `GET /api/metrics/query_range?query=&start=&end=&step=` (range) — the F3 proxy.
- PromQL (the metrics built in slice 6a):
  - Active connections: `sum(chat_active_connections)`
  - Messages/sec: `sum(rate(chat_messages_total[1m]))`
  - Fan-out p99 (ms): `histogram_quantile(0.99, sum(rate(chat_fanout_latency_seconds_bucket[5m])) by (le)) * 1000`
  - Queue depth: `sum(chat_persist_queue_depth)`
  - Persisted/sec: `sum(rate(chat_messages_persisted_total[1m]))`
  - Gateway replicas: `count(chat_active_connections)`
  - Service health: `up` (vector; one series per scraped target with a `job` label)

## Components (frontend, `frontend/src/admin/`)

- `metricsApi.ts` — `queryInstant(promql) → number`, `queryRange(promql, rangeSec,
  stepSec) → {t,v}[]`, `queryVector(promql) → {labels,value}[]` (for health),
  parsing Prometheus's `vector`/`matrix` result shapes.
- `AdminDashboard.tsx` — polls (~5s) the six metric cards (instant), three trend
  charts (range over the last ~15m), and the health table (`up`). Uses
  `recharts` for the line charts.
- `MetricCard.tsx` — label + big number (rounded; ms/s suffix as apt).
- `TrendChart.tsx` — a small responsive `recharts` line chart from a `{t,v}[]`.
- `HealthTable.tsx` — rows from `up`: job + Healthy/Down badge.

Navigation: `App` gains a view toggle (Chat ↔ Admin) — a link in the sidebar/
header switches the main pane to `AdminDashboard`. Dark theme + blue accent,
matching the reference admin screen.

## Dependency

- `recharts` (React + TS charts). Add to `frontend`.

## Error handling

- A failed/empty Prometheus query → the card shows `—` / the chart shows empty;
  never crash. (When Prometheus isn't running, the proxy returns 503 → the view
  shows placeholders.)

## Testing

- **Vitest:** `metricsApi` parses an instant vector (returns the value), a range
  matrix (returns points), and an empty result (returns 0/[]), with a mock
  `fetch`. A `MetricCard` render test (label + formatted value). Charts are not
  unit-tested in jsdom (recharts needs layout) — a smoke render of
  `AdminDashboard` with mocked api is optional.
- **Build gate:** `npm run typecheck` + `npm test` + `npm run build`.

## Acceptance criteria

- The admin view renders metric cards, trend charts, and a health table from the
  Prometheus proxy; polls live; degrades to placeholders when Prometheus is
  down.
- `npm run typecheck`, `npm test`, `npm run build` pass; the build is committed
  into `web/`.
