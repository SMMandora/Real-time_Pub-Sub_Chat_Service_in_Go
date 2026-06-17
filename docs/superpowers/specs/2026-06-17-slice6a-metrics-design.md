# Slice 6a Design — Prometheus Metrics + Grafana Dashboard

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 6a of 7)

## Context

Slices 1-5 deliver the full chat feature set. Slice 6 is observability; it
splits into **6a metrics + dashboard** (this spec) and **6b correlation IDs +
tracing**. 6a adds Prometheus metrics to the gateway and worker, a `/metrics`
endpoint on each, a Grafana dashboard, and Prometheus/Grafana wiring in
compose.

### Roadmap

1-5. In-memory → fan-out → persistence/history → presence/typing → auth/rate-limit *(done)*
6. **Slice 6a — Metrics + dashboard** *(this spec)* → 6b correlation IDs + OTel/Jaeger tracing
7. K8s deploy + load test + public demo

## Goals

- Expose Prometheus metrics for active connections, messages, fan-out latency,
  and persistence (batch size, queue depth) on every service.
- Commit a Grafana dashboard and the Prometheus scrape config; wire both into
  docker-compose.

## Non-goals (deferred)

- Correlation IDs across services and OpenTelemetry/Jaeger tracing (slice 6b).
- K8s, load test, public demo (slice 7).

## Decisions

- **Library:** `github.com/prometheus/client_golang`.
- **Fan-out latency** is measured as `now − frame.TS` (TS stamped at SEND),
  observed only on live deliveries via the bus (`onBusMessage`), so replayed
  history messages do not pollute the histogram.
- **Wiring style: package-global metrics via `promauto`**, one metrics file per
  package. No metrics object is threaded through constructors. Because each
  binary imports only its own package, each `/metrics` exposes only that
  service's metrics (gateway metrics in the gateway binary, persistence metrics
  in the worker binary).

## Architecture

```
gateway  :8080/metrics  ← chat_active_connections, chat_messages_total, chat_fanout_latency_seconds
worker   :8090/metrics  ← chat_messages_persisted_total, chat_persist_batch_size, chat_persist_queue_depth
Prometheus scrapes both → Grafana dashboard
```

Instrumentation is a handful of `.Inc()`/`.Observe()`/`.Set()` calls at existing
hot spots; metrics never affect request handling.

## Components

### `internal/gateway/metrics.go` (new)

```go
var (
	ActiveConnections    = promauto.NewGauge(...)     // chat_active_connections
	MessagesTotal        = promauto.NewCounter(...)   // chat_messages_total
	FanoutLatencySeconds = promauto.NewHistogram(...) // chat_fanout_latency_seconds
)
```
`FanoutLatencySeconds` uses buckets up to ~1s (e.g. `.001,.005,.01,.025,.05,.1,.25,.5,1`).

### `internal/persistence/metrics.go` (new)

```go
var (
	MessagesPersisted = promauto.NewCounter(...)   // chat_messages_persisted_total
	BatchSize         = promauto.NewHistogram(...) // chat_persist_batch_size
	QueueDepth        = promauto.NewGauge(...)      // chat_persist_queue_depth
)
```
`BatchSize` buckets `1,5,10,25,50,100`.

### Instrumentation (small edits)

- `internal/gateway/server.go`:
  - `handleWS`: `ActiveConnections.Inc()` after registering the client;
    `ActiveConnections.Dec()` in the disconnect defer.
  - `Router()`: `r.Handle("/metrics", promhttp.Handler())`.
- `internal/gateway/hub.go`:
  - `Publish`: `MessagesTotal.Inc()` on a successful publish.
  - `onBusMessage`: for a `message` frame with `f.TS > 0`,
    `FanoutLatencySeconds.Observe(float64(nowMillis()-f.TS) / 1000)`.
- `internal/persistence/batcher.go`:
  - On a successful flush: `MessagesPersisted.Add(float64(len(msgs)))` and
    `BatchSize.Observe(float64(len(msgs)))`.
  - In `Run`, when receiving from the input channel: `QueueDepth.Set(float64(len(b.in)))`.
- `cmd/worker/main.go`: `mux.Handle("/metrics", promhttp.Handler())`.
- `cmd/gateway/main.go`: no change (metrics register on import; `/metrics` is on
  the router).

### Deploy artifacts

- `deploy/prometheus/prometheus.yml` — scrape `gateway:8080` and `worker:8090`
  on `/metrics`.
- `deploy/grafana/dashboard.json` — panels: active connections, messages/sec
  (`rate(chat_messages_total[1m])`), fan-out p99
  (`histogram_quantile(0.99, sum(rate(chat_fanout_latency_seconds_bucket[5m])) by (le))`),
  queue depth, messages persisted.
- `docker-compose.yml` — add `prometheus` (mounting the scrape config) and
  `grafana` services.

## Metric names

`chat_active_connections`, `chat_messages_total`, `chat_fanout_latency_seconds`,
`chat_messages_persisted_total`, `chat_persist_batch_size`,
`chat_persist_queue_depth`.

## Error handling

- Metric observation is fire-and-forget and never affects handling.
- `len(channel)` reads for queue depth are safe (return the buffered count).

## Testing

- **gateway:** `GET /metrics` returns 200 and the body contains
  `chat_active_connections` (httptest); `MessagesTotal` increments after a
  `hub.Publish` (prometheus `testutil` delta, loopback fakeBus).
- **persistence:** `MessagesPersisted` and `BatchSize` increase after a batcher
  flush (`testutil` delta, fake store).
- Grafana rendering and live Prometheus scraping cannot be verified headlessly
  (Docker daemon off); the committed JSON/YAML and compose services are the
  deliverables and come up under `docker compose up` when Docker is running.
- All concurrency tests run under `-race`.

## Acceptance criteria

- The gateway serves `/metrics` exposing `chat_active_connections`,
  `chat_messages_total`, and `chat_fanout_latency_seconds`; the worker serves
  `/metrics` exposing the persistence metrics.
- `chat_messages_total` increments on publish; `chat_fanout_latency_seconds`
  records only live deliveries; `chat_messages_persisted_total` /
  `chat_persist_batch_size` update on flush.
- `deploy/prometheus/prometheus.yml` and `deploy/grafana/dashboard.json` exist
  and are valid; `docker compose config` parses with the new services.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
