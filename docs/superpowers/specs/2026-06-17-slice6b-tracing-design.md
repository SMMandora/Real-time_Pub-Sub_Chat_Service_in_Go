# Slice 6b Design — Distributed Tracing + Correlation IDs

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Real-time Pub/Sub Chat Service (slice 6b of 7)

## Context

Slice 6a added Prometheus metrics. Slice 6b finishes observability: OpenTelemetry
tracing across the gateway and worker (visible in Jaeger) plus correlation IDs in
logs. The two unify — the trace ID is the correlation ID flowing across services.

### Roadmap

1-5. Feature set *(done)*
6a. Metrics + Grafana *(done)*
6b. **Tracing + correlation IDs** *(this spec)*
7. K8s deploy + load test + public demo

## Goals

- Trace one message's journey SEND (gateway) → Redis → persist (worker) as a
  single distributed trace, viewable in Jaeger.
- Log the trace ID in both services so their logs correlate.

## Non-goals (deferred / out of scope)

- Metric-trace exemplars, log shipping, sampling tuning (always-on for the demo).
- K8s, load test, public demo (slice 7).

## Decisions

- **Propagation:** W3C `traceparent` carried in a new optional `trace` field on
  the message frame; the gateway injects it on publish, the worker and the
  gateway's own fan-out extract it.
- **Export:** OTLP/HTTP to a Jaeger all-in-one container (Jaeger ingests OTLP
  natively). Endpoint from the standard `OTEL_EXPORTER_OTLP_ENDPOINT`; when
  unset, a **no-op tracer** is used (zero overhead).
- **Spans:** three, linked via the propagated `traceparent` — `chat.send`
  (gateway), `chat.fanout` (gateway delivery), `chat.consume` (worker).
- **Sampling:** always-on (no head/tail sampling) for the demo.
- **Correlation = trace ID:** both services log `trace_id` at their span points.

## Architecture

```
SEND ─chat.send (gateway)─→ inject traceparent into frame ─→ Redis
                                         │
        ┌────────────────────────────────┴───────────────┐
   gateway onBusMessage                              worker handle
   chat.fanout (child)                               chat.consume (child)
```

Tracing is configured by a small `internal/tracing` package; instrumentation is
a handful of span starts plus inject/extract at the message boundaries.

## Components

### `internal/tracing/tracing.go` (new)

```go
// Init configures the global tracer provider and W3C propagator. When
// OTEL_EXPORTER_OTLP_ENDPOINT is unset it installs a no-op provider. Returns a
// shutdown function.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error)

// Tracer returns the package tracer (otel.Tracer("chat")).
func Tracer() trace.Tracer

// Inject returns the W3C traceparent for the span in ctx (empty if none).
func Inject(ctx context.Context) string

// Extract returns a context carrying the remote span referenced by traceparent.
func Extract(ctx context.Context, traceparent string) context.Context
```

`Init` (endpoint set): build an `otlptracehttp` exporter, an
`sdktrace.TracerProvider` with a `service.name` resource and a batch span
processor, set it global, and set `propagation.TraceContext{}` as the global
propagator. (endpoint unset): set only the propagator and return a no-op
shutdown — the global provider stays the default no-op.

`Inject`/`Extract` use the global propagator over a `propagation.MapCarrier`
keyed by `traceparent`.

### `internal/gateway/protocol.go` (modify)

Add `Trace string \`json:"trace,omitempty"\`` to `Frame`.

### `internal/gateway/client.go` (modify)

In the SEND case of `handleFrame`:
- `ctx, span := tracing.Tracer().Start(c.ctx, "chat.send")` then `defer span.End()`.
- build the message frame and set `f.Trace = tracing.Inject(ctx)`.
- `c.log.Info("message sent", "user", c.username, "room", room, "trace_id", span.SpanContext().TraceID().String())`.
- publish via `c.hub.Publish`.

### `internal/gateway/hub.go` (modify)

In `onBusMessage`, for a `message` frame: `ctx := tracing.Extract(context.Background(), f.Trace)`; `_, span := tracing.Tracer().Start(ctx, "chat.fanout")`; `span.End()` after `deliverLocal`. (The existing fan-out latency metric stays.)

### `internal/persistence/worker.go` (modify)

`inbound` gains `Trace string \`json:"trace"\``. In `handle`, after decoding a
`message`: `ctx := tracing.Extract(context.Background(), in.Trace)`; `_, span :=
tracing.Tracer().Start(ctx, "chat.consume")`; log `trace_id`; `span.End()`;
submit to the batcher.

### `cmd/gateway/main.go` + `cmd/worker/main.go` (modify)

Call `tracing.Init(ctx, "gateway")` / `"worker"` at startup; defer the returned
shutdown.

### `docker-compose.yml` (modify)

Add `jaegertracing/all-in-one` (UI `16686`, OTLP gRPC `4317`, OTLP HTTP `4318`).

### README

Document `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g. `http://localhost:4318`) and the
Jaeger UI at `http://localhost:16686`.

## Error handling

- Tracing never affects message handling: no-op fallback when unconfigured;
  exporter errors are handled by the batch processor (async), not on the hot
  path.
- An empty or invalid `trace` field → `Extract` yields a fresh context and the
  consumer starts a new root span (no crash).
- Trace IDs are visible to clients in the frame `trace` field (accepted).

## Testing (in-memory exporter, no Jaeger needed)

- **`tracing_test.go`:** with a real test provider, `Inject` then `Extract`
  round-trips a span context (same trace ID).
- **gateway:** with a test tracer provider installed globally, a SEND produces a
  `chat.send` span and the published frame's `Trace` field is non-empty (fake
  registry + `tracetest.NewInMemoryExporter`).
- **worker:** `handle` of a message carrying a `trace` value produces a
  `chat.consume` span whose trace ID equals the injected one (proves
  cross-service linkage).
- All concurrency tests run under `-race`.

## Acceptance criteria

- A SEND creates a `chat.send` span and stamps a `traceparent` into the
  published frame; the gateway fan-out and worker consume spans link to that
  trace (verified with an in-memory exporter).
- With `OTEL_EXPORTER_OTLP_ENDPOINT` unset, tracing is a no-op and nothing
  breaks; both binaries still build and run.
- Both services log `trace_id` at their span points.
- `docker compose config` parses with the Jaeger service.
- `go test ./... -race` passes (Postgres integration tests skip without Docker).
