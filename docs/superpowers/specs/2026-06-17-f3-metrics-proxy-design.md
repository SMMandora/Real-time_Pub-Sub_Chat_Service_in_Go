# Design — F3: Prometheus Query Proxy

**Status:** Approved
**Date:** 2026-06-17
**Part of:** Frontend program (slice F3 of F1–F5)

## Context

The admin dashboard (F5) needs metric time-series from Prometheus. Calling
Prometheus directly from the SPA means CORS + exposing Prometheus. F3 adds a
same-origin proxy on the gateway.

## Goals

- `GET /api/metrics/query` and `GET /api/metrics/query_range` forward to
  Prometheus's `/api/v1/query` and `/api/v1/query_range` and return the JSON.

## Non-goals

- Auth on the proxy (the demo is open); arbitrary upstreams (only the two
  Prometheus query endpoints at the configured host).

## Components

### `internal/gateway/server.go`

- `ServerConfig` gains `PrometheusURL string`; `Server` stores it as `promURL`.
- Routes `GET /api/metrics/query` and `GET /api/metrics/query_range`, each
  calling `proxyPrometheus(w, r, "/api/v1/query"[ _range])`.
- `proxyPrometheus`: if `promURL == ""` → 503 ("metrics not configured"); else
  forward `r.URL.RawQuery` to `{promURL}{path}` with a 10s timeout, copy the
  upstream status + body, set `Content-Type: application/json`. Upstream errors
  → 502. The path is fixed (only `/api/v1/query[_range]`); only the query string
  (PromQL + time params) is user-supplied, so this is not an open proxy.

### `cmd/gateway/main.go`

- Read `PROMETHEUS_URL` (default `http://localhost:9090`); set
  `ServerConfig.PrometheusURL`.

## Error handling

- `promURL` unset → 503. Upstream unreachable → 502. Otherwise pass through
  Prometheus's status/body verbatim.

## Testing

- **gateway (httptest):** stand up a fake Prometheus (`httptest`) returning a
  canned `{"status":"success",…}`; point `PrometheusURL` at it; `GET
  /api/metrics/query?query=up` returns 200 with that body and the upstream sees
  path `/api/v1/query`. With `PrometheusURL` empty → 503.
- `CGO_ENABLED=0 go test ./...` passes.

## Acceptance criteria

- The two proxy endpoints forward to Prometheus and return its JSON; unset
  config → 503; the query string is passed through.
- `CGO_ENABLED=0 go test ./...` passes.
