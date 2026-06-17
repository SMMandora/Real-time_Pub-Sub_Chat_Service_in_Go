# Slice 6a — Prometheus Metrics + Grafana Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Prometheus metrics on the gateway and worker, serve `/metrics` on each, and commit a Grafana dashboard + Prometheus scrape config wired into docker-compose.

**Architecture:** Package-global metrics via `promauto` (one metrics file per package, registered to the default registry). A few `.Inc()`/`.Observe()`/`.Set()` calls at existing hot spots. `/metrics` via `promhttp.Handler()`. Prometheus + Grafana added to compose with committed config + dashboard.

**Tech Stack:** Go 1.22+, `github.com/prometheus/client_golang`, existing chi/go-redis/pgx; Prometheus + Grafana images in compose.

**Commit convention:** Commit locally on `main`. Do NOT push. No Claude/Anthropic attribution. Use `git -c commit.gpgsign=false commit`.

**-race needs a C compiler on this machine.** Prefix race-test Bash commands with:
```
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
```

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `internal/gateway/metrics.go` | **new** | gateway metric globals + `metricsHandler()` |
| `internal/gateway/metrics_test.go` | **new** | `/metrics` serves; publish increments counter |
| `internal/gateway/server.go` | modify | `/metrics` route; active-connection gauge |
| `internal/gateway/hub.go` | modify | messages counter; fan-out latency histogram |
| `internal/persistence/metrics.go` | **new** | persistence metric globals |
| `internal/persistence/metrics_test.go` | **new** | flush updates metrics |
| `internal/persistence/batcher.go` | modify | persisted/batch-size/queue-depth |
| `cmd/worker/main.go` | modify | `/metrics` on the health mux |
| `deploy/prometheus/prometheus.yml` | **new** | scrape gateway + worker |
| `deploy/grafana/provisioning/datasources/prometheus.yml` | **new** | Prometheus datasource |
| `deploy/grafana/provisioning/dashboards/dashboards.yml` | **new** | dashboard provider |
| `deploy/grafana/dashboards/chat.json` | **new** | the dashboard |
| `docker-compose.yml` | modify | add prometheus + grafana |
| `README.md` | modify | metrics docs |

---

### Task 1: Add dependency

- [ ] **Step 1: Add prometheus client**

Run:
```bash
go get github.com/prometheus/client_golang@v1.20.5
```
Expected: added to `go.mod`. (Patch drift fine.) Do NOT run `go mod tidy` yet.

- [ ] **Step 2: Verify build**

Run:
```bash
go build ./...
```
Expected: clean, exit 0.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git -c commit.gpgsign=false commit -m "Add prometheus client_golang dependency"
```

---

### Task 2: Gateway metrics

**Files:**
- Create: `internal/gateway/metrics.go`, `internal/gateway/metrics_test.go`
- Modify: `internal/gateway/server.go`, `internal/gateway/hub.go`

- [ ] **Step 1: Write `metrics_test.go`**

Create `internal/gateway/metrics_test.go`:
```go
package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsEndpointServes(t *testing.T) {
	srv := newTestServer()
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "chat_active_connections") {
		t.Fatalf("expected chat_active_connections in /metrics output")
	}
}

func TestPublishIncrementsMessagesTotal(t *testing.T) {
	bus := newFakeBus()
	h := NewHub(bus)
	h.Join("x", &fakeMember{id: "a"})

	before := testutil.ToFloat64(MessagesTotal)
	if err := h.Publish("x", messageFrame("x", "alice", "hi", nowMillis())); err != nil {
		t.Fatal(err)
	}
	if after := testutil.ToFloat64(MessagesTotal); after != before+1 {
		t.Fatalf("MessagesTotal: before %v after %v, want +1", before, after)
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run "TestMetricsEndpoint|TestPublishIncrements" 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: MessagesTotal`.

- [ ] **Step 3: Create `metrics.go`**

Create `internal/gateway/metrics.go`:
```go
package gateway

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ActiveConnections is the number of currently connected WebSocket clients.
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chat_active_connections",
		Help: "Currently connected WebSocket clients.",
	})
	// MessagesTotal counts chat messages published to the bus.
	MessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_messages_total",
		Help: "Chat messages published to the bus.",
	})
	// FanoutLatencySeconds measures send-to-local-delivery latency.
	FanoutLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "chat_fanout_latency_seconds",
		Help:    "Latency from message send to local delivery.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
	})
)

// metricsHandler serves the default Prometheus registry.
func metricsHandler() http.Handler { return promhttp.Handler() }
```

- [ ] **Step 4: Instrument `server.go`**

In `internal/gateway/server.go`, add the metrics route to `Router()` (after the `/api/rooms/...` line):
```go
	r.Handle("/metrics", metricsHandler())
```
In `handleWS`, after `s.hub.Register(client)`, add:
```go
	ActiveConnections.Inc()
```
In the disconnect `defer func() { ... }()`, add `ActiveConnections.Dec()` as the first statement inside the defer (before `client.leaveAll()`):
```go
	defer func() {
		ActiveConnections.Dec()
		client.leaveAll()
```

- [ ] **Step 5: Instrument `hub.go`**

In `internal/gateway/hub.go`, in `Publish`, replace the final line:
```go
	return h.bus.Publish(context.Background(), roomChannel(roomID), payload)
```
with:
```go
	if err := h.bus.Publish(context.Background(), roomChannel(roomID), payload); err != nil {
		return err
	}
	MessagesTotal.Inc()
	return nil
```
In `onBusMessage`, after the decode-error check and before `h.deliverLocal(...)`, add the fan-out latency observation:
```go
	if f.Type == TypeMessage && f.TS > 0 {
		FanoutLatencySeconds.Observe(float64(nowMillis()-f.TS) / 1000)
	}
```

- [ ] **Step 6: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/gateway/ -race
```
Expected: build+vet clean; `ok`, no race warnings (existing tests + the 2 metrics tests pass).

- [ ] **Step 7: Commit**

```bash
git add internal/gateway/metrics.go internal/gateway/metrics_test.go internal/gateway/server.go internal/gateway/hub.go
git -c commit.gpgsign=false commit -m "Add gateway Prometheus metrics and /metrics endpoint"
```

---

### Task 3: Worker metrics

**Files:**
- Create: `internal/persistence/metrics.go`, `internal/persistence/metrics_test.go`
- Modify: `internal/persistence/batcher.go`, `cmd/worker/main.go`

- [ ] **Step 1: Write `metrics_test.go`**

Create `internal/persistence/metrics_test.go`:
```go
package persistence

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestFlushUpdatesPersistenceMetrics(t *testing.T) {
	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()

	before := testutil.ToFloat64(MessagesPersisted)
	b.Submit(Message{RoomID: "x", ID: 1, Body: "hi"})

	waitUntil(t, time.Second, func() bool {
		return testutil.ToFloat64(MessagesPersisted) >= before+1
	})
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run TestFlushUpdatesPersistenceMetrics 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: MessagesPersisted`.

- [ ] **Step 3: Create `metrics.go`**

Create `internal/persistence/metrics.go`:
```go
package persistence

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesPersisted counts messages written to Postgres.
	MessagesPersisted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "chat_messages_persisted_total",
		Help: "Messages written to Postgres.",
	})
	// BatchSize observes the number of rows per persistence batch.
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "chat_persist_batch_size",
		Help:    "Rows per persistence batch.",
		Buckets: []float64{1, 5, 10, 25, 50, 100},
	})
	// QueueDepth is the number of messages waiting in the batcher input channel.
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chat_persist_queue_depth",
		Help: "Pending messages in the batcher input channel.",
	})
)
```

- [ ] **Step 4: Instrument `batcher.go`**

In `internal/persistence/batcher.go`, in `Run`, change the main-loop receive case:
```go
		case m := <-b.in:
			batch = append(batch, m)
			if len(batch) >= b.maxSize {
				flush()
			}
```
to:
```go
		case m := <-b.in:
			QueueDepth.Set(float64(len(b.in)))
			batch = append(batch, m)
			if len(batch) >= b.maxSize {
				flush()
			}
```
Replace `writeBatch` with a version that records metrics on success:
```go
// writeBatch persists a copy of the batch, retrying once before dropping.
func (b *Batcher) writeBatch(batch []Message) {
	msgs := make([]Message, len(batch))
	copy(msgs, batch)
	if err := b.tryInsert(msgs); err != nil {
		if err2 := b.tryInsert(msgs); err2 != nil {
			b.log.Warn("dropping batch after retry", "count", len(msgs), "err", err2)
			return
		}
	}
	MessagesPersisted.Add(float64(len(msgs)))
	BatchSize.Observe(float64(len(msgs)))
}
```

- [ ] **Step 5: Serve `/metrics` on the worker**

In `cmd/worker/main.go`, add `"github.com/prometheus/client_golang/prometheus/promhttp"` to the imports, and add a metrics route to the mux (after the `/readyz` handler registration):
```go
	mux.Handle("/metrics", promhttp.Handler())
```

- [ ] **Step 6: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/persistence/ -race
```
Expected: build+vet clean; `ok`, no race warnings.

- [ ] **Step 7: Commit**

```bash
git add internal/persistence/metrics.go internal/persistence/metrics_test.go internal/persistence/batcher.go cmd/worker/main.go
git -c commit.gpgsign=false commit -m "Add worker Prometheus metrics and /metrics endpoint"
```

---

### Task 4: Deploy artifacts + README + verification

**Files:**
- Create: `deploy/prometheus/prometheus.yml`, `deploy/grafana/provisioning/datasources/prometheus.yml`, `deploy/grafana/provisioning/dashboards/dashboards.yml`, `deploy/grafana/dashboards/chat.json`
- Modify: `docker-compose.yml`, `README.md`

- [ ] **Step 1: Prometheus scrape config**

Create `deploy/prometheus/prometheus.yml`:
```yaml
global:
  scrape_interval: 5s
scrape_configs:
  - job_name: gateway
    static_configs:
      - targets: ["host.docker.internal:8080"]
  - job_name: worker
    static_configs:
      - targets: ["host.docker.internal:8090"]
```
(Targets use `host.docker.internal` because the gateway/worker run on the host via `go run`; containerizing them is slice 7.)

- [ ] **Step 2: Grafana datasource provisioning**

Create `deploy/grafana/provisioning/datasources/prometheus.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 3: Grafana dashboard provider**

Create `deploy/grafana/provisioning/dashboards/dashboards.yml`:
```yaml
apiVersion: 1
providers:
  - name: default
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 4: The dashboard**

Create `deploy/grafana/dashboards/chat.json`:
```json
{
  "title": "Chat Service",
  "uid": "chat-service",
  "schemaVersion": 39,
  "version": 1,
  "time": { "from": "now-15m", "to": "now" },
  "refresh": "5s",
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Active connections",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "datasource": { "type": "prometheus", "uid": "prometheus" }, "expr": "sum(chat_active_connections)" }
      ]
    },
    {
      "id": 2,
      "type": "timeseries",
      "title": "Messages / sec",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "datasource": { "type": "prometheus", "uid": "prometheus" }, "expr": "sum(rate(chat_messages_total[1m]))" }
      ]
    },
    {
      "id": 3,
      "type": "timeseries",
      "title": "Fan-out latency p99 (s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "datasource": { "type": "prometheus", "uid": "prometheus" }, "expr": "histogram_quantile(0.99, sum(rate(chat_fanout_latency_seconds_bucket[5m])) by (le))" }
      ]
    },
    {
      "id": 4,
      "type": "timeseries",
      "title": "Persist queue depth",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "datasource": { "type": "prometheus", "uid": "prometheus" }, "expr": "sum(chat_persist_queue_depth)" }
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Messages persisted / sec",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "datasource": { "type": "prometheus", "uid": "prometheus" }, "expr": "sum(rate(chat_messages_persisted_total[1m]))" }
      ]
    }
  ]
}
```

- [ ] **Step 5: Add Prometheus + Grafana to compose**

Replace the entire contents of `docker-compose.yml` with:
```yaml
services:
  redis:
    image: redis:7
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "no"]
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: chat
      POSTGRES_USER: chat
      POSTGRES_PASSWORD: chat
    ports:
      - "5432:5432"
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    extra_hosts:
      - "host.docker.internal:host-gateway"
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/grafana/dashboards:/var/lib/grafana/dashboards:ro
```

- [ ] **Step 6: Validate compose YAML and JSON**

Run:
```bash
docker compose config >/dev/null && echo "compose ok"
python -c "import json,sys; json.load(open('deploy/grafana/dashboards/chat.json')); print('json ok')" 2>/dev/null || node -e "require('./deploy/grafana/dashboards/chat.json'); console.log('json ok')"
```
Expected: `compose ok` and `json ok`. (If neither python nor node is available, instead run `go run` a tiny check or confirm the JSON parses via any available tool; report what you used.)

- [ ] **Step 7: Update the README**

In `README.md`, add a new `## Observability` section right before `## Test`:
```markdown
## Observability

Each service exposes Prometheus metrics at `/metrics`:

- gateway (`:8080/metrics`): `chat_active_connections`, `chat_messages_total`,
  `chat_fanout_latency_seconds`.
- worker (`:8090/metrics`): `chat_messages_persisted_total`,
  `chat_persist_batch_size`, `chat_persist_queue_depth`.

`docker compose up -d` also starts Prometheus (`:9090`, scraping the host-run
gateway/worker) and Grafana (`:3000`, anonymous admin) with the "Chat Service"
dashboard provisioned from `deploy/grafana/dashboards/chat.json`.
```
Update the `## Roadmap` line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence + typing (done) → auth + rate limiting (done) →
metrics (done) → tracing → K8s + load test.
```

- [ ] **Step 8: Final verification**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go mod tidy
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok` (PG tests skip), no race warnings.

- [ ] **Step 9: Confirm tree clean and commit**

Run:
```bash
git status --porcelain
```
Expected: empty after the commit below (ignore/delete any local `*.exe`).

```bash
git add deploy/ docker-compose.yml README.md go.mod go.sum
git -c commit.gpgsign=false commit -m "Add Prometheus + Grafana deploy artifacts and metrics docs"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** gateway metrics (Task 2: active connections, messages, fan-out latency), worker metrics (Task 3: persisted, batch size, queue depth), `/metrics` on both (Tasks 2,3), Prometheus scrape + Grafana dashboard + compose services (Task 4), metric tests (Tasks 2,3). Out-of-scope (correlation IDs, tracing) absent — those are slice 6b.
- **Metric names:** `chat_active_connections`, `chat_messages_total`, `chat_fanout_latency_seconds`, `chat_messages_persisted_total`, `chat_persist_batch_size`, `chat_persist_queue_depth` — consistent across code, dashboard, and README.
- **Global registry isolation:** gateway metrics live in package `gateway`, persistence metrics in package `persistence`; `go test ./...` runs a separate binary per package, so there is no double-registration. Each binary only imports its own package, so each `/metrics` exposes only its service's metrics.
- **Fan-out latency** is observed only in `onBusMessage` for live `message` frames with `TS > 0`; replayed history (enqueued directly in `Client.replay`) is not counted.
- **No constructor churn:** metrics are package globals, so `NewHub`/`NewServer`/`NewBatcher` signatures are unchanged.
