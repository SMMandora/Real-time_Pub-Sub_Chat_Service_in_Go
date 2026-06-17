# Slice 6b — Distributed Tracing + Correlation IDs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Trace a message SEND (gateway) → Redis → persist (worker) as one OpenTelemetry trace exported to Jaeger, with the trace ID logged in both services.

**Architecture:** A small `internal/tracing` package configures the OTel SDK (OTLP/HTTP → Jaeger when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else no-op). The gateway starts a `chat.send` span and injects W3C `traceparent` into a new `trace` field on the published frame; the gateway fan-out (`chat.fanout`) and worker (`chat.consume`) extract it, linking three spans into one trace.

**Tech Stack:** Go 1.22+, `go.opentelemetry.io/otel` (+ sdk, otlptracehttp), existing stack; Jaeger all-in-one in compose.

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
| `internal/tracing/tracing.go` | **new** | `Init`/`Tracer`/`Inject`/`Extract` |
| `internal/tracing/tracing_test.go` | **new** | inject↔extract round-trip |
| `internal/gateway/protocol.go` | modify | `Frame.Trace` field |
| `internal/gateway/client.go` | modify | `chat.send` span + inject + trace_id log |
| `internal/gateway/hub.go` | modify | `chat.fanout` span in onBusMessage |
| `internal/gateway/client_tracing_test.go` | **new** | SEND span + frame.Trace stamped |
| `internal/persistence/worker.go` | modify | `inbound.Trace`; `chat.consume` span + log |
| `internal/persistence/worker_tracing_test.go` | **new** | consume span linked to injected trace |
| `cmd/gateway/main.go` | modify | `tracing.Init("gateway")` |
| `cmd/worker/main.go` | modify | `tracing.Init("worker")` |
| `docker-compose.yml` | modify | add Jaeger |
| `README.md` | modify | tracing docs |

---

### Task 1: Add OpenTelemetry dependencies

- [ ] **Step 1: Add otel modules**

Run:
```bash
go get go.opentelemetry.io/otel@v1.32.0
go get go.opentelemetry.io/otel/sdk@v1.32.0
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp@v1.32.0
```
Expected: added to `go.mod` (with transitive otel deps). Patch/minor drift is fine if the build passes. Do NOT run `go mod tidy` yet.

- [ ] **Step 2: Verify build**

Run:
```bash
go build ./...
```
Expected: clean, exit 0.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git -c commit.gpgsign=false commit -m "Add OpenTelemetry tracing dependencies"
```

---

### Task 2: Tracing package

**Files:**
- Create: `internal/tracing/tracing.go`, `internal/tracing/tracing_test.go`

- [ ] **Step 1: Write `tracing_test.go`**

Create `internal/tracing/tracing_test.go`:
```go
package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectExtractRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(sdktrace.NewTracerProvider())

	ctx, span := Tracer().Start(context.Background(), "test")
	defer span.End()

	tp := Inject(ctx)
	if tp == "" {
		t.Fatal("expected a non-empty traceparent")
	}

	got := Extract(context.Background(), tp)
	sc := trace.SpanContextFromContext(got)
	if sc.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("trace id mismatch: %s vs %s", sc.TraceID(), span.SpanContext().TraceID())
	}
}

func TestExtractEmptyIsNoop(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	ctx := Extract(context.Background(), "")
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("expected no span context from empty traceparent")
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/tracing/ 2>&1 | head -10
```
Expected: FAIL to compile — `undefined: Tracer` / `Inject` / `Extract`.

- [ ] **Step 3: Implement `tracing.go`**

Create `internal/tracing/tracing.go`:
```go
package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Init configures the global W3C propagator and tracer provider. When
// OTEL_EXPORTER_OTLP_ENDPOINT is unset it installs only the propagator and
// leaves the default no-op provider, returning a no-op shutdown. The OTLP HTTP
// exporter reads the endpoint from the standard env var.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the package tracer.
func Tracer() trace.Tracer { return otel.Tracer("chat") }

// Inject returns the W3C traceparent for the span in ctx, or "" if none.
func Inject(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Extract returns a context carrying the remote span named by traceparent.
func Extract(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
```

- [ ] **Step 4: Run tests (green)**

Run:
```bash
go build ./... && go vet ./...
go test ./internal/tracing/ -race -v
```
Expected: PASS (2 tests), no race warnings.

- [ ] **Step 5: Commit**

```bash
git add internal/tracing/tracing.go internal/tracing/tracing_test.go
git -c commit.gpgsign=false commit -m "Add tracing package with OTLP setup and propagation"
```

---

### Task 3: Gateway tracing

**Files:**
- Create: `internal/gateway/client_tracing_test.go`
- Modify: `internal/gateway/protocol.go`, `internal/gateway/client.go`, `internal/gateway/hub.go`, `cmd/gateway/main.go`

- [ ] **Step 1: Write `client_tracing_test.go`**

Create `internal/gateway/client_tracing_test.go`:
```go
package gateway

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSendStartsSpanAndStampsTrace(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	exp := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp)))

	reg := &fakeRegistry{}
	c := newRateClient(reg, &fakeRateLimiter{allow: true})
	c.handleFrame(Frame{Type: TypeJoin, Room: "x"})
	c.handleFrame(Frame{Type: TypeSend, Room: "x", Text: "hi"})

	if len(reg.published) != 1 || reg.published[0].Trace == "" {
		t.Fatalf("expected one published message with non-empty Trace, got %+v", reg.published)
	}
	found := false
	for _, s := range exp.GetSpans() {
		if s.Name == "chat.send" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a chat.send span")
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/gateway/ -run TestSendStartsSpan 2>&1 | head -10
```
Expected: FAIL — `reg.published[0].Trace undefined` (Frame has no Trace field yet).

- [ ] **Step 3: Add `Trace` to `Frame`**

In `internal/gateway/protocol.go`, add to the `Frame` struct (after `Members`):
```go
	Trace string `json:"trace,omitempty"`
```

- [ ] **Step 4: Instrument `client.go`**

Add the tracing import to `internal/gateway/client.go`:
```go
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
```
Replace the `TypeSend` case's publish block:
```go
		if !c.allowSend() {
			c.enqueue(errorFrame("rate limit exceeded"))
			return
		}
		if err := c.hub.Publish(f.Room, messageFrame(f.Room, c.username, f.Text, nowMillis())); err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
```
with:
```go
		if !c.allowSend() {
			c.enqueue(errorFrame("rate limit exceeded"))
			return
		}
		ctx, span := tracing.Tracer().Start(c.ctx, "chat.send")
		msg := messageFrame(f.Room, c.username, f.Text, nowMillis())
		msg.Trace = tracing.Inject(ctx)
		c.log.Info("message sent", "user", c.username, "room", f.Room,
			"trace_id", span.SpanContext().TraceID().String())
		err := c.hub.Publish(f.Room, msg)
		span.End()
		if err != nil {
			c.enqueue(errorFrame("failed to send message"))
		}
```

- [ ] **Step 5: Instrument `hub.go`**

Add the tracing import to `internal/gateway/hub.go`:
```go
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
```
Replace `onBusMessage` with:
```go
func (h *Hub) onBusMessage(channel string, payload []byte) {
	f, err := decodeFrame(payload)
	if err != nil {
		return
	}
	if f.Type == TypeMessage && f.TS > 0 {
		FanoutLatencySeconds.Observe(float64(nowMillis()-f.TS) / 1000)
	}
	room := roomFromChannel(channel)
	if f.Type == TypeMessage {
		ctx := tracing.Extract(context.Background(), f.Trace)
		_, span := tracing.Tracer().Start(ctx, "chat.fanout")
		h.deliverLocal(room, f)
		span.End()
		return
	}
	h.deliverLocal(room, f)
}
```

- [ ] **Step 6: Init tracing in `cmd/gateway/main.go`**

Add the tracing import to `cmd/gateway/main.go`:
```go
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
```
After the `log := slog.New(...)` line, add:
```go
	shutdownTracing, err := tracing.Init(context.Background(), "gateway")
	if err != nil {
		log.Error("tracing init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracing(sctx)
		scancel()
	}()
```
(Note: `main` already declares `err` later via `pool, err := ...`; the `err` above is the first declaration, so the later `pool, err :=` still works because `pool` is new. If the compiler complains about `err` redeclaration, change the later one's `:=`/`=` as needed — but `pool, err :=` declaring a new `pool` is valid.)

- [ ] **Step 7: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/gateway/ -race
```
Expected: build+vet clean; `ok`, no race warnings (existing tests + the new tracing test pass).

- [ ] **Step 8: Commit**

```bash
git add internal/gateway/ cmd/gateway/main.go
git -c commit.gpgsign=false commit -m "Trace gateway send and fan-out, propagate via frame"
```

---

### Task 4: Worker tracing

**Files:**
- Create: `internal/persistence/worker_tracing_test.go`
- Modify: `internal/persistence/worker.go`, `cmd/worker/main.go`

- [ ] **Step 1: Write `worker_tracing_test.go`**

Create `internal/persistence/worker_tracing_test.go`:
```go
package persistence

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
)

func TestHandleStartsConsumeSpanLinkedToTrace(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	exp := tracetest.NewInMemoryExporter()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp)))

	pctx, parent := tracing.Tracer().Start(context.Background(), "parent")
	traceparent := tracing.Inject(pctx)
	parent.End()
	wantTrace := parent.SpanContext().TraceID().String()

	store := &fakeStore{}
	b := NewBatcher(store, 1, time.Hour, testLogger())
	go b.Run()
	defer b.Close()
	w := NewWorker(nil, b, testLogger())

	w.handle(`{"type":"message","room":"x","id":1,"from":"u","text":"hi","ts":1,"trace":"` + traceparent + `"}`)

	var got string
	for _, s := range exp.GetSpans() {
		if s.Name == "chat.consume" {
			got = s.SpanContext.TraceID().String()
		}
	}
	if got == "" {
		t.Fatal("expected a chat.consume span")
	}
	if got != wantTrace {
		t.Fatalf("consume span trace %s != parent %s", got, wantTrace)
	}
}
```

- [ ] **Step 2: Run to verify red**

Run:
```bash
go test ./internal/persistence/ -run TestHandleStartsConsumeSpan 2>&1 | head -10
```
Expected: FAIL — the produced spans contain no `chat.consume` (worker not instrumented yet), so the test fails its assertion (or compiles and fails). If it instead passes trivially, ensure the worker change in Step 3 is what creates the span.

- [ ] **Step 3: Instrument `worker.go`**

In `internal/persistence/worker.go`, add the tracing import:
```go
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
```
Add a `Trace` field to `inbound`:
```go
	Trace string `json:"trace"`
```
Replace `handle` with:
```go
func (w *Worker) handle(payload string) {
	var in inbound
	if err := json.Unmarshal([]byte(payload), &in); err != nil {
		w.log.Debug("skipping malformed payload", "err", err)
		return
	}
	if in.Type != "message" {
		return
	}
	ctx := tracing.Extract(context.Background(), in.Trace)
	_, span := tracing.Tracer().Start(ctx, "chat.consume")
	w.log.Info("message consumed", "room", in.Room, "id", in.ID,
		"trace_id", span.SpanContext().TraceID().String())
	span.End()

	w.batcher.Submit(Message{
		RoomID:    in.Room,
		ID:        in.ID,
		Sender:    in.From,
		Body:      in.Text,
		CreatedMS: in.TS,
	})
}
```

- [ ] **Step 4: Init tracing in `cmd/worker/main.go`**

Add the tracing import to `cmd/worker/main.go`:
```go
	"github.com/SMMandora/Real-time_Pub-Sub_Chat_Service_in_Go/internal/tracing"
```
After the `log := slog.New(...)` line, add:
```go
	shutdownTracing, err := tracing.Init(context.Background(), "worker")
	if err != nil {
		log.Error("tracing init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = shutdownTracing(sctx)
		scancel()
	}()
```
(If the compiler reports `err` redeclared, the later `pool, err :=` still validly declares the new `pool`; no change needed. If `no new variables on left side of :=` appears anywhere, switch that specific `:=` to `=`.)

- [ ] **Step 5: Run tests (green) with race**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go build ./... && go vet ./...
go test ./internal/persistence/ -race
```
Expected: build+vet clean; `ok`, no race warnings.

- [ ] **Step 6: Commit**

```bash
git add internal/persistence/worker.go internal/persistence/worker_tracing_test.go cmd/worker/main.go
git -c commit.gpgsign=false commit -m "Trace worker consume, link to propagated trace"
```

---

### Task 5: Jaeger in compose + README + verification

**Files:**
- Modify: `docker-compose.yml`, `README.md`

- [ ] **Step 1: Add Jaeger to compose**

In `docker-compose.yml`, add this service (keep all existing services):
```yaml
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"
      - "4317:4317"
      - "4318:4318"
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
```

- [ ] **Step 2: Update the README**

In `README.md`, in the `## Observability` section, append a tracing paragraph after the existing metrics/Grafana text:
```markdown
Distributed tracing is OpenTelemetry → Jaeger. Set
`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` when running the gateway and
worker; spans (`chat.send` → `chat.fanout` → `chat.consume`) appear in the Jaeger
UI at http://localhost:16686. Without that env var, tracing is a no-op. Each
service also logs `trace_id` for log correlation.
```
Update the `## Roadmap` line to:
```markdown
Slice 1 (done) → Redis fan-out (done) → Postgres persistence (done) →
history + replay (done) → presence + typing (done) → auth + rate limiting (done) →
metrics + tracing (done) → K8s + load test.
```

- [ ] **Step 3: Validate compose**

Run:
```bash
docker compose config >/dev/null && echo "compose ok"
```
Expected: `compose ok`.

- [ ] **Step 4: Final verification**

Run:
```bash
export PATH="$PATH:/c/Users/shubh/AppData/Local/Microsoft/WinGet/Packages/BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe/mingw64/bin"
export CGO_ENABLED=1
go mod tidy
go build ./... && go vet ./... && go test ./... -race -count=2
```
Expected: builds clean, vet clean, all tests `ok` (PG tests skip), no race warnings.

- [ ] **Step 5: Confirm tree clean and commit**

Run:
```bash
git status --porcelain
```
Expected: empty after the commit (ignore/delete any local `*.exe`).

```bash
git add docker-compose.yml README.md go.mod go.sum
git -c commit.gpgsign=false commit -m "Add Jaeger to compose and document tracing"
```

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `tracing` package with OTLP/no-op + propagation (Task 2), `Frame.Trace` + `chat.send` inject (Task 3 client), `chat.fanout` extract (Task 3 hub), `chat.consume` extract + linkage (Task 4 worker), trace_id logged in both services (Tasks 3,4), `Init` in both mains (Tasks 3,4), Jaeger in compose + docs (Task 5), in-memory-exporter tests (Tasks 2,3,4). Out-of-scope (exemplars, sampling tuning) absent.
- **Signature consistency:** `tracing.Init(ctx, name) (func(context.Context) error, error)`, `Tracer()`, `Inject(ctx) string`, `Extract(ctx, traceparent) context.Context`. `Frame.Trace` and worker `inbound.Trace` use the same json key `trace`.
- **No-op safety:** with no provider/endpoint, `Tracer().Start` returns a non-recording span, `Inject` returns "", `Extract("")` returns the input ctx — nothing breaks.
- **Global provider in tests:** the tracing tests set the global otel provider; package tests run sequentially (no `t.Parallel`), so there is no concurrent write to the global. The gateway/worker tests that don't assert spans are unaffected by whichever provider is current.
- **`err` in main:** the new `shutdownTracing, err := tracing.Init(...)` is the first `err` declaration; later `pool, err :=` still validly declares the new `pool`. Watch for a `no new variables` error and switch a `:=` to `=` only if the compiler demands it.
