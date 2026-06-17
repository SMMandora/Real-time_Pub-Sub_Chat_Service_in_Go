# F3 — Prometheus Query Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Checkbox steps.

**Goal:** Same-origin gateway proxy for Prometheus `/api/v1/query[_range]`.

**Architecture:** `ServerConfig.PrometheusURL` + two routes that forward the query string to Prometheus and copy back the response.

**Commit on `main`, no push, no attribution.** Tests run with `CGO_ENABLED=0 go test ./...` (race detector's C compiler unavailable). Disk error → `go clean -cache -testcache`.

---

### Task 1: Metrics proxy

**Files:** modify `internal/gateway/server.go`, `internal/gateway/server_test.go`, `cmd/gateway/main.go`.

- [ ] **Step 1: Test** — append to `internal/gateway/server_test.go`:
```go
func TestMetricsQueryProxiesToPrometheus(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("upstream path = %s, want /api/v1/query", r.URL.Path)
		}
		if r.URL.Query().Get("query") != "up" {
			t.Errorf("query not forwarded: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[]}}`))
	}))
	defer prom.Close()

	bus := newFakeBus()
	srv := NewServer(ServerConfig{
		Hub: NewHub(bus), Bus: bus, History: &fakeHistory{}, Presence: newFakePresenceStore(),
		Limiter: &fakeRateLimiter{allow: true}, Rooms: newFakeRoomStore(), Members: newFakeMemberStore(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), WebDir: "web", PrometheusURL: prom.URL,
	})

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/metrics/query?query=up", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("proxy = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "success") {
		t.Fatalf("expected upstream body, got %s", rec.Body.String())
	}
}

func TestMetricsQueryUnconfigured503(t *testing.T) {
	srv := newTestServer() // no PrometheusURL
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/metrics/query?query=up", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured proxy = %d, want 503", rec.Code)
	}
}
```

- [ ] **Step 2: Run red** — `go test ./internal/gateway/ -run TestMetricsQuery 2>&1 | head` → `unknown field PrometheusURL` / route 404.

- [ ] **Step 3: Implement (`server.go`)** — add `"io"` and `"strings"` to the imports. Add `PrometheusURL string` to `ServerConfig`; add `promURL string` to `Server`; set `promURL: cfg.PrometheusURL` in `NewServer`. In `Router()`, add (after the members route):
```go
	r.Get("/api/metrics/query", s.handleMetricsQuery)
	r.Get("/api/metrics/query_range", s.handleMetricsQueryRange)
```
Add the handlers (near `handleRoomMembers`):
```go
func (s *Server) handleMetricsQuery(w http.ResponseWriter, r *http.Request) {
	s.proxyPrometheus(w, r, "/api/v1/query")
}

func (s *Server) handleMetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	s.proxyPrometheus(w, r, "/api/v1/query_range")
}

func (s *Server) proxyPrometheus(w http.ResponseWriter, r *http.Request, path string) {
	if s.promURL == "" {
		http.Error(w, "metrics not configured", http.StatusServiceUnavailable)
		return
	}
	target := strings.TrimRight(s.promURL, "/") + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		http.Error(w, "bad query", http.StatusBadRequest)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "prometheus unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
```

- [ ] **Step 4: main (`cmd/gateway/main.go`)** — add a `promURL := getenv("PROMETHEUS_URL", "http://localhost:9090")` line near the other `getenv` calls, and set `PrometheusURL: promURL` in the `gateway.NewServer(ServerConfig{...})` call.

- [ ] **Step 5: Build/vet/test** — `go build ./... && go vet ./...`; `CGO_ENABLED=0 go test ./...` → all green.

- [ ] **Step 6: Commit** — `git add internal/gateway/ cmd/gateway/main.go && git -c commit.gpgsign=false commit -m "Add Prometheus query proxy endpoints"`.

---

## Self-Review

- Spec coverage: two proxy routes (Step 3), config + default (Step 4), 503-when-unset + pass-through (Step 3 + tests Step 1). Path fixed, only query string forwarded.
- Consistency: `ServerConfig.PrometheusURL` → `Server.promURL`; `getenv` already exists in main.
