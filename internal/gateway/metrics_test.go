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
