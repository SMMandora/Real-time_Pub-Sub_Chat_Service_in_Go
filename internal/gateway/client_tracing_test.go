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
