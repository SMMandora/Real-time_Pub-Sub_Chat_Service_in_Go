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
