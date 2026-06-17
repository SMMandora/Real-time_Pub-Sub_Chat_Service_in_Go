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
