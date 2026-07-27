// Package tracing configures OpenTelemetry tracing for Kairos, exporting
// spans via OTLP HTTP to a collector (e.g. Jaeger, Tempo, or the OTel
// Collector).
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the package-wide tracer name used for all Kairos spans.
const Tracer = "github.com/harshalvk/kairos"

// Setup configures the global OpenTelemetry trace provider, exporting
// spans via OTLP HTTP to endpoint (e.g. "localhost:4318" for a local
// collector). serviceName identifies this process in traces (e.g.
// "kairos-worker"). Returns a shutdown func to flush and close the
// exporter on process exit.
func Setup(ctx context.Context, endpoint, serviceName string) (func(context.Context) error, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(), // local collector, no TLS
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// StartSpan is a small convenience wrapper so call sites don't need to
// import otel.Tracer(...) directly everywhere.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return otel.Tracer(Tracer).Start(ctx, name)
}
