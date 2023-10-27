package tracing

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	otrace "go.opentelemetry.io/otel/trace"
)

type NoopSpan struct{}

func (NoopSpan) End(options ...otrace.SpanEndOption)                  {}
func (NoopSpan) AddEvent(name string, options ...otrace.EventOption)  {}
func (NoopSpan) IsRecording() bool                                    { return false }
func (NoopSpan) RecordError(err error, options ...otrace.EventOption) {}
func (NoopSpan) SpanContext() otrace.SpanContext                      { return otrace.SpanContext{} }
func (NoopSpan) SetStatus(code codes.Code, description string)        {}
func (NoopSpan) SetName(name string)                                  {}
func (NoopSpan) SetAttributes(kv ...attribute.KeyValue)               {}
func (NoopSpan) TracerProvider() otrace.TracerProvider                { return nil }

type NoopTracer struct{}

func (d *NoopTracer) Start(ctx context.Context, spanName string, opts ...otrace.SpanStartOption) (context.Context, otrace.Span) {
	return ctx, NoopSpan{}
}

type NoopProvider struct{}

type Provider struct {
	serviceName string
	exporterURL string
	provider    *trace.TracerProvider
}

func NewProvider(ctx context.Context, serviceName, exporterURL string, isNoop bool) (*Provider, error) {
	log.Debug().Msgf("Creating new tracing provider with service name %s and exporter url %s", serviceName, exporterURL)
	e, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(exporterURL),
		otlptracegrpc.WithInsecure(),
	))
	if err != nil {
		return nil, err
	}

	r := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String("0.0.1"),
	)

	s := trace.AlwaysSample()

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(s),
		trace.WithBatcher(e),
		trace.WithResource(r),
	)
	if isNoop {
		tracerProvider.Shutdown(ctx)
	}

	return &Provider{
		serviceName: serviceName,
		exporterURL: exporterURL,
		provider:    tracerProvider,
	}, nil
}

func (p *Provider) RegisterAsGlobal() (func(ctx context.Context) error, error) {
	log.Debug().Msgf("Registering tracing provider as global")
	// set global provider
	otel.SetTracerProvider(p.provider)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return p.provider.Shutdown, nil
}
