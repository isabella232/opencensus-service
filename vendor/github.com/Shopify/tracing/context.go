package tracing

import opentracing "github.com/opentracing/opentracing-go"

type spanContext interface {
	opentracing.SpanContext
	TraceID() string
	SpanID() uint64
	Sampled() bool
}

type wireContext struct {
	traceID string
	spanID  uint64
	sampled bool
}

func (c wireContext) TraceID() string                                   { return c.traceID }
func (c wireContext) SpanID() uint64                                    { return c.spanID }
func (c wireContext) Sampled() bool                                     { return c.sampled }
func (c wireContext) ForeachBaggageItem(handler func(k, v string) bool) {}
