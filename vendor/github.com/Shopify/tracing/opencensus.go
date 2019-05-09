package tracing

import (
	"context"
	"fmt"
	"math/big"
	"net/http"

	gpropagation "go.opencensus.io/exporter/stackdriver/propagation"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

// HTTPFormat implements propagation.HTTPFormat to propagate
// traces in HTTP headers for Shopify services.
type HTTPFormat struct{}

var (
	_ propagation.HTTPFormat = (*HTTPFormat)(nil)

	googlePropagationFormat = &gpropagation.HTTPFormat{}
)

// SpanContextFromRequest extracts a Shopify/tracing span context from incoming requests.
func (f *HTTPFormat) SpanContextFromRequest(req *http.Request) (sc trace.SpanContext, ok bool) {
	if req.Header.Get(shopifyTraceHeader) != "" {
		sc, ok = googlePropagationFormat.SpanContextFromRequest(req)
	}
	return
}

// SpanContextToRequest modifies the given request to include a Shopify/tracing header.
func (f *HTTPFormat) SpanContextToRequest(sc trace.SpanContext, req *http.Request) {
	googlePropagationFormat.SpanContextToRequest(sc, req)
	req.Header.Set(shopifyTraceHeader, req.Header.Get(googleCloudTraceHeader))
}

type Exporter struct {
	*uploader
}

func NewExporter(ctx context.Context, opts ...Option) *Exporter {
	return &Exporter{uploader: newUploader(ctx, opts...)}
}

func (e *Exporter) ExportSpan(sd *trace.SpanData) {
	e.uploader.enqueue(translateSpan(sd))
}

func translateSpan(sd *trace.SpanData) *Span {
	span := &Span{
		Id:        translateSpanId(sd.SpanID),
		TraceId:   sd.TraceID.String(),
		Operation: sd.Name,
		Start:     timeToTimestamp(sd.StartTime),
		End:       timeToTimestamp(sd.EndTime),
		Tags:      make(map[string]string, len(sd.Attributes)),
	}
	if p := sd.ParentSpanID; p != (trace.SpanID{}) {
		span.Reference = &Span_ChildOf{ChildOf: translateSpanId(sd.ParentSpanID)}
	}
	for k, v := range sd.Attributes {
		span.Tags[k] = fmt.Sprintf("%v", v)
	}
	return span
}

func translateSpanId(id trace.SpanID) uint64 {
	var x big.Int
	return x.SetBytes(id[:]).Uint64()
}
