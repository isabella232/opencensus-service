package tracing

import (
	"fmt"
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"google.golang.org/grpc/metadata"
)

type textMapPropagator struct{ *tracer }

const (
	googleCloudTraceHeader = "x-cloud-trace-context"
	shopifyTraceHeader     = "x-shopify-trace-context"
)

func (p *textMapPropagator) Inject(context opentracing.SpanContext, opaqueCarrier interface{}) error {
	sc, ok := context.(spanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	carrier, ok := opaqueCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	header := toGoogleTraceHeader(sc.TraceID(), sc.SpanID(), sc.Sampled())
	carrier.Set(googleCloudTraceHeader, header)
	carrier.Set(shopifyTraceHeader, header)
	return nil
}

func (p *textMapPropagator) Extract(opaqueCarrier interface{}) (opentracing.SpanContext, error) {
	carrier, ok := opaqueCarrier.(opentracing.TextMapReader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	var traceID string
	var spanID uint64
	var sampled bool
	var err error
	var googleCloudTraceHeaderFound bool
	var shopifyTraceHeaderFound bool
	err = carrier.ForeachKey(func(k, v string) error {
		switch strings.ToLower(k) {
		case shopifyTraceHeader:
			shopifyTraceHeaderFound = true
		case googleCloudTraceHeader:
			traceID, spanID, sampled, googleCloudTraceHeaderFound = fromGoogleTraceHeader(v)
			if !googleCloudTraceHeaderFound {
				return opentracing.ErrSpanContextCorrupted
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !shopifyTraceHeaderFound || !googleCloudTraceHeaderFound {
		return nil, opentracing.ErrSpanContextNotFound
	}

	if tracerDog != nil {
		if sampled {
			tracerDog.Count("sampled", 1, statsTags, 1)
		} else {
			tracerDog.Count("unsampled", 1, statsTags, 1)
		}
	}
	return wireContext{
		traceID: traceID,
		spanID:  spanID,
		sampled: sampled,
	}, nil
}

type grpcMetadataPropagator struct{ *tracer }

func (p *grpcMetadataPropagator) Inject(context opentracing.SpanContext, opaqueCarrier interface{}) error {
	sc, ok := context.(spanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	carrier, ok := opaqueCarrier.(metadata.MD)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	header := toGoogleTraceHeader(sc.TraceID(), sc.SpanID(), sc.Sampled())
	carrier[googleCloudTraceHeader] = []string{header}
	return nil
}

func (p *grpcMetadataPropagator) Extract(opaqueCarrier interface{}) (opentracing.SpanContext, error) {
	carrier, ok := opaqueCarrier.(metadata.MD)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}

	headers, ok := carrier[googleCloudTraceHeader]
	if !ok || len(headers) < 1 {
		return nil, opentracing.ErrSpanContextNotFound
	}

	traceID, spanID, sampled, googleCloudTraceHeaderFound := fromGoogleTraceHeader(headers[0])
	if !googleCloudTraceHeaderFound {
		return nil, opentracing.ErrSpanContextCorrupted
	}

	if tracerDog != nil {
		if sampled {
			tracerDog.Count("sampled", 1, statsTags, 1)
		} else {
			tracerDog.Count("unsampled", 1, statsTags, 1)
		}
	}

	return wireContext{
		traceID: traceID,
		spanID:  spanID,
		sampled: sampled,
	}, nil
}

func toGoogleTraceHeader(traceID string, spanID uint64, sampled bool) string {
	options := 0
	if sampled {
		options = 1
	}
	return fmt.Sprintf("%s/%d;o=%d", traceID, spanID, options)
}

func fromGoogleTraceHeader(h string) (traceID string, spanID uint64, sampled, ok bool) {
	// See https://cloud.google.com/trace/docs/faq for the header format.
	// Return if the header is empty or missing, or if the header is unreasonably
	// large, to avoid making unnecessary copies of a large string.
	if h == "" || len(h) > 200 {
		return
	}

	// Parse the trace id field.
	slash := strings.Index(h, `/`)
	if slash == -1 {
		return
	}
	traceID, h = h[:slash], h[slash+1:]

	// Parse the span id field.
	semicolon := strings.Index(h, `;`)
	if semicolon == -1 {
		return
	}
	spanstr, h := h[:semicolon], h[semicolon+1:]
	spanID, err := strconv.ParseUint(spanstr, 10, 64)
	if err != nil {
		return
	}

	// Parse the options field.
	if !strings.HasPrefix(h, "o=") {
		return
	}
	o, err := strconv.ParseUint(h[2:], 10, 64)
	if err != nil {
		return
	}
	sampled = o != 0
	ok = true
	return
}

// EnvironmentCarrier satisfies both opentracing.TextMapWriter and opentracing.TextMapReader.
//
// Example usage for extraction:
//
//     m := map[string]string
//     carrier := tracer.EnvironmentCarrier(m)
//     spanContext, err := tracer.Extract(tracer.Environment, carrier)
//
// Example usage injection:
//
//     m := map[string]string
//     carrier := tracer.EnvironmentCarrier(m)
//     err := tracer.Inject(
//         span.Context(),
//         tracer.Environment,
//         carrier)
//
type EnvironmentCarrier map[string]string

// Set conforms to the TextMapWriter interface.
func (c EnvironmentCarrier) Set(key, val string) {
	c[strings.Replace(key, "-", "_", -1)] = val
}

// ForeachKey conforms to the TextMapReader interface.
func (c EnvironmentCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range c {
		switch hk := strings.Replace(k, "_", "-", -1); hk {
		case googleCloudTraceHeader, shopifyTraceHeader:
			k = hk
		}
		if err := handler(k, v); err != nil {
			return err
		}
	}
	return nil
}
