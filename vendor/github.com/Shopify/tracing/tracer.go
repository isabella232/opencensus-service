package tracing

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	dogstatsd "github.com/Shopify/go-dogstatsd"
	"github.com/golang/protobuf/ptypes/timestamp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type Format byte

const (
	// Whitelists
	ShopifyDomainWhitelist = `(\.shopify((\.com|\.io)|((dc|cloud)\.com))$)|(\.myshopify\.(io|com)$)`
	PrivateIP10Whitelist   = `\A10\.\d{1,3}\.\d{1,3}\.\d{1,3}\z`
	PrivateIP192Whitelist  = `\A192\.168\.\d{1,3}\.\d{1,3}\z`
	PrivateIP172Whitelist  = `\A172\.(1[6-9]|2[0-9]|3[01])\.\d{1,3}\.\d{1,3}\z`
	LocalhostWhitelist     = `\A127\.0\.0\.1\z`

	// Environment represents SpanContexts as key:value string pairs.
	//
	// Unlike TextMap, the Environment format requires that the keys
	// must match the regex [A-Za-z_][A-Za-z0-9_]*.
	//
	// For Tracer.Inject(): the carrier must be a `TextMapWriter`.
	//
	// For Tracer.Extract(): the carrier must be a `TextMapReader`.
	Environment Format = iota

	// GRPC represents SpanContexts as headers in GRPC Metadata
	// The carrier must be a metadata.MD from grpc-go
	GRPCMetadata
)

var (
	// SpanKindJob marks a span representing the execution of a background job.
	SpanKindJob      = opentracing.Tag{Key: string(ext.SpanKind), Value: SpanKindJobEnum}
	SpanKindJobEnum  = ext.SpanKindEnum("job")
	CommonWhitelists = Whitelists{ShopifyDomainWhitelist, PrivateIP10Whitelist, PrivateIP192Whitelist, PrivateIP172Whitelist}
	tracerDog        *dogstatsd.Client
	statsTags        []string
)

type jobOption struct {
	enqueueContext opentracing.SpanContext
}

func (j jobOption) Apply(o *opentracing.StartSpanOptions) {
	if j.enqueueContext != nil {
		opentracing.FollowsFrom(j.enqueueContext).Apply(o)
	}
	SpanKindJob.Apply(o)
}

// JobOption returns a StartSpanOption appropriate for a background job span
// with `enqueue` representing the metadata for the remote peer Span if available.
// In case enqueue == nil, due to the submitter not being instrumented, this job
// span will be a root span.
func JobOption(enqueue opentracing.SpanContext) opentracing.StartSpanOption {
	return jobOption{enqueue}
}

type tracer struct {
	textPropagator  *textMapPropagator
	grpcPropagator  *grpcMetadataPropagator
	spanIDCounter   uint64
	spanIDIncrement uint64
	hostWhitelist   *regexp.Regexp
	*uploader
}

func timeToTimestamp(t time.Time) *timestamp.Timestamp {
	return &timestamp.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}

func timestampToTime(t *timestamp.Timestamp) time.Time {
	return time.Unix(t.Seconds, int64(t.Nanos))
}

func TraceURLForContext(ctx context.Context) string {
	span, _ := opentracing.SpanFromContext(ctx).(*span)
	if span == nil {
		return ""
	}
	return fmt.Sprintf("https://console.cloud.google.com/traces/traces?project=shopify-tiers&tid=%s", span.TraceID())
}

func New(ctx context.Context, opts ...Option) opentracing.Tracer {
	// Taken from cloud.google.com/go/trace.init().
	// Set spanIDCounter and spanIDIncrement to random values. nextSpanID will
	// return an arithmetic progression using these values, skipping zero. We set
	// the LSB of spanIDIncrement to 1, so that the cycle length is 2^64.
	var spanIDCounter, spanIDIncrement uint64
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDCounter)
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDIncrement)
	spanIDIncrement |= 1

	options := options{}
	for _, o := range opts {
		options = o.apply(options)
	}

	if options.applicationName != "" {
		statsTags = append(statsTags, fmt.Sprintf("application:%s", options.applicationName))
	}
	statsdAddr, ok := os.LookupEnv("STATSD_ADDR")
	if ok {
		if dog, err := dogstatsd.New(statsdAddr, &dogstatsd.Context{Namespace: "shopify.tracing."}); err == nil {
			tracerDog = dog
		}
	}

	t := &tracer{
		spanIDCounter:   spanIDCounter,
		spanIDIncrement: spanIDIncrement,
		uploader:        newUploader(ctx, opts...),
	}
	if whitelist := strings.Join(options.whitelists, "|"); whitelist != "" {
		t.hostWhitelist = regexp.MustCompile(whitelist)
	}
	t.textPropagator = &textMapPropagator{t}
	return t
}

// nextSpanID returns a new span ID. It will never return zero.
// Taken from cloud.google.com/go/trace.nextSpanID().
func (t *tracer) nextSpanID() uint64 {
	var id uint64
	for id == 0 {
		id = atomic.AddUint64(&t.spanIDCounter, t.spanIDIncrement)
	}
	return id
}

// nextTraceID returns a new trace ID.
// Taken from cloud.google.com/go/trace.nextTraceID().
func (t *tracer) nextTraceID() string {
	id1 := t.nextSpanID()
	id2 := t.nextSpanID()
	return fmt.Sprintf("%016x%016x", id1, id2)
}

func (t *tracer) StartSpan(operationName string, opts ...opentracing.StartSpanOption) opentracing.Span {
	sso := opentracing.StartSpanOptions{}
	for _, o := range opts {
		o.Apply(&sso)
	}
	return t.StartSpanWithOptions(operationName, sso)
}

func (t *tracer) StartSpanWithOptions(operationName string, opts opentracing.StartSpanOptions) opentracing.Span {
	startTime := opts.StartTime
	if startTime.IsZero() {
		startTime = time.Now()
	}

	s := &span{tracer: t}
	s.Operation = operationName
	s.Start = timeToTimestamp(startTime)
	s.Tags = make(map[string]string)
	for k, v := range opts.Tags {
		s.Tags[k] = fmt.Sprint(v)
	}
	newSpanID := t.nextSpanID()
	for _, ref := range opts.References {
		switch ref.Type {
		case opentracing.ChildOfRef, opentracing.FollowsFromRef:
			refCtx := ref.ReferencedContext.(spanContext)
			if ref.Type == opentracing.ChildOfRef {
				s.Reference = &Span_ChildOf{refCtx.SpanID()}
			} else {
				s.Reference = &Span_FollowsFrom{refCtx.SpanID()}
			}
			s.Span.Id = newSpanID
			s.Span.TraceId = refCtx.TraceID()
			return s
		}
	}
	s.Span.Id = newSpanID
	s.Span.TraceId = t.nextTraceID()
	return s
}

func (t *tracer) Inject(sc opentracing.SpanContext, format interface{}, carrier interface{}) error {
	switch format {
	case opentracing.TextMap, opentracing.HTTPHeaders, Environment:
		return t.textPropagator.Inject(sc, carrier)
	case GRPCMetadata:
		return t.grpcPropagator.Inject(sc, carrier)
	}
	return opentracing.ErrUnsupportedFormat
}

func (t *tracer) Extract(format interface{}, carrier interface{}) (opentracing.SpanContext, error) {
	switch format {
	case opentracing.TextMap, opentracing.HTTPHeaders, Environment:
		return t.textPropagator.Extract(carrier)
	case GRPCMetadata:
		return t.grpcPropagator.Extract(carrier)
	}
	return nil, opentracing.ErrUnsupportedFormat
}

func (t *tracer) recordSpan(s Span) {
	t.uploader.enqueue(&s)
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
