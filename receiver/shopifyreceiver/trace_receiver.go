package shopifyreceiver

import (
	"context"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"sync"

	"github.com/Shopify/tracing"
	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/proto"
	"go.opencensus.io/trace"

	"github.com/census-instrumentation/opencensus-service/consumer"
	"github.com/census-instrumentation/opencensus-service/data"
	"github.com/census-instrumentation/opencensus-service/observability"
	"github.com/census-instrumentation/opencensus-service/receiver"
	tracetranslator "github.com/census-instrumentation/opencensus-service/translator/trace"
)

const (
	requestSizeLimit = 1024 * 1024
	defaultAddress   = ":8096"
	traceSource      = "Shopify"
)

// ShopifyReceiver type is used to handle spans received in the Shopify format.
type ShopifyReceiver struct {
	// mu protects the fields of this struct
	mu sync.Mutex

	// addr is the address onto which the HTTP server will be bound
	addr string

	nextConsumer consumer.TraceConsumer

	startOnce sync.Once
	stopOnce  sync.Once
	server    *http.Server
}

var (
	_ receiver.TraceReceiver = (*ShopifyReceiver)(nil)
	_ http.Handler           = (*ShopifyReceiver)(nil)

	errNilNextConsumer = errors.New("nil nextConsumer")
	errAlreadyStarted  = errors.New("already started")
	errAlreadyStopped  = errors.New("already stopped")
)

// New creates a new shopifyreceiver.ShopifyReceiver reference.
func New(address string, nextConsumer consumer.TraceConsumer) (*ShopifyReceiver, error) {
	if nextConsumer == nil {
		return nil, errNilNextConsumer
	}

	sr := &ShopifyReceiver{
		addr:         address,
		nextConsumer: nextConsumer,
	}
	return sr, nil
}

func (sr *ShopifyReceiver) address() string {
	addr := sr.addr
	if addr == "" {
		addr = defaultAddress
	}
	return addr
}

// TraceSource returns the name of the trace data source.
func (sr *ShopifyReceiver) TraceSource() string {
	return traceSource
}

// StartTraceReception spins up the receiver's HTTP server and makes the receiver start its processing.
func (sr *ShopifyReceiver) StartTraceReception(ctx context.Context, asyncErrorChan chan<- error) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	var err = errAlreadyStarted

	sr.startOnce.Do(func() {
		ln, lerr := net.Listen("tcp", sr.address())
		if lerr != nil {
			err = lerr
			return
		}

		server := &http.Server{Handler: sr}
		go func() {
			asyncErrorChan <- server.Serve(ln)
		}()

		sr.server = server

		err = nil
	})

	return err
}

// StopTraceReception tells the receiver that should stop reception,
// giving it a chance to perform any necessary clean-up and shutting down
// its HTTP server.
func (sr *ShopifyReceiver) StopTraceReception(ctx context.Context) error {
	var err = errAlreadyStopped
	sr.stopOnce.Do(func() {
		err = sr.server.Close()
	})
	return err
}

func (sr *ShopifyReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/":
		sr.ingest(w, r)
	default:
		http.NotFound(w, r)
	}
}

func extractKind(span *tracing.Span) tracepb.Span_SpanKind {
	switch span.GetTags()["span.kind"] {
	case "client":
		return tracepb.Span_CLIENT
	case "server":
		return tracepb.Span_SERVER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func tagsToTraceAttributes(tags map[string]string) *tracepb.Span_Attributes {
	if len(tags) == 0 {
		return nil
	}
	attributes := make(map[string]*tracepb.AttributeValue, len(tags))
	for k, v := range tags {
		if k == "span.kind" || k == "error" {
			continue
		}
		attributes[k] = &tracepb.AttributeValue{
			Value: &tracepb.AttributeValue_StringValue{
				StringValue: &tracepb.TruncatableString{
					Value: v}}}
	}
	if len(attributes) == 0 {
		return nil
	}
	return &tracepb.Span_Attributes{AttributeMap: attributes}
}

func extractLinks(span *tracing.Span) *tracepb.Span_Links {
	links := span.GetLinks().GetLink()
	if len(links) == 0 {
		return nil
	}
	ls := make([]*tracepb.Span_Link, 0, len(links))
	for _, l := range links {
		if l == nil {
			continue
		}
		ls = append(ls, &tracepb.Span_Link{
			TraceId: l.TraceId,
			SpanId:  l.SpanId,
			Type:    tracepb.Span_Link_Type(l.Type),
		})
	}
	return &tracepb.Span_Links{Link: ls}
}

func extractStatus(span *tracing.Span) *tracepb.Status {
	// TODO: the 'error' tag from OpenTracing is too simplistic. We could map 'http.status_code' here instead.
	if span.GetTags()["error"] != "true" {
		return nil
	}
	return &tracepb.Status{
		Code:    2,
		Message: "UNKNOWN",
	}
}

func toTraceSpans(spans tracing.Spans) (ocSpans []*tracepb.Span, invalidSpans int, err error) {
	ocSpans = make([]*tracepb.Span, 0, len(spans.Spans))
	for _, span := range spans.Spans {
		if span == nil {
			invalidSpans++
			continue
		}
		traceID, err := hex.DecodeString(span.TraceId)
		if err != nil {
			invalidSpans++
			continue
		}
		parentSpanID := span.GetChildOf()
		if parentSpanID == 0 {
			parentSpanID = span.GetFollowsFrom()
		}
		ocSpans = append(ocSpans, &tracepb.Span{
			TraceId:      traceID[:16],
			SpanId:       tracetranslator.UInt64ToByteSpanID(span.Id),
			ParentSpanId: tracetranslator.UInt64ToByteSpanID(parentSpanID),
			Name:         &tracepb.TruncatableString{Value: span.Operation},
			Kind:         extractKind(span),
			StartTime:    span.Start,
			EndTime:      span.End,
			Attributes:   tagsToTraceAttributes(span.Tags),
			Links:        extractLinks(span),
			Status:       extractStatus(span),
		})
	}
	return
}

func (sr *ShopifyReceiver) ingest(w http.ResponseWriter, r *http.Request) {
	// Trace this method
	ctx, span := trace.StartSpan(context.Background(), "ShopifyReceiver.Export")
	defer span.End()

	// Check method.
	if r.Method != http.MethodPost {
		span.SetStatus(trace.Status{
			Code:    trace.StatusCodeUnimplemented,
			Message: http.StatusText(http.StatusMethodNotAllowed),
		})
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read.
	b, err := ioutil.ReadAll(http.MaxBytesReader(w, r.Body, requestSizeLimit))
	if err != nil {
		span.SetStatus(trace.Status{
			Code:    trace.StatusCodeInvalidArgument,
			Message: err.Error(),
		})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate.
	spans := &tracing.Spans{}
	if err := proto.Unmarshal(b, spans); err != nil {
		span.SetStatus(trace.Status{
			Code:    trace.StatusCodeInvalidArgument,
			Message: err.Error(),
		})
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	proxiedApplication := r.Header.Get("X-Shopify-Trace-Application")
	if proxiedApplication == "" {
		proxiedApplication = "unknown"
	}

	// Translate.
	ocSpans, invalidSpans, err := toTraceSpans(*spans)
	if err != nil {
		span.SetStatus(trace.Status{
			Code:    trace.StatusCodeInvalidArgument,
			Message: err.Error(),
		})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Process.
	ctxWithReceiverName := observability.ContextWithReceiverName(ctx, "shopify")
	sr.nextConsumer.ConsumeTraceData(ctxWithReceiverName, data.TraceData{
		Node:  &commonpb.Node{ServiceInfo: &commonpb.ServiceInfo{Name: proxiedApplication}},
		Spans: ocSpans,
	})

	observability.RecordTraceReceiverMetrics(ctxWithReceiverName, len(ocSpans), invalidSpans)
}
