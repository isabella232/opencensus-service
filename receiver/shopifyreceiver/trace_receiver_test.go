package shopifyreceiver

import (
	"reflect"
	"testing"

	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/Shopify/tracing"
)

func TestConvertSpansToTraceSpans(t *testing.T) {
	spans := tracing.Spans{
		Spans: []*tracing.Span{
			nil,
			&tracing.Span{
				TraceId: "not hex",
			},
			&tracing.Span{
				Id:        1,
				TraceId:   "0123456789abcdef0123456789abcdef",
				Operation: "op",
				Start:     &timestamp.Timestamp{},
				End:       &timestamp.Timestamp{},
				Reference: &tracing.Span_FollowsFrom{FollowsFrom: 2},
			},
			&tracing.Span{
				Id:        2,
				TraceId:   "0123456789abcdef0123456789abcdef",
				Operation: "op",
				Start:     &timestamp.Timestamp{},
				End:       &timestamp.Timestamp{},
				Reference: &tracing.Span_ChildOf{ChildOf: 3},
				Tags: map[string]string{
					"span.kind": "client",
					"error":     "true",
				},
			},
			&tracing.Span{
				Id:        3,
				TraceId:   "0123456789abcdef0123456789abcdef",
				Operation: "op",
				Start:     &timestamp.Timestamp{},
				End:       &timestamp.Timestamp{},
				Tags: map[string]string{
					"span.kind": "client",
					"error":     "true",
					"key":       "value",
				},
				Links: &tracing.Links{
					Link: []*tracing.Link{
						nil,
						&tracing.Link{
							TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
							SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 1},
							Type:    tracing.Link_CHILD_LINKED_SPAN,
						},
						&tracing.Link{
							TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
							SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 2},
							Type:    tracing.Link_PARENT_LINKED_SPAN,
						},
						&tracing.Link{
							TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
							SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 3},
						},
					},
				},
			},
		},
	}
	ocSpans, invalidSpans, err := toTraceSpans(spans)

	if err != nil {
		t.Fatalf("Failed to convert Shopify spans to Trace spans: %v", err)
	}

	if invalidSpans != 2 {
		t.Fatalf("Expected 2 invalid span, got %d", invalidSpans)
	}

	if validSpans := len(spans.Spans) - 2; len(ocSpans) != validSpans {
		t.Fatalf("Expected %d Trace spans, got %d", validSpans, len(ocSpans))
	}

	want := []*tracepb.Span{
		&tracepb.Span{
			TraceId:      []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			SpanId:       []byte{0, 0, 0, 0, 0, 0, 0, 1},
			ParentSpanId: []byte{0, 0, 0, 0, 0, 0, 0, 2},
			Name:         &tracepb.TruncatableString{Value: "op"},
			Kind:         tracepb.Span_SPAN_KIND_UNSPECIFIED,
			StartTime:    &timestamp.Timestamp{},
			EndTime:      &timestamp.Timestamp{},
		},
		&tracepb.Span{
			TraceId:      []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			SpanId:       []byte{0, 0, 0, 0, 0, 0, 0, 2},
			ParentSpanId: []byte{0, 0, 0, 0, 0, 0, 0, 3},
			Name:         &tracepb.TruncatableString{Value: "op"},
			Kind:         tracepb.Span_CLIENT,
			StartTime:    &timestamp.Timestamp{},
			EndTime:      &timestamp.Timestamp{},
			Status: &tracepb.Status{
				Code:    2,
				Message: "UNKNOWN",
			},
		},
		&tracepb.Span{
			TraceId:   []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
			SpanId:    []byte{0, 0, 0, 0, 0, 0, 0, 3},
			Name:      &tracepb.TruncatableString{Value: "op"},
			Kind:      tracepb.Span_CLIENT,
			StartTime: &timestamp.Timestamp{},
			EndTime:   &timestamp.Timestamp{},
			Status: &tracepb.Status{
				Code:    2,
				Message: "UNKNOWN",
			},
			Attributes: &tracepb.Span_Attributes{
				AttributeMap: map[string]*tracepb.AttributeValue{
					"key": &tracepb.AttributeValue{
						Value: &tracepb.AttributeValue_StringValue{
							StringValue: &tracepb.TruncatableString{
								Value: "value"}}},
				},
			},
			Links: &tracepb.Span_Links{
				Link: []*tracepb.Span_Link{
					&tracepb.Span_Link{
						TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
						SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 1},
						Type:    tracepb.Span_Link_CHILD_LINKED_SPAN,
					},
					&tracepb.Span_Link{
						TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
						SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 2},
						Type:    tracepb.Span_Link_PARENT_LINKED_SPAN,
					},
					&tracepb.Span_Link{
						TraceId: []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef},
						SpanId:  []byte{0, 0, 0, 0, 0, 0, 0, 3},
						Type:    tracepb.Span_Link_TYPE_UNSPECIFIED,
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(want, ocSpans) {
		t.Fatalf("Failed to convert Shopify spans to Trace spans, want: %s got: %s", want, ocSpans)
	}
}
