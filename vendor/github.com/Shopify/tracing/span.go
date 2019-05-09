package tracing

import (
	"fmt"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type span struct {
	*tracer
	sync.Mutex
	Span
}

// parentSpanID is a helper function on the generated type Span.
func (s Span) parentSpanID() uint64 {
	if id := s.GetChildOf(); id != 0 {
		return id
	}
	return s.GetFollowsFrom()
}

func (s *span) TraceID() string                                   { return s.Span.TraceId }
func (s *span) SpanID() uint64                                    { return s.Span.Id }
func (s *span) Sampled() bool                                     { return true }
func (s *span) Finish()                                           { s.FinishWithOptions(opentracing.FinishOptions{}) }
func (s *span) Tracer() opentracing.Tracer                        { return s.tracer }
func (s *span) Context() opentracing.SpanContext                  { return s }
func (s *span) ForeachBaggageItem(handler func(k, v string) bool) {}

func (s *span) SetOperationName(operationName string) opentracing.Span {
	s.Lock()
	defer s.Unlock()
	s.Operation = operationName
	return s
}

func (s *span) SetTag(key string, value interface{}) opentracing.Span {
	s.Lock()
	defer s.Unlock()
	if s.Tags == nil {
		s.Tags = map[string]string{key: fmt.Sprint(value)}
	} else {
		s.Tags[key] = fmt.Sprint(value)
	}
	return s
}

func (s *span) FinishWithOptions(opts opentracing.FinishOptions) {
	finishTime := opts.FinishTime
	if finishTime.IsZero() {
		finishTime = time.Now()
	}

	s.Lock()
	defer s.Unlock()

	s.End = timeToTimestamp(finishTime)
	s.tracer.recordSpan(s.Span)
}

// Logging & baggage not implemented.
func (s *span) SetBaggageItem(key, val string) opentracing.Span       { return s }
func (s *span) BaggageItem(key string) string                         { return "" }
func (s *span) LogFields(fields ...log.Field)                         {}
func (s *span) LogKV(keyVals ...interface{})                          {}
func (s *span) LogEvent(event string)                                 {}
func (s *span) LogEventWithPayload(event string, payload interface{}) {}
func (s *span) Log(data opentracing.LogData)                          {}
