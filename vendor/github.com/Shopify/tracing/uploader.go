package tracing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context/ctxhttp"
)

const MaxRequestSize = 1024 * 1024
const DefaultProxyURL = "http://trace-proxy.cluster-services:8096/"

type uploader struct {
	options
	spanChannel chan *Span
}

func newUploader(ctx context.Context, opts ...Option) *uploader {
	proxyURL, ok := os.LookupEnv("TRACE_PROXY_ADDR")
	if !ok {
		proxyURL = DefaultProxyURL
	}
	options := options{
		flushInterval: 1 * time.Second,
		byteLimit:     MaxRequestSize,
		bufferLimit:   100000,
		uploadTimeout: 10 * time.Second,
		proxyURL:      proxyURL,
	}
	for _, o := range opts {
		options = o.apply(options)
	}
	if _, err := url.Parse(options.proxyURL); err != nil {
		options.proxyURL = DefaultProxyURL
	}

	u := &uploader{
		options:     options,
		spanChannel: make(chan *Span, options.bufferLimit),
	}
	log.WithFields(log.Fields{"proxyURL": u.proxyURL, "bufferLimit": u.bufferLimit, "byteLimit": u.byteLimit}).Info("initialized proxy uploader")
	if u.flushInterval > 0 {
		go u.periodicUploader(ctx, u.flushInterval)
	}
	return u
}

func (u *uploader) periodicUploader(ctx context.Context, uploadInterval time.Duration) {
	ticker := time.NewTicker(uploadInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(u.spanChannel)
			return
		case <-ticker.C:
			u.upload(ctx)
		}
	}
}

func (u *uploader) reenqueueOrFail(ctx context.Context, spansToSend []*Span) {
	spansLeft := len(spansToSend)
	for _, s := range spansToSend {
		spansLeft--
		if err := u.enqueue(s); err != nil {
			if tracerDog != nil && spansLeft > 0 {
				tracerDog.Count("proxy_producer.dropped_span", int64(spansLeft), statsTags, 1)
			}
			break
		}
	}
}

var ErrEnqueueBufferFull = errors.New("Enqueue buffer is full, dropping span")

func (u *uploader) enqueue(s *Span) error {
	select {
	case u.spanChannel <- s:
	default:
		if tracerDog != nil {
			tracerDog.Count("proxy_producer.dropped_span", 1, statsTags, 1)
		}
		return ErrEnqueueBufferFull
	}
	return nil
}

func (u *uploader) dequeueSpans() (spans []*Span) {
	totalSize := 0 // Total size of the protobuf-encoded individual spans.
	for {
		select {
		case s := <-u.spanChannel:
			protoSize := proto.Size(s)

			// The encoded buffer will include the encoded span + the encoded length + a tag byte for each span entry.
			protoSize += 1 + proto.SizeVarint(uint64(protoSize))
			totalSize += protoSize

			switch {
			case protoSize > u.byteLimit: // Drop the span if it is too big for a packet by itself.
			case totalSize > u.byteLimit: // Re-enqueue the span if it pushed us over the limit.
				u.enqueue(s)
				return
			default:
				spans = append(spans, s)
			}

		default: // No more spans in the queue - we're done.
			return
		}
	}
}

// Upload read Spans from channel until we reach our limits, or until it is empty
func (u *uploader) upload(ctx context.Context) error {
	spansToSend := u.dequeueSpans()
	if len(spansToSend) == 0 {
		return nil
	}

	// Marshal spans.
	marshalStart := time.Now()
	marshaledBytes, err := proto.Marshal(&Spans{Spans: spansToSend})
	marshalTime := time.Since(marshalStart)
	if err != nil {
		if tracerDog != nil {
			tracerDog.Count("proxy_producer.dropped_span", int64(len(spansToSend)), statsTags, 1)
		}
		return err
	}
	if tracerDog != nil {
		tracerDog.Timer("proxy_producer.encode", marshalTime, statsTags, 1)
		tracerDog.Count("proxy_producer.payload_bytes", int64(len(marshaledBytes)), statsTags, 1)
		tracerDog.Histogram("proxy_producer.payload_size", float64(len(marshaledBytes)), statsTags, 1)
	}

	// Build request.
	req, err := http.NewRequest(http.MethodPost, u.proxyURL, bytes.NewReader(marshaledBytes))
	if err != nil {
		if tracerDog != nil {
			tracerDog.Count("proxy_producer.dropped_span", int64(len(spansToSend)), statsTags, 1)
		}
		return err
	}
	req.Header.Add("X-Shopify-Trace-Application", u.applicationName)

	// Send request.
	ctx, cancel := context.WithTimeout(ctx, u.uploadTimeout)
	defer cancel()
	patchStart := time.Now()
	resp, err := ctxhttp.Do(ctx, nil, req)
	patchTime := time.Since(patchStart)
	if err != nil {
		u.reenqueueOrFail(ctx, spansToSend)
		if tracerDog != nil {
			switch err {
			case context.DeadlineExceeded:
				tracerDog.Count("proxy_producer.api_calls.failure", 1, []string{"reason:timeout"}, 1)
			default:
				tracerDog.Count("proxy_producer.api_calls.failure", 1, []string{"reason:unknown"}, 1)
			}
		}
		return err
	}

	if tracerDog != nil {
		// Happy path.
		if resp.StatusCode == http.StatusOK {
			traces := make(map[string]int)
			for _, span := range spansToSend {
				traces[span.TraceId]++
			}
			tracerDog.Timer("proxy_producer.post_traces", patchTime, statsTags, 1)
			tracerDog.Count("proxy_producer.uploaded_traces", int64(len(traces)), statsTags, 1)
			tracerDog.Count("proxy_producer.api_calls.success", 1, statsTags, 1)
			for _, trace := range traces {
				tracerDog.Count("proxy_producer.spans", int64(trace), statsTags, 1)
				tracerDog.Histogram("proxy_producer.span_length", float64(trace), statsTags, 1)
			}
			return nil
		}

		// Sad path.
		reason := "unknown"
		switch resp.StatusCode {
		case http.StatusBadRequest:
			reason = "bad_request"
		case http.StatusUnsupportedMediaType:
			reason = "unsupported_request"
		case http.StatusMethodNotAllowed:
			reason = "method_not_allowed"
		case http.StatusInternalServerError:
			reason = "server_error"
		}
		errorTags := []string{fmt.Sprintf("reason:%s", reason)}
		tracerDog.Count("proxy_producer.api_calls.failure", 1, errorTags, 1)
	}
	return nil
}
