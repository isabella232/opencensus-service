package tracing

import (
	"net/http"
	"net/url"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type tracingTransport struct {
	transport http.RoundTripper
}

func hostWhitelisted(req *http.Request) bool {
	if s := opentracing.SpanFromContext(req.Context()); s == nil {
		return true // No span in request context.
	} else if ot := s.Tracer(); ot == nil {
		return true // No tracer for span.
	} else if t, ok := ot.(*tracer); !ok {
		return true // Unknown tracer.
	} else if t.hostWhitelist == nil {
		return true // If nothing has been whitelisted assume everything is allowed.
	} else {
		u := CleanURL(req)
		return t.hostWhitelist.MatchString(u.Hostname())
	}
}

func (t tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !hostWhitelisted(req) {
		return t.transport.RoundTrip(req)
	}
	u := CleanURL(req)
	span, _ := opentracing.StartSpanFromContext(req.Context(), u.String(), ext.SpanKindRPCClient, opentracing.Tags{string(ext.PeerService): u.Hostname(), string(ext.HTTPMethod): req.Method, string(ext.HTTPUrl): u})
	span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(req.Header))
	defer span.Finish()

	resp, err := t.transport.RoundTrip(req)
	if err != nil {
		ext.Error.Set(span, true)
	} else {
		ext.HTTPStatusCode.Set(span, uint16(resp.StatusCode))
	}

	return resp, err
}

func NewHTTPTransport(orig http.RoundTripper) http.RoundTripper {
	return tracingTransport{orig}
}

func NewHTTPClient(orig *http.Client) *http.Client {
	if orig == nil {
		orig = http.DefaultClient
	}
	transport := orig.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &http.Client{
		Transport:     NewHTTPTransport(transport),
		CheckRedirect: orig.CheckRedirect,
		Jar:           orig.Jar,
		Timeout:       orig.Timeout,
	}
}

func CleanURL(req *http.Request) *url.URL {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	return &url.URL{Scheme: req.URL.Scheme, Host: host}
}
