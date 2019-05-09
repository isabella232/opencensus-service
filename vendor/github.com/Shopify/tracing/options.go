package tracing

import "time"

// An Option is a functional option.
type Option interface {
	apply(options) options
}

type option func(options) options

func (f option) apply(opts options) options { return f(opts) }

type options struct {
	uploadTimeout   time.Duration
	flushInterval   time.Duration
	proxyURL        string
	byteLimit       int
	bufferLimit     int
	whitelists      []string
	applicationName string
}

func WithBufferLimit(bufferLimit int) Option {
	return option(func(opts options) options {
		opts.bufferLimit = bufferLimit
		return opts
	})
}

func WithByteLimit(bytes int) Option {
	return option(func(opts options) options {
		opts.byteLimit = bytes
		return opts
	})
}

func WithFlushInterval(flushInterval time.Duration) Option {
	return option(func(opts options) options {
		opts.flushInterval = flushInterval
		return opts
	})
}

func WithProxyURL(proxyURL string) Option {
	return option(func(opts options) options {
		opts.proxyURL = proxyURL
		return opts
	})
}

func WithTimeout(timeout time.Duration) Option {
	return option(func(opts options) options {
		opts.uploadTimeout = timeout
		return opts
	})
}

func WithApplicationName(an string) Option {
	return option(func(opts options) options {
		opts.applicationName = an
		return opts
	})
}

type Whitelists []string

func (ws Whitelists) apply(opts options) options {
	opts.whitelists = append(opts.whitelists, ws...)
	return opts
}
