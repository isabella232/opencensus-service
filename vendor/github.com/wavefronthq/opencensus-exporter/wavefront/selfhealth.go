package wavefront

import (
	"log"
	"sync/atomic"
	"time"
)

const (
	DefaultSelfReportDuration = 5 * time.Minute
	metDropReportName         = "opencensus.exporter.dropped_metrics"
	spanDropReportName        = "opencensus.exporter.dropped_spans"
	senderErrorsReportName    = "opencensus.exporter.sender_errors"
)

type _SelfMetrics struct {
	spansDropped     uint64
	metricsDropped   uint64
	senderErrors     uint64
	selfHealthTicker *time.Ticker
	stopSelfHealth   chan struct{}
}

// ReportSelfHealth sends exporter specific metrics to wavefront
// Currently, only dropped span & metric counts are reported
func (e *Exporter) ReportSelfHealth() {
	e.reportStart(DefaultSelfReportDuration)
}

func (e *Exporter) reportStart(d time.Duration) {
	e.selfHealthTicker = time.NewTicker(DefaultSelfReportDuration)
	e.stopSelfHealth = make(chan struct{})
	go func() {
		for {
			select {
			case <-e.selfHealthTicker.C:
				e.sendSelfHealth()
			case <-e.stopSelfHealth:
				e.selfHealthTicker.Stop()
				e.selfHealthTicker = nil
				return
			}
		}
	}()
}

// StopSelfHealth stops reporting exporter specific metrics
func (e *Exporter) StopSelfHealth() {
	if e.stopSelfHealth == nil {
		return
	}

	e.stopSelfHealth <- struct{}{}
	e.stopSelfHealth = nil
	e.sendSelfHealth()
	if e.senderErrors > 0 {
		log.Printf("Warning: Total %d send errors", e.senderErrors)
	}
	if e.spansDropped > 0 {
		log.Printf("Warning: Total %d spans were dropped", e.spansDropped)
	}
	if e.metricsDropped > 0 {
		log.Printf("Warning: Total %d metrics were dropped", e.metricsDropped)
	}
}

func (e *Exporter) sendSelfHealth() {
	se := e.SenderErrors()
	md := e.MetricsDropped()
	sd := e.SpansDropped()

	var err error
	if se > 0 {
		err = e.sender.SendMetric(senderErrorsReportName,
			float64(se),
			time.Now().UnixNano()/nanoToMillis,
			e.Source, e.appMap,
		)
	}
	if err == nil && md > 0 {
		err = e.sender.SendMetric(metDropReportName,
			float64(md),
			time.Now().UnixNano()/nanoToMillis,
			e.Source, e.appMap,
		)
	}
	if err == nil && sd > 0 {
		err = e.sender.SendMetric(spanDropReportName,
			float64(sd),
			time.Now().UnixNano()/nanoToMillis,
			e.Source, e.appMap,
		)
	}

	if err != nil {
		e.logError("Report SelfHealth failed:", err)
	}
}

// SpansDropped counts spans dropped
// when exporter queue is full
func (e *Exporter) SpansDropped() uint64 {
	return atomic.LoadUint64(&e.spansDropped)
}

// SpansDropped counts metrics dropped
// when exporter queue is full
func (e *Exporter) MetricsDropped() uint64 {
	return atomic.LoadUint64(&e.metricsDropped)
}

// SenderErrors counts Sender errors
func (e *Exporter) SenderErrors() uint64 {
	return atomic.LoadUint64(&e.senderErrors)
}
