package wavefront

import (
	"log"
	"sync/atomic"

	"github.com/wavefronthq/wavefront-sdk-go/histogram"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	unitTagKey = "unit"
)

// view conversion
func (e *Exporter) processView(vd *view.Data) {
	var cmd SendCmd

	// Custom Tags & App Tags
	appTags := e.appMap
	otherTags := make(map[string]string, 2+len(appTags))
	for k, v := range appTags {
		otherTags[k] = v
	}
	unit := vd.View.Measure.Unit()
	if unit != "" && unit != stats.UnitDimensionless {
		otherTags[unitTagKey] = unit
	}

	for _, row := range vd.Rows {
		pointTags := makePointTags(row.Tags, otherTags)
		timestamp := vd.End.UnixNano() / nanoToMillis

		switch agg := row.Data.(type) {
		case *view.CountData:
			value := float64(agg.Value)
			cmd = func() {
				defer e.semRelease()

				e.logError("Error sending metric:", e.sender.SendMetric(
					vd.View.Name,
					value, timestamp, e.Source,
					pointTags,
				))
			}

		case *view.LastValueData:
			value := agg.Value
			cmd = func() {
				defer e.semRelease()

				e.logError("Error sending metric:", e.sender.SendMetric(
					vd.View.Name,
					value, timestamp, e.Source,
					pointTags,
				))
			}

		case *view.SumData:
			value := agg.Value
			cmd = func() {
				defer e.semRelease()

				e.logError("Error sending metric:", e.sender.SendMetric(
					vd.View.Name,
					value, timestamp, e.Source,
					pointTags,
				))
			}

		case *view.DistributionData:
			centroids := makeCentroids(vd.View.Aggregation.Buckets, agg)
			cmd = func() {
				defer e.semRelease()

				e.logError("Error sending histogram:", e.sender.SendDistribution(
					vd.View.Name,
					centroids, e.Hgs,
					timestamp, e.Source,
					pointTags,
				))
			}

		default:
			log.Printf("Unsupported Aggregation type: %T", vd.View.Aggregation)
		}

		if !e.queueCmd(cmd) {
			atomic.AddUint64(&e.metricsDropped, 1)
		}
	}
}

func makePointTags(tags []tag.Tag, otherTags map[string]string) map[string]string {
	pointTags := make(map[string]string, len(tags)+len(otherTags))
	for _, t := range tags {
		pointTags[t.Key.Name()] = t.Value
	}
	for k, v := range otherTags { //otherTags first?
		pointTags[k] = v
	}
	return pointTags
}

func makeCentroids(bounds []float64, agg *view.DistributionData) []histogram.Centroid {
	counts := agg.CountPerBucket
	if len(bounds) != len(counts)-1 {
		log.Println("Error: Bounds and Bucket counts don't match")
		return nil
	}

	centroids := make([]histogram.Centroid, 0, len(counts))
	for i, c := range counts {
		var m float64
		switch {
		case c == 1:
			m = agg.ExemplarsPerBucket[i].Value
		case c > 1:
			switch i {
			case 0:
				m = mean2(agg.Min, float64(bounds[i]))
			case len(counts) - 1:
				m = mean2(float64(bounds[i-1]), agg.Max)
			default:
				m = mean2(float64(bounds[i-1]), float64(bounds[i]))
			}
		default:
			continue // Count is <= 0. Skip
		}
		centroids = append(centroids, histogram.Centroid{
			Value: m, Count: int(c), // overflow? if int is 32bit
		})
	}

	return centroids
}

func mean2(a, b float64) float64 {
	return (a + b) / 2.0
}
