package exporter

import (
	"sync/atomic"
)

type CollectFailReporter struct {
	failCount uint64
}

func (r *CollectFailReporter) Inc(reporter Reporter) {
	atomic.AddUint64(&r.failCount, 1)
	reporter.Counter(&DataPoint{
		Metric:      "exporterScrapeFailuresTotal",
		Value:       float64(r.failCount),
		Description: "Number of errors while scraping clickhouse.",
	})
}

func NewCollectFailReporter() *CollectFailReporter {
	return &CollectFailReporter{}
}
