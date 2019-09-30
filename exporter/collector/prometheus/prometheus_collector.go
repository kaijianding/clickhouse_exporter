package prometheus

import (
	"github.com/kaijianding/clickhouse_exporter/exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
)

// Collector collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Collector struct {
	clickhouseMetrics *collector.ClickhouseMetrics
}

func NewPrometheusCollector(clickhouseMetrics *collector.ClickhouseMetrics) *Collector {
	return &Collector{
		clickhouseMetrics: clickhouseMetrics,
	}
}

// Describe describes all the metrics ever exported by the clickhouse exporter. It
// implements prometheus.Metrics.
func (e *Collector) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from clickhouse. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

// Collect fetches the stats from configured clickhouse location and delivers them
// as Prometheus metrics. It implements prometheus.Metrics.
func (e *Collector) Collect(ch chan<- prometheus.Metric) {
	e.clickhouseMetrics.Collect(NewPrometheusReporter(ch))
}

// check interface
var _ prometheus.Collector = (*Collector)(nil)
