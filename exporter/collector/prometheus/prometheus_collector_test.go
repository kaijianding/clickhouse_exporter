package prometheus

import (
	"github.com/kaijianding/clickhouse_exporter/exporter/collector"
	"net/url"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestScrape(t *testing.T) {
	clickhouseUrl, err := url.Parse("http://localhost:8123")
	if err != nil {
		t.Fatal(err)
	}
	uri := clickhouseUrl.String()
	clickhouseMetrics := collector.NewClickhouseMetrics(
		collector.NewClickhouseClient(&uri, false, "", ""), 10, "",
	)
	exporter := NewPrometheusCollector(clickhouseMetrics)

	t.Run("Describe", func(t *testing.T) {
		ch := make(chan *prometheus.Desc)
		go func() {
			exporter.Describe(ch)
			close(ch)
		}()

		for range ch {
		}
	})

	t.Run("Collect", func(t *testing.T) {
		ch := make(chan prometheus.Metric)
		go func() {
			exporter.Collect(ch)
			close(ch)
		}()

		for range ch {
		}
	})
}
