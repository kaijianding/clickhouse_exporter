package collector

import (
	"fmt"
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"github.com/kaijianding/clickhouse_exporter/exporter/metrics"
	"strings"
)

type ClickhouseMetrics struct {
	clickhouseClient *ClickhouseClient
	metrics          []metrics.Metrics
	scrapeFailures   *exporter.CollectFailReporter
}

func NewClickhouseMetrics(
	clickhouseClient *ClickhouseClient,
	collectIntervalInSecond int,
	usedMetrics string,
) *ClickhouseMetrics {
	var __usedMetrics map[string]bool = nil
	if len(usedMetrics) > 0 {
		__usedMetrics = make(map[string]bool)
		for _, m := range strings.Split(usedMetrics, ",") {
			__usedMetrics[strings.TrimSpace(m)] = true
		}
	}
	return &ClickhouseMetrics{
		clickhouseClient: clickhouseClient,
		scrapeFailures:   exporter.NewCollectFailReporter(),
		metrics: []metrics.Metrics{
			metrics.NewClickhouseMetrics(__usedMetrics),
			metrics.NewClickhouseAsyncMetrics(__usedMetrics),
			metrics.NewClickhouseEvents(__usedMetrics),
			metrics.NewClickhouseParts(__usedMetrics),
			metrics.NewClickhouseMutations(__usedMetrics),
			metrics.NewClickhouseQueries(__usedMetrics, collectIntervalInSecond),
			metrics.NewClickhouseFailedQueries(__usedMetrics, collectIntervalInSecond),
			metrics.NewClickhouseTableQueryCount(__usedMetrics, collectIntervalInSecond),
		},
	}
}

// Collect fetches the stats from configured clickhouse location and delivers to the reporter
func (m *ClickhouseMetrics) Collect(r exporter.Reporter) {
	for _, metric := range m.metrics {
		if err := m.collect(r, metric); err != nil {
			fmt.Printf("Error scraping clickhouse: %s\n", err)
			m.scrapeFailures.Inc(r)
		}
	}
}

func (m *ClickhouseMetrics) collect(reporter exporter.Reporter, metric metrics.Metrics) error {
	data, err := m.clickhouseClient.Request(metric.GetCurrentQuery())
	if err != nil {
		return err
	}
	// Parsing results
	lines := strings.Split(string(data), "\n")

	for i, line := range lines {
		values := strings.Fields(line)
		if len(values) == 0 {
			continue
		}
		if len(values) != metric.GetExpectedResultSize() {
			return fmt.Errorf("parse response: unexpected %d line: %s", i, line)
		}
		if err := metric.Collect(reporter, values); err != nil {
			return err
		}
	}
	return nil
}
