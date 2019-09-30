package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseAsyncMetrics struct {
	usedMetrics map[string]bool
	query       string
}

func NewClickhouseAsyncMetrics(usedMetrics map[string]bool) *ClickhouseAsyncMetrics {
	return &ClickhouseAsyncMetrics{
		usedMetrics: usedMetrics,
		query:       "select metric, value from system.asynchronous_metrics",
	}
}

func (c *ClickhouseAsyncMetrics) GetCurrentQuery() *string {
	return &c.query
}

func (c *ClickhouseAsyncMetrics) GetExpectedResultSize() int {
	return 2
}

func (c *ClickhouseAsyncMetrics) Collect(reporter exporter.Reporter, values []string) error {
	k := strings.TrimSpace(values[0])
	if c.usedMetrics != nil && !c.usedMetrics[k] {
		return nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(values[1]))
	if err != nil {
		return err
	}

	reporter.Gauge(&exporter.DataPoint{
		Metric:      k,
		Value:       float64(v),
		Description: "Number of " + k + " async processed",
	})
	return nil
}
