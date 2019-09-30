package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseMetrics struct {
	usedMetrics map[string]bool
	query       string
}

func NewClickhouseMetrics(usedMetrics map[string]bool) *ClickhouseMetrics {
	return &ClickhouseMetrics{
		usedMetrics: usedMetrics,
		query:       "select metric, value from system.metrics",
	}
}

func (c *ClickhouseMetrics) GetCurrentQuery() *string {
	return &c.query
}

func (c *ClickhouseMetrics) GetExpectedResultSize() int {
	return 2
}

func (c *ClickhouseMetrics) Collect(reporter exporter.Reporter, values []string) error {
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
		Labels:      nil,
		Value:       float64(v),
		Description: "Number of " + k + " currently processed",
	})
	return nil
}
