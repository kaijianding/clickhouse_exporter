package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseEvents struct {
	usedMetrics map[string]bool
	query       string
}

func NewClickhouseEvents(usedMetrics map[string]bool) *ClickhouseEvents {
	return &ClickhouseEvents{
		usedMetrics: usedMetrics,
		query:       "select event, value from system.events",
	}
}

func (c *ClickhouseEvents) GetCurrentQuery() *string {
	return &c.query
}

func (c *ClickhouseEvents) GetExpectedResultSize() int {
	return 2
}

func (c *ClickhouseEvents) Collect(reporter exporter.Reporter, values []string) error {
	k := strings.TrimSpace(values[0])
	if c.usedMetrics != nil && !c.usedMetrics[k] {
		return nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(values[1]))
	if err != nil {
		return err
	}

	reporter.Const(&exporter.DataPoint{
		Metric:      k + "Total",
		Labels:      nil,
		Value:       float64(v),
		Description: "Number of " + k + " total processed",
	})
	return nil
}
