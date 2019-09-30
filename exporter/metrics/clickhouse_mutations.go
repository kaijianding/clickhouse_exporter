package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseMutations struct {
	usedMetrics map[string]bool
	query       string
}

func NewClickhouseMutations(usedMetrics map[string]bool) *ClickhouseMutations {
	return &ClickhouseMutations{
		usedMetrics: usedMetrics,
		query:       "select database, table, count() as mutations, sum(parts_to_do) as parts_to_do from system.mutations where is_done = 0 group by database, table",
	}
}

func (c *ClickhouseMutations) GetCurrentQuery() *string {
	return &c.query
}

func (c *ClickhouseMutations) GetExpectedResultSize() int {
	return 4
}

func (c *ClickhouseMutations) Collect(reporter exporter.Reporter, values []string) error {
	database := strings.TrimSpace(values[0])
	table := strings.TrimSpace(values[1])

	mutations, err := strconv.Atoi(strings.TrimSpace(values[2]))
	if err != nil {
		return err
	}

	partsToDo, err := strconv.Atoi(strings.TrimSpace(values[3]))
	if err != nil {
		return err
	}

	if c.usedMetrics == nil || c.usedMetrics["TableMutationsCount"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TableMutationsCount",
			Labels: map[string]string{
				"database": database,
				"table":    table,
			},
			Value:       float64(mutations),
			Description: "Number of mutations of the table",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["TableMutationsParts"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TableMutationsParts",
			Labels: map[string]string{
				"database": database,
				"table":    table,
			},
			Value:       float64(partsToDo),
			Description: "Number of pending mutation parts to do of the table",
		})
	}
	return nil
}
