package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseParts struct {
	usedMetrics map[string]bool
	query       string
}

func NewClickhouseParts(usedMetrics map[string]bool) *ClickhouseParts {
	return &ClickhouseParts{
		usedMetrics: usedMetrics,
		query:       "select database, table, sum(bytes) as bytes, count() as parts, sum(rows) as rows from system.parts where active = 1 group by database, table",
	}
}

func (c *ClickhouseParts) GetCurrentQuery() *string {
	return &c.query
}

func (c *ClickhouseParts) GetExpectedResultSize() int {
	return 5
}

func (c *ClickhouseParts) Collect(reporter exporter.Reporter, values []string) error {
	database := strings.TrimSpace(values[0])
	table := strings.TrimSpace(values[1])

	bytes, err := strconv.Atoi(strings.TrimSpace(values[2]))
	if err != nil {
		return err
	}

	count, err := strconv.Atoi(strings.TrimSpace(values[3]))
	if err != nil {
		return err
	}

	rows, err := strconv.Atoi(strings.TrimSpace(values[4]))
	if err != nil {
		return err
	}

	if c.usedMetrics == nil || c.usedMetrics["TableBytes"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TableBytes",
			Labels: map[string]string{
				"database": database,
				"table":    table,
			},
			Value:       float64(bytes),
			Description: "Table size in bytes",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["TablePartsCount"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TablePartsCount",
			Labels: map[string]string{
				"database": database,
				"table":    table,
			},
			Value:       float64(count),
			Description: "Number of parts of the table",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["TableRows"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TableRows",
			Labels: map[string]string{
				"database": database,
				"table":    table,
			},
			Value:       float64(rows),
			Description: "Number of rows in the table",
		})
	}
	return nil
}
