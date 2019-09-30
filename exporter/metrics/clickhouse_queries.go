package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseQueries struct {
	usedMetrics map[string]bool
	baseQuery   string
}

func NewClickhouseQueries(usedMetrics map[string]bool, collectIntervalInSecond int) *ClickhouseQueries {
	return &ClickhouseQueries{
		usedMetrics: usedMetrics,
		baseQuery: "select initial_user as user,avg(query_duration_ms) as avg_ms,max(query_duration_ms) as max_ms,count(*) as query_count from system.query_log" +
			" where is_initial_query=1 and written_rows=0 and type=2 and query_duration_ms>0 and initial_user != 'default' " +
			"and event_time>addSeconds(now(), -" + strconv.Itoa(collectIntervalInSecond) + ") group by initial_user",
	}
}

func (c *ClickhouseQueries) GetCurrentQuery() *string {
	return &c.baseQuery
}

func (c *ClickhouseQueries) GetExpectedResultSize() int {
	return 4
}

func (c *ClickhouseQueries) Collect(reporter exporter.Reporter, values []string) error {
	user := strings.TrimSpace(values[0])

	avgMs, err := strconv.ParseFloat(strings.TrimSpace(values[1]), 32)
	if err != nil {
		return err
	}

	maxMs, err := strconv.ParseFloat(strings.TrimSpace(values[2]), 32)
	if err != nil {
		return err
	}

	count, err := strconv.Atoi(strings.TrimSpace(values[3]))
	if err != nil {
		return err
	}

	if c.usedMetrics != nil && c.usedMetrics["AverageQueryTime"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "AverageQueryTime",
			Labels: map[string]string{
				"user": user,
			},
			Value:       avgMs,
			Description: "Average query time of mutations of the user",
		})
	}

	if c.usedMetrics != nil && c.usedMetrics["MaxQueryTime"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "MaxQueryTime",
			Labels: map[string]string{
				"user": user,
			},
			Value:       maxMs,
			Description: "Max query time of mutations of the user",
		})
	}

	if c.usedMetrics != nil && c.usedMetrics["QueryCount"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "QueryCount",
			Labels: map[string]string{
				"user": user,
			},
			Value:       float64(count),
			Description: "Query count of mutations of the user",
		})
	}
	return nil
}
