package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseFailedQueries struct {
	usedMetrics map[string]bool
	baseQuery   string
}

func NewClickhouseFailedQueries(usedMetrics map[string]bool, collectIntervalInSecond int) *ClickhouseFailedQueries {
	return &ClickhouseFailedQueries{
		usedMetrics: usedMetrics,
		baseQuery: `select initial_user as user,avg(query_duration_ms) as avg_ms,max(query_duration_ms) as max_ms,count(*) as query_count from system.query_log
			 where is_initial_query=1 and written_rows=0 and read_rows>0 and type=4 and query_duration_ms>0 and initial_user != 'default'
			and position(lower(query), 'from')>0
			and event_time>addSeconds(now(), -` + strconv.Itoa(collectIntervalInSecond) + ") group by initial_user",
	}
}

func (c *ClickhouseFailedQueries) GetCurrentQuery() *string {
	return &c.baseQuery
}

func (c *ClickhouseFailedQueries) GetExpectedResultSize() int {
	return 4
}

func (c *ClickhouseFailedQueries) Collect(reporter exporter.Reporter, values []string) error {
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

	if c.usedMetrics == nil || c.usedMetrics["AverageFailedQueryTime"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "AverageFailedQueryTime",
			Labels: map[string]string{
				"user": user,
			},
			Value:       avgMs,
			Description: "Average failed query time of the user",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["MaxFailedQueryTime"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "MaxFailedQueryTime",
			Labels: map[string]string{
				"user": user,
			},
			Value:       maxMs,
			Description: "Max failed query time of the user",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["FailedQueryCount"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "FailedQueryCount",
			Labels: map[string]string{
				"user": user,
			},
			Value:       float64(count),
			Description: "Failed query count of the user",
		})
	}
	return nil
}
