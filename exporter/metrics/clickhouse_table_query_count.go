package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseTableQueryCount struct {
	usedMetrics map[string]bool
	baseQuery   string
}

func NewClickhouseTableQueryCount(usedMetrics map[string]bool, collectIntervalInSecond int) *ClickhouseTableQueryCount {
	return &ClickhouseTableQueryCount{
		usedMetrics: usedMetrics,
		baseQuery: `select t,initial_user,count(*) as c from (
	select arrayJoin(arrayDistinct(case when notEmpty(t1) then t1 else t2 end)) as t,initial_user from (
		select ` +
			"extractAll(query,'FROM\\\\s(`?[\\\\w_-]+`?[.\\\\w_-`]*)') as t1, " +
			"extractAll(query,'from\\\\s(`?[\\\\w_-]+`?[.\\\\w_-`]*)') as t2, " +
			`initial_user from system.query_log
		where is_initial_query=1 and written_rows=0 and read_rows>0 and query_duration_ms>0
        and position(lower(query), 'from')>0
		and event_time>addSeconds(now(), -` + strconv.Itoa(collectIntervalInSecond) + `)
	)
) where not startsWith(lower(t),'system') group by t,initial_user`,
	}
}

func (c *ClickhouseTableQueryCount) GetCurrentQuery() *string {
	return &c.baseQuery
}

func (c *ClickhouseTableQueryCount) GetExpectedResultSize() int {
	return 3
}

func (c *ClickhouseTableQueryCount) Collect(reporter exporter.Reporter, values []string) error {
	database := ""
	table := strings.TrimSpace(values[0])
	arr := strings.Split(table, ".")
	if len(arr) > 1 {
		database = strings.ReplaceAll(arr[0], "`", "")
		table = arr[1]
	}
	table = strings.ReplaceAll(table, "`", "")
	user := strings.TrimSpace(values[1])

	count, err := strconv.Atoi(strings.TrimSpace(values[2]))
	if err != nil {
		return err
	}

	if c.usedMetrics == nil || c.usedMetrics["TableQueryCount"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "TableQueryCount",
			Labels: map[string]string{
				"user":     user,
				"database": database,
				"table":    table,
			},
			Value:       float64(count),
			Description: "Query count of the table",
		})
	}
	return nil
}
