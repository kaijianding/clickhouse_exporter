package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"strconv"
	"strings"
)

type ClickhouseTableInserts struct {
	usedMetrics map[string]bool
	baseQuery   string
}

func NewClickhouseTableInserts(usedMetrics map[string]bool, collectIntervalInSecond int) *ClickhouseTableInserts {
	return &ClickhouseTableInserts{
		usedMetrics: usedMetrics,
		baseQuery: `select case when notEmpty(t1) then t1 else t2 end as table,initial_user,sum(written_rows) as written_rows,sum(written_bytes) as written_bytes from (
	select initial_user,written_rows,written_bytes,` +
			"extract(query,'insert into\\\\s(`?[\\\\w_-]+`?[.\\\\w_-`]*)') as t1,extract(query,'INSERT INTO\\\\s(`?[\\\\w_-]+`?[.\\\\w_-`]*)') as t2 " +
			`from system.query_log where written_rows>0 
			and event_time>addSeconds(now(), -` + strconv.Itoa(collectIntervalInSecond) + `)
	) group by table,initial_user order by table`,
	}
}

func (c *ClickhouseTableInserts) GetCurrentQuery() *string {
	return &c.baseQuery
}

func (c *ClickhouseTableInserts) GetExpectedResultSize() int {
	return 4
}

func (c *ClickhouseTableInserts) Collect(reporter exporter.Reporter, values []string) error {
	database := ""
	table := strings.TrimSpace(values[0])
	arr := strings.Split(table, ".")
	if len(arr) > 1 {
		database = strings.ReplaceAll(arr[0], "`", "")
		table = arr[1]
	}
	table = strings.ReplaceAll(table, "`", "")
	user := strings.TrimSpace(values[1])

	rows, err := strconv.Atoi(strings.TrimSpace(values[2]))
	if err != nil {
		return err
	}

	bytes, err := strconv.Atoi(strings.TrimSpace(values[3]))
	if err != nil {
		return err
	}

	if c.usedMetrics == nil || c.usedMetrics["InsertedRows"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "InsertedRows",
			Labels: map[string]string{
				"user":     user,
				"database": database,
				"table":    table,
			},
			Value:       float64(rows),
			Description: "Inserted rows of the table",
		})
	}

	if c.usedMetrics == nil || c.usedMetrics["InsertedBytes"] {
		reporter.Gauge(&exporter.DataPoint{
			Metric: "InsertedBytes",
			Labels: map[string]string{
				"user":     user,
				"database": database,
				"table":    table,
			},
			Value:       float64(bytes),
			Description: "Inserted bytes of the table",
		})
	}
	return nil
}
