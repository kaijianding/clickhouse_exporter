package metrics

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
)

type Metrics interface {
	GetCurrentQuery() *string
	/*
		return how many columns in the result, it is used to verify
	*/
	GetExpectedResultSize() int
	Collect(reporter exporter.Reporter, values []string) error
}
