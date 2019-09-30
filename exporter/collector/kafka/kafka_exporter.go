package kafka

import (
	"github.com/kaijianding/clickhouse_exporter/exporter/collector"
	"time"
)

// Exporter collects clickhouse stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	bootstrapServers        []string
	topic                   *string
	kafkaSasl               bool
	kafkaUser               *string
	kafkaPassword           *string
	clickhouseMetrics       *collector.ClickhouseMetrics
	collectIntervalInSecond int
}

func NewKafkaExporter(
	bootstrapServers []string,
	topic *string,
	kafkaSasl bool,
	kafkaUser *string,
	kafkaPassword *string,
	clickhouseMetrics *collector.ClickhouseMetrics,
	collectIntervalInSecond int,
) *Exporter {
	return &Exporter{
		bootstrapServers:        bootstrapServers,
		topic:                   topic,
		kafkaSasl:               kafkaSasl,
		kafkaUser:               kafkaUser,
		kafkaPassword:           kafkaPassword,
		clickhouseMetrics:       clickhouseMetrics,
		collectIntervalInSecond: collectIntervalInSecond,
	}
}

func (e *Exporter) Start() {
	ticker := time.NewTicker(time.Duration(e.collectIntervalInSecond) * time.Second)
	r := NewKafkaReporter(e.bootstrapServers, e.topic, e.kafkaSasl, e.kafkaUser, e.kafkaPassword)
	go func() {
		for _ = range ticker.C {
			e.clickhouseMetrics.Collect(r)
		}
	}()
}
