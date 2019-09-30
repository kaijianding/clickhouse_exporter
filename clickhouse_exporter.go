package main

import (
	"flag"
	"github.com/kaijianding/clickhouse_exporter/exporter/collector"
	"github.com/kaijianding/clickhouse_exporter/exporter/collector/kafka"
	"github.com/kaijianding/clickhouse_exporter/exporter/collector/prometheus"
	"log"
	"net/url"
	"os"
	"strings"
)

var (
	enablePrometheus           = flag.Bool("enable_prometheus", false, "Enable prometheus exporter")
	prometheusListeningAddress = flag.String("telemetry.address", ":9116", "Address on which to expose metrics.")
	prometheusMetricsEndpoint  = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
	kafkaBootstrapServers      = flag.String("kafka.bootstrap.servers", "", "Kafka brokers.")
	kafkaTopic                 = flag.String("kafka.topic", "", "Kafka topic")
	kafkaSasl                  = flag.Bool("kafka.sasl", false, "Kafka enable sasl")
	clickhouseScrapeURI        = flag.String("scrape_uri", "http://localhost:8123/", "URI to clickhouse http endpoint")
	clickhouseScrapeInterval   = flag.Int("scrape_interval", 10, "Interval to scrap clickhouse")
	insecure                   = flag.Bool("insecure", true, "Ignore server certificate if using https")
	user                       = os.Getenv("CLICKHOUSE_USER")
	password                   = os.Getenv("CLICKHOUSE_PASSWORD")
	kafkaUser                  = os.Getenv("KAFKA_USER")
	kafkaPassword              = os.Getenv("KAFKA_PASSWORD")
	usedMetrics                = os.Getenv("CLICKHOUSE_USED_METRICS")
)

func main() {
	flag.Parse()

	_, err := url.Parse(*clickhouseScrapeURI)
	if err != nil {
		log.Fatal(err)
	}
	clickhouseMetrics := collector.NewClickhouseMetrics(
		collector.NewClickhouseClient(clickhouseScrapeURI, *insecure, user, password),
		*clickhouseScrapeInterval,
		usedMetrics,
	)
	if len(*kafkaBootstrapServers) > 0 && len(*kafkaTopic) > 0 {
		kafka.NewKafkaExporter(
			strings.Split(*kafkaBootstrapServers, ","),
			kafkaTopic,
			*kafkaSasl,
			&kafkaUser,
			&kafkaPassword,
			clickhouseMetrics,
			*clickhouseScrapeInterval,
		).Start()
	}
	if *enablePrometheus {
		prometheus.NewPrometheusExporter(*prometheusListeningAddress, *prometheusMetricsEndpoint, clickhouseMetrics).Start()
	}

	doneCh := make(chan struct{})
	// avoid exit
	<-doneCh
}
