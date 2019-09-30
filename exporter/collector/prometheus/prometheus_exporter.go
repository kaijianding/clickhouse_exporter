package prometheus

import (
	"github.com/kaijianding/clickhouse_exporter/exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
)

type Exporter struct {
	listeningAddress  string
	metricsEndpoint   string
	clickhouseMetrics *collector.ClickhouseMetrics
}

func NewPrometheusExporter(
	listeningAddress string,
	metricsEndpoint string,
	clickhouseMetrics *collector.ClickhouseMetrics,
) *Exporter {
	return &Exporter{
		listeningAddress:  listeningAddress,
		metricsEndpoint:   metricsEndpoint,
		clickhouseMetrics: clickhouseMetrics,
	}
}

func (e *Exporter) Start() {
	go func() {
		c := NewPrometheusCollector(e.clickhouseMetrics)
		prometheus.MustRegister(c)

		log.Printf("Starting Server: %s", e.listeningAddress)
		http.Handle(e.metricsEndpoint, promhttp.Handler())
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`<html>
			<head><title>Clickhouse Collector</title></head>
			<body>
			<h1>Clickhouse Collector</h1>
			<p><a href="` + e.metricsEndpoint + `">Metrics</a></p>
			</body>
			</html>`))
		})

		log.Fatal(http.ListenAndServe(e.listeningAddress, nil))
	}()
}
