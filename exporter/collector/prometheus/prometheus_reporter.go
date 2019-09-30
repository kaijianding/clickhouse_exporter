package prometheus

import (
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"unicode"
)

const (
	namespace = "clickhouse"
)

type Reporter struct {
	ch chan<- prometheus.Metric
}

func NewPrometheusReporter(ch chan<- prometheus.Metric) *Reporter {
	return &Reporter{ch: ch}
}

func (p *Reporter) Gauge(dataPoint *exporter.DataPoint) {
	keys := p.mapKeys(dataPoint.Labels)
	newPartsMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      p.metricName(dataPoint.Metric),
		Help:      dataPoint.Description,
	}, keys).With(dataPoint.Labels)
	newPartsMetric.Set(dataPoint.Value)
	newPartsMetric.Collect(p.ch)
}

func (p *Reporter) Const(dataPoint *exporter.DataPoint) {
	newMetric, _ := prometheus.NewConstMetric(
		prometheus.NewDesc(
			namespace+"_"+p.metricName(dataPoint.Metric),
			dataPoint.Description, []string{}, nil),
		prometheus.CounterValue, dataPoint.Value)
	p.ch <- newMetric
}

func (p *Reporter) Counter(dataPoint *exporter.DataPoint) {
	prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      p.metricName(dataPoint.Metric),
		Help:      dataPoint.Description,
	}).Collect(p.ch)
}

func (p *Reporter) mapKeys(m map[string]string) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func (p *Reporter) metricName(in string) string {
	out := p.toSnake(in)
	return strings.Replace(out, ".", "_", -1)
}

// toSnake convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func (p *Reporter) toSnake(in string) string {
	runes := []rune(in)
	length := len(runes)

	var out []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < length && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}
