package exporter

type Reporter interface {
	Gauge(dataPoint *DataPoint)
	Const(dataPoint *DataPoint)
	Counter(dataPoint *DataPoint)
}
