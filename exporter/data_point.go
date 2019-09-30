package exporter

import (
	"encoding/json"
)

type DataPoint struct {
	Metric      string
	Labels      map[string]string
	Value       float64
	Description string
}

func (p *DataPoint) MarshalJSON() ([]byte, error) {
	ss := make(map[string]interface{}, len(p.Labels)+2)
	for k, v := range p.Labels {
		ss[k] = v
	}
	ss["metric"] = p.Metric
	ss["value"] = p.Value
	return json.Marshal(ss)
}
