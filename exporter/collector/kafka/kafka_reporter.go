package kafka

import (
	"context"
	"encoding/json"
	"github.com/kaijianding/clickhouse_exporter/exporter"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"log"
	"os"
	"strconv"
	"time"
)

type Reporter struct {
	w     *kafka.Writer
	host  *string
	topic *string
}

func NewKafkaReporter(
	bootstrapServers []string,
	topic *string,
	sasl bool,
	user *string,
	password *string,
) *Reporter {
	var dialer *kafka.Dialer = nil
	if sasl {
		dialer = &kafka.Dialer{
			Timeout:   5 * time.Second,
			DualStack: true,
			SASLMechanism: plain.Mechanism{
				Username: *user,
				Password: *password,
			},
		}
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Async:    true,
		Brokers:  bootstrapServers,
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
		Dialer:   dialer,
	})
	host, _ := os.Hostname()
	return &Reporter{
		w:     w,
		host:  &host,
		topic: topic,
	}
}

func (r *Reporter) write(dataPoint *exporter.DataPoint) {
	if dataPoint.Labels == nil {
		dataPoint.Labels = make(map[string]string)
	}
	dataPoint.Labels["hostname"] = *r.host
	dataPoint.Labels["eventTime"] = strconv.Itoa(int(time.Now().Unix()))
	bytes, err := json.Marshal(dataPoint)
	if err != nil {
		log.Println(err)
		return
	}
	err = r.w.WriteMessages(context.Background(), kafka.Message{
		Value: bytes,
	})
	if err != nil {
		log.Println(err)
	}
}

func (r *Reporter) Gauge(dataPoint *exporter.DataPoint) {
	r.write(dataPoint)
}

func (r *Reporter) Const(dataPoint *exporter.DataPoint) {
	r.write(dataPoint)
}

func (r *Reporter) Counter(dataPoint *exporter.DataPoint) {
	r.write(dataPoint)
}
