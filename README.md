# Clickhouse Exporter for Prometheus and Kafka

This is inspired by http://github.com/f1yegor/clickhouse_exporter  

Differences are:
1. many refactor for better readable code
2. kafka support is added
3. query time metrics are added
4. useless metrics can be removed by setting environment variable ```CLICKHOUSE_USED_METRICS``` as a comma separated string

Prometheus support is turned off by default, use -enable_prometheus=true to turn it on

To enable kafka support, please set -kafka.bootstrap.servers $servers -kafka.topic $topic.  
If kafka sasl is enabled, please set -kafka.sasl=true and set environment variables 
```
KAFKA_USER
KAFKA_PASSWORD
```

To run it:

```bash
./clickhouse_exporter [flags]
```

Help on flags:
```bash
./clickhouse_exporter --help
```

Credentials(if not default):

via environment variables
```
CLICKHOUSE_USER
CLICKHOUSE_PASSWORD
```

## Sample dashboard
Grafana dashboard could be a start for inspiration https://grafana.net/dashboards/882
