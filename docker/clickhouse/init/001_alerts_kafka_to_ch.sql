-- ClickHouse realtime dashboard storage
-- Ingest AlertEvent JSON from Kafka topic `alerts`, then materialize into MergeTree tables.

CREATE DATABASE IF NOT EXISTS risk;

-- 1) Kafka engine table (raw JSON).
CREATE TABLE IF NOT EXISTS risk.alerts_kafka_raw
(
  `value` String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'alerts',
  kafka_group_name = 'ch-alerts-consumer',
  kafka_format = 'LineAsString',
  kafka_num_consumers = 1;

-- 2) Parsed alerts (wide table for Grafana queries).
CREATE TABLE IF NOT EXISTS risk.alerts
(
  alertId String,
  ruleId String,
  ruleType LowCardinality(String),
  level LowCardinality(String),
  userId String,
  ip String,
  deviceId String,
  matchCount UInt32,
  riskScore Float64,
  alertTimestampMs Int64,
  firstEventTimestampMs Int64,
  lastEventTimestampMs Int64,
  message String,
  extra String,
  alertTime DateTime DEFAULT toDateTime(intDiv(alertTimestampMs, 1000))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(alertTime)
ORDER BY (alertTime, ruleType, ruleId, ip, userId);

-- 3) Materialized view: parse JSON into columns.
CREATE MATERIALIZED VIEW IF NOT EXISTS risk.alerts_mv
TO risk.alerts
AS
SELECT
  JSONExtractString(value, 'alertId') AS alertId,
  JSONExtractString(value, 'ruleId') AS ruleId,
  JSONExtractString(value, 'ruleType') AS ruleType,
  JSONExtractString(value, 'level') AS level,
  JSONExtractString(value, 'userId') AS userId,
  JSONExtractString(value, 'ip') AS ip,
  JSONExtractString(value, 'deviceId') AS deviceId,
  toUInt32OrZero(JSONExtractString(value, 'matchCount')) AS matchCount,
  toFloat64OrZero(JSONExtractString(value, 'riskScore')) AS riskScore,
  toInt64OrZero(JSONExtractString(value, 'alertTimestamp')) AS alertTimestampMs,
  toInt64OrZero(JSONExtractString(value, 'firstEventTimestamp')) AS firstEventTimestampMs,
  toInt64OrZero(JSONExtractString(value, 'lastEventTimestamp')) AS lastEventTimestampMs,
  JSONExtractString(value, 'message') AS message,
  JSONExtractString(value, 'extra') AS extra
FROM risk.alerts_kafka_raw;

-- 4) Aggregation table: 1-minute counts by type (fast dashboards).
CREATE TABLE IF NOT EXISTS risk.alerts_agg_1m
(
  bucket DateTime,
  ruleType LowCardinality(String),
  cnt UInt64,
  avgRiskScore Float64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket)
ORDER BY (bucket, ruleType);

CREATE MATERIALIZED VIEW IF NOT EXISTS risk.alerts_agg_1m_mv
TO risk.alerts_agg_1m
AS
SELECT
  toStartOfMinute(alertTime) AS bucket,
  ruleType,
  count() AS cnt,
  avg(riskScore) AS avgRiskScore
FROM risk.alerts
GROUP BY bucket, ruleType;

