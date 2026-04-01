#!/usr/bin/env bash
set -euo pipefail

JAR_PATH="${JAR_PATH:-/workspace/target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BEHAVIOR_TOPIC="${BEHAVIOR_TOPIC:-user-behavior}"
RULE_TOPIC="${RULE_TOPIC:-risk-rules}"
ALERT_TOPIC="${ALERT_TOPIC:-alerts}"
PARALLELISM="${PARALLELISM:-2}"
ENABLE_KAFKA_SINK="${ENABLE_KAFKA_SINK:-true}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[submit-flink-job] jar not found, build first in runner container."
  exit 1
fi

echo "[submit-flink-job] submitting Flink job ..."
flink run \
  -m jobmanager:8081 \
  -d \
  "${JAR_PATH}" \
  --kafka.bootstrap.servers "${KAFKA_BOOTSTRAP}" \
  --kafka.behavior.topic "${BEHAVIOR_TOPIC}" \
  --kafka.rule.topic "${RULE_TOPIC}" \
  --kafka.alert.topic "${ALERT_TOPIC}" \
  --kafka.sink.enabled "${ENABLE_KAFKA_SINK}" \
  --parallelism "${PARALLELISM}"

echo "[submit-flink-job] submitted"
