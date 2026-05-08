#!/usr/bin/env bash
set -euo pipefail

CONF_FILE="${CONF_FILE:-/workspace/docker/conf/flink-job.conf}"
if [[ -f "${CONF_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONF_FILE}"
  echo "[submit-flink-job] loaded config: ${CONF_FILE}"
fi

JAR_PATH="${JAR_PATH:-/workspace/target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BEHAVIOR_TOPIC="${BEHAVIOR_TOPIC:-user-behavior}"
RULE_TOPIC="${RULE_TOPIC:-risk-rules}"
ALERT_TOPIC="${ALERT_TOPIC:-alerts}"
PARALLELISM="${PARALLELISM:-2}"
ENABLE_KAFKA_SINK="${ENABLE_KAFKA_SINK:-true}"
STATE_BACKEND="${STATE_BACKEND:-hashmap}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-60000}"
CHECKPOINT_TIMEOUT_MS="${CHECKPOINT_TIMEOUT_MS:-120000}"
CHECKPOINT_MIN_PAUSE_MS="${CHECKPOINT_MIN_PAUSE_MS:-30000}"
CHECKPOINT_MAX_CONCURRENT="${CHECKPOINT_MAX_CONCURRENT:-1}"
CHECKPOINT_TOLERABLE_FAILURES="${CHECKPOINT_TOLERABLE_FAILURES:-3}"
CHECKPOINT_UNALIGNED_ENABLED="${CHECKPOINT_UNALIGNED_ENABLED:-false}"
CHECKPOINT_EXTERNALIZED_RETAINED="${CHECKPOINT_EXTERNALIZED_RETAINED:-true}"
CHECKPOINT_STORAGE="${CHECKPOINT_STORAGE:-}"
DEBUG_MATCHED_PRINT="${DEBUG_MATCHED_PRINT:-false}"
DEBUG_ALERT_PRINT="${DEBUG_ALERT_PRINT:-false}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[submit-flink-job] jar not found, build first in runner container."
  exit 1
fi

echo "[submit-flink-job] submitting Flink job ..."
CMD=(flink run
  -m jobmanager:8081
  -d
  "${JAR_PATH}"
  --kafka.bootstrap.servers "${KAFKA_BOOTSTRAP}"
  --kafka.behavior.topic "${BEHAVIOR_TOPIC}"
  --kafka.rule.topic "${RULE_TOPIC}"
  --kafka.alert.topic "${ALERT_TOPIC}"
  --kafka.sink.enabled "${ENABLE_KAFKA_SINK}"
  --parallelism "${PARALLELISM}"
  --state.backend "${STATE_BACKEND}"
  --checkpoint.interval.ms "${CHECKPOINT_INTERVAL_MS}"
  --checkpoint.timeout.ms "${CHECKPOINT_TIMEOUT_MS}"
  --checkpoint.min.pause.ms "${CHECKPOINT_MIN_PAUSE_MS}"
  --checkpoint.max.concurrent "${CHECKPOINT_MAX_CONCURRENT}"
  --checkpoint.tolerable.failures "${CHECKPOINT_TOLERABLE_FAILURES}"
  --checkpoint.unaligned.enabled "${CHECKPOINT_UNALIGNED_ENABLED}"
  --checkpoint.externalized.retained "${CHECKPOINT_EXTERNALIZED_RETAINED}"
  --debug.matched.print "${DEBUG_MATCHED_PRINT}"
  --debug.alert.print "${DEBUG_ALERT_PRINT}")

if [[ -n "${CHECKPOINT_STORAGE}" ]]; then
  CMD+=(--checkpoint.storage "${CHECKPOINT_STORAGE}")
fi

printf '[submit-flink-job] command: %q ' "${CMD[@]}"
printf '\n'

"${CMD[@]}"

echo "[submit-flink-job] submitted"
