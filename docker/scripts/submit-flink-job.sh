#!/usr/bin/env bash
set -euo pipefail

APP_HOME="${APP_HOME:-/opt/app}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
APP_JAR_NAME="${APP_JAR_NAME:-flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"

DEFAULT_CONF_FILE="${APP_HOME}/conf/flink-job.conf"
if [[ ! -f "${DEFAULT_CONF_FILE}" && -f "${WORKSPACE_DIR}/docker/conf/flink-job.conf" ]]; then
  DEFAULT_CONF_FILE="${WORKSPACE_DIR}/docker/conf/flink-job.conf"
fi

CONF_FILE="${CONF_FILE:-${DEFAULT_CONF_FILE}}"
if [[ -f "${CONF_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONF_FILE}"
  echo "[submit-flink-job] loaded config: ${CONF_FILE}"
fi

DEFAULT_JAR_PATH="${APP_HOME}/lib/${APP_JAR_NAME}"
if [[ ! -f "${DEFAULT_JAR_PATH}" && -f "${WORKSPACE_DIR}/target/${APP_JAR_NAME}" ]]; then
  DEFAULT_JAR_PATH="${WORKSPACE_DIR}/target/${APP_JAR_NAME}"
fi

JAR_PATH="${JAR_PATH:-${DEFAULT_JAR_PATH}}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-flink-detection-group}"
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
FLINK_TARGET="${FLINK_TARGET:-jobmanager:8081}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[submit-flink-job] jar not found at ${JAR_PATH}."
  echo "[submit-flink-job] build the project first or use a prebuilt app image."
  exit 1
fi

echo "[submit-flink-job] submitting Flink job ..."
CMD=(flink run
  -m "${FLINK_TARGET}"
  -d
  "${JAR_PATH}"
  --kafka.bootstrap.servers "${KAFKA_BOOTSTRAP}"
  --kafka.group.id "${KAFKA_GROUP_ID}"
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
