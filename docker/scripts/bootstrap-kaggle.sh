#!/usr/bin/env bash
set -euo pipefail

APP_HOME="${APP_HOME:-/opt/app}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
APP_JAR_NAME="${APP_JAR_NAME:-flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"

DEFAULT_CONF_FILE="${APP_HOME}/conf/bootstrap.conf"
if [[ ! -f "${DEFAULT_CONF_FILE}" && -f "${WORKSPACE_DIR}/docker/conf/bootstrap.conf" ]]; then
  DEFAULT_CONF_FILE="${WORKSPACE_DIR}/docker/conf/bootstrap.conf"
fi

CONF_FILE="${CONF_FILE:-${DEFAULT_CONF_FILE}}"
if [[ -f "${CONF_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONF_FILE}"
  echo "[bootstrap-kaggle] loaded config: ${CONF_FILE}"
fi

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BEHAVIOR_TOPIC="${BEHAVIOR_TOPIC:-user-behavior}"
RULE_TOPIC="${RULE_TOPIC:-risk-rules}"
PROFILE="${PROFILE:-multi_category}"
RULE_VERSION="${RULE_VERSION:-1}"
INPUT_CSV="${INPUT_CSV:-${APP_HOME}/data/kaggle-events.csv}"
RULE_FILE="${RULE_FILE:-${APP_HOME}/samples/generated-risk-rules.json}"
MAX_ROWS="${MAX_ROWS:--1}"

DEFAULT_JAR_PATH="${APP_HOME}/lib/${APP_JAR_NAME}"
if [[ ! -f "${DEFAULT_JAR_PATH}" && -f "${WORKSPACE_DIR}/target/${APP_JAR_NAME}" ]]; then
  DEFAULT_JAR_PATH="${WORKSPACE_DIR}/target/${APP_JAR_NAME}"
fi
JAR_PATH="${JAR_PATH:-${DEFAULT_JAR_PATH}}"

WAIT_FOR_PORT_SCRIPT="${APP_HOME}/scripts/wait-for-port.sh"
if [[ ! -x "${WAIT_FOR_PORT_SCRIPT}" && -x "${WORKSPACE_DIR}/docker/scripts/wait-for-port.sh" ]]; then
  WAIT_FOR_PORT_SCRIPT="${WORKSPACE_DIR}/docker/scripts/wait-for-port.sh"
fi

BUILD_SCRIPT="${WORKSPACE_DIR}/docker/scripts/build-jar.sh"
if [[ ! -f "${JAR_PATH}" ]]; then
  if command -v mvn >/dev/null 2>&1 && [[ -x "${BUILD_SCRIPT}" ]]; then
    echo "[bootstrap-kaggle] jar not found, building from workspace ..."
    "${BUILD_SCRIPT}"
  else
    echo "[bootstrap-kaggle] jar not found at ${JAR_PATH}."
    echo "[bootstrap-kaggle] build the project first or use a prebuilt app image."
    exit 1
  fi
fi

if [[ ! -f "${INPUT_CSV}" ]]; then
  echo "[bootstrap-kaggle] input csv not found: ${INPUT_CSV}"
  exit 1
fi

mkdir -p "$(dirname "${RULE_FILE}")"

"${WAIT_FOR_PORT_SCRIPT}" kafka 9092 180

echo "[bootstrap-kaggle] generating rules profile=${PROFILE} version=${RULE_VERSION}"
java -cp "${JAR_PATH}" cn.edu.ustb.detection.tools.KaggleRuleTemplateGenerator \
  --profile "${PROFILE}" \
  --version "${RULE_VERSION}" \
  --output "${RULE_FILE}"

echo "[bootstrap-kaggle] publishing rules to topic=${RULE_TOPIC}"
java -cp "${JAR_PATH}" cn.edu.ustb.detection.tools.JsonFileKafkaPublisher \
  --input "${RULE_FILE}" \
  --kafka-bootstrap "${KAFKA_BOOTSTRAP}" \
  --kafka-topic "${RULE_TOPIC}" \
  --key-field ruleId

echo "[bootstrap-kaggle] sending user behavior from csv=${INPUT_CSV} to topic=${BEHAVIOR_TOPIC}"
java -cp "${JAR_PATH}" cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool \
  --input "${INPUT_CSV}" \
  --kafka-bootstrap "${KAFKA_BOOTSTRAP}" \
  --kafka-topic "${BEHAVIOR_TOPIC}" \
  --profile "${PROFILE}" \
  --max-rows "${MAX_ROWS}"

echo "[bootstrap-kaggle] done"
