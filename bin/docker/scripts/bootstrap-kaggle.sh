#!/usr/bin/env bash
set -euo pipefail

cd /workspace

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
BEHAVIOR_TOPIC="${BEHAVIOR_TOPIC:-user-behavior}"
RULE_TOPIC="${RULE_TOPIC:-risk-rules}"
PROFILE="${PROFILE:-multi_category}"
RULE_VERSION="${RULE_VERSION:-1}"
INPUT_CSV="${INPUT_CSV:-/workspace/data/kaggle-events.csv}"
RULE_FILE="${RULE_FILE:-/workspace/samples/generated-risk-rules.json}"
MAX_ROWS="${MAX_ROWS:--1}"
JAR_PATH="${JAR_PATH:-/workspace/target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "[bootstrap-kaggle] jar not found, building first ..."
  /workspace/docker/scripts/build-jar.sh
fi

/workspace/docker/scripts/wait-for-port.sh kafka 9092 180

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
