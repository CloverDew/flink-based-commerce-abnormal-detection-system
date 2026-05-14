#!/usr/bin/env bash
set -euo pipefail

APP_HOME="${APP_HOME:-/opt/app}"
WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"
APP_JAR_NAME="${APP_JAR_NAME:-flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar}"

JAR_PATH="${JAR_PATH:-${APP_HOME}/lib/${APP_JAR_NAME}}"
if [[ ! -f "${JAR_PATH}" ]]; then
  JAR_PATH="${WORKSPACE_DIR}/target/${APP_JAR_NAME}"
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  BUILD_SCRIPT="${WORKSPACE_DIR}/docker/scripts/build-jar.sh"
  if command -v mvn >/dev/null 2>&1 && [[ -x "${BUILD_SCRIPT}" ]]; then
    echo "[run-e2e] building jar ..."
    bash "${BUILD_SCRIPT}"
  else
    echo "[run-e2e] jar not found and workspace build is unavailable."
    exit 1
  fi
else
  echo "[run-e2e] using existing jar: ${JAR_PATH}"
fi

SUBMIT_SCRIPT="${APP_HOME}/scripts/submit-flink-job.sh"
if [[ ! -x "${SUBMIT_SCRIPT}" && -x "${WORKSPACE_DIR}/docker/scripts/submit-flink-job.sh" ]]; then
  SUBMIT_SCRIPT="${WORKSPACE_DIR}/docker/scripts/submit-flink-job.sh"
fi

BOOTSTRAP_SCRIPT="${APP_HOME}/scripts/bootstrap-kaggle.sh"
if [[ ! -x "${BOOTSTRAP_SCRIPT}" && -x "${WORKSPACE_DIR}/docker/scripts/bootstrap-kaggle.sh" ]]; then
  BOOTSTRAP_SCRIPT="${WORKSPACE_DIR}/docker/scripts/bootstrap-kaggle.sh"
fi

echo "[run-e2e] submit flink job ..."
bash "${SUBMIT_SCRIPT}"

echo "[run-e2e] bootstrap kaggle data and rules ..."
bash "${BOOTSTRAP_SCRIPT}"

echo "[run-e2e] completed"
