#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"

cd "${WORKSPACE_DIR}"
echo "[build-jar] packaging project in ${WORKSPACE_DIR} ..."
mvn -q -DskipTests clean package
echo "[build-jar] done"
