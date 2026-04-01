#!/usr/bin/env bash
set -euo pipefail

echo "[run-e2e] building jar ..."
bash /workspace/docker/scripts/build-jar.sh

echo "[run-e2e] submit flink job ..."
bash /workspace/docker/scripts/submit-flink-job.sh

echo "[run-e2e] bootstrap kaggle data and rules ..."
bash /workspace/docker/scripts/bootstrap-kaggle.sh

echo "[run-e2e] completed"
