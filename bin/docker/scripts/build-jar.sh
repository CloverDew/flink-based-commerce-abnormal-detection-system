#!/usr/bin/env bash
set -euo pipefail

cd /workspace
echo "[build-jar] packaging project ..."
mvn -q -DskipTests clean package
echo "[build-jar] done"
