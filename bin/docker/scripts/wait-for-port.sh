#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-9092}"
TIMEOUT="${3:-120}"

echo "Waiting for ${HOST}:${PORT} ..."
for ((i=0; i< TIMEOUT; i++)); do
  if bash -c "echo > /dev/tcp/${HOST}/${PORT}" >/dev/null 2>&1; then
    echo "Port ready: ${HOST}:${PORT}"
    exit 0
  fi
  sleep 1
done

echo "Timeout waiting for ${HOST}:${PORT}" >&2
exit 1
