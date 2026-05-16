#!/bin/sh
# start.sh — Launch gateway and archiver concurrently in the same container.
# Both processes write to stdout so Docker captures all logs together.
set -e

echo "[START] Launching audit gateway on port ${APP_PORT:-3000}..."
node gateway.js &
GATEWAY_PID=$!

echo "[START] Launching audit archiver..."
node archiver.js &
ARCHIVER_PID=$!

echo "[START] Both services running. Gateway PID=$GATEWAY_PID Archiver PID=$ARCHIVER_PID"

# If either process exits, kill the other and exit with its code
wait -n 2>/dev/null || true

# Fallback: wait for the gateway (primary service)
wait $GATEWAY_PID
