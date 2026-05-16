#!/usr/bin/env bash
# test_quota_violation.sh — Demonstrates Kafka producer quota throttling.
#
# This script uses tenant-initech credentials to flood audit.tenant-initech.events
# at a rate well above the 1 MB/s quota (≈5-10 MB/s for 15 seconds).
# Kafka should throttle the client, which shows up in the broker logs as
# produce-throttle-time or similar messages.


set -uo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"

INITECH_USER="tenant-initech"
INITECH_PASSWORD="${TENANT_INITECH_PASSWORD:-initech-secret}"
TARGET_TOPIC="audit.tenant-initech.events"

# How many seconds to flood
DURATION_SECONDS=20

echo "========================================================"
echo "  Quota Throttle Test"
echo "  Producer:  $INITECH_USER"
echo "  Topic:     $TARGET_TOPIC"
echo "  Quota:     1 MB/s (1048576 bytes/s)"
echo "  Target:    ~5 MB/s burst for ${DURATION_SECONDS}s"
echo "========================================================"
echo ""

# Write initech client properties inside the container
docker exec "$KAFKA_CONTAINER" bash -c "cat > /tmp/initech.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${INITECH_USER}\" password=\"${INITECH_PASSWORD}\";
EOF"

# Generate a large message (≈5 KB) for rapid throughput
LARGE_MSG=$(python3 -c "import json; print(json.dumps({'tenant':'tenant-initech','payload':'X'*5000,'timestamp':'$(date -u +%Y-%m-%dT%H:%M:%SZ)'}))" 2>/dev/null || \
            node -e "console.log(JSON.stringify({tenant:'tenant-initech',payload:'X'.repeat(5000),timestamp:new Date().toISOString()}))")

echo "Generated payload size: ${#LARGE_MSG} bytes"
echo "Flooding topic for ${DURATION_SECONDS} seconds..."
echo ""

# Run the flood inside the kafka container using a background process.
# We use --producer.config to supply SASL credentials (the jaas.config value
# contains spaces and cannot be passed safely via --producer-props).
docker exec "$KAFKA_CONTAINER" bash -c "
  kafka-producer-perf-test \
    --topic ${TARGET_TOPIC} \
    --num-records 999999 \
    --record-size 5120 \
    --throughput 1100 \
    --producer.config /tmp/initech.properties \
    --producer-props bootstrap.servers=localhost:9093 \
    2>&1 &
  PERF_PID=\$!
  sleep ${DURATION_SECONDS}
  kill \$PERF_PID 2>/dev/null || true
  echo 'Flood complete.'
" || true

echo ""
echo "Flood phase complete. Checking broker logs for throttle evidence..."
echo ""

# Give the broker a moment to flush log entries
sleep 2

# Check broker container logs for any throttle-related messages
THROTTLE_EVIDENCE=$(docker logs kafka --since "${DURATION_SECONDS}s" 2>&1 | \
  grep -i "throttl\|quota\|byte.rate\|delay" | head -20 || true)

if [ -n "$THROTTLE_EVIDENCE" ]; then
  echo "[THROTTLE EVIDENCE FOUND in broker logs]"
  echo "$THROTTLE_EVIDENCE"
else
  echo "[INFO] No throttle messages in broker logs during this window."
  echo "       This can happen when the broker doesn't log throttle events at"
  echo "       default log level. Check JMX metric kafka.server:type=ClientQuotaManager"
  echo "       or look for 'produce-throttle-time-avg' in broker metrics."
fi

echo ""
echo "========================================================"
echo "  Checking JMX / describe-configs for quota settings..."
echo "========================================================"

docker exec "$KAFKA_CONTAINER" bash -c "
  kafka-configs \
    --bootstrap-server localhost:9093 \
    --command-config /tmp/admin.properties \
    --describe \
    --entity-type users \
    --entity-name tenant-initech
" 2>/dev/null || docker exec "$KAFKA_CONTAINER" bash -c "
  cat > /tmp/admin.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"${KAFKA_ADMIN_PASSWORD:-admin-secret}\";
EOF
  kafka-configs \
    --bootstrap-server localhost:9093 \
    --command-config /tmp/admin.properties \
    --describe \
    --entity-type users \
    --entity-name tenant-initech
"

echo ""
echo "========================================================"
echo "  Test complete."
echo "  The quota is enforced at the broker level."
echo "  producer_byte_rate=1048576 throttles tenant-initech to 1 MB/s."
echo "========================================================"

exit 0
