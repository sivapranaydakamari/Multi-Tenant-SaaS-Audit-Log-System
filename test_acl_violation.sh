#!/usr/bin/env bash
# test_acl_violation.sh — Proves that Kafka ACLs block cross-tenant access.
#
# This script authenticates as tenant-globex and attempts to produce a message
# to audit.tenant-acme.events (which tenant-globex has NO write ACL for).
# Expected result: the attempt fails with a TopicAuthorizationException.
# The script exits with code 1 on failure, which is the expected outcome.

set -uo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"

# tenant-globex credentials — set by provision.sh
GLOBEX_USER="tenant-globex"
GLOBEX_PASSWORD="${TENANT_GLOBEX_PASSWORD:-globex-secret}"

# The topic that belongs to tenant-ACME (globex has NO ACL here)
TARGET_TOPIC="audit.tenant-acme.events"

echo "========================================================"
echo "  ACL Violation Test"
echo "  Producer:  $GLOBEX_USER"
echo "  Topic:     $TARGET_TOPIC (belongs to tenant-acme)"
echo "========================================================"
echo ""
echo "Attempting cross-tenant produce (should be DENIED)..."
echo ""

# Write globex client properties inside the container
docker exec "$KAFKA_CONTAINER" bash -c "cat > /tmp/globex.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${GLOBEX_USER}\" password=\"${GLOBEX_PASSWORD}\";
EOF"


TMPOUT=$(mktemp)
TMPERR=$(mktemp)

docker exec "$KAFKA_CONTAINER" bash -c "
  echo '{\"cross_tenant\":\"attempt\"}' | \
  kafka-console-producer \
    --bootstrap-server localhost:9093 \
    --producer.config /tmp/globex.properties \
    --topic ${TARGET_TOPIC} \
    --timeout 10000 \
    --request-required-acks 1 \
  2>&1
" > "$TMPOUT" 2>"$TMPERR" || true

COMBINED=$(cat "$TMPOUT" "$TMPERR")
rm -f "$TMPOUT" "$TMPERR"

echo "--- Kafka output ---"
echo "$COMBINED"
echo "--------------------"
echo ""

# Check for the authorization exception string in the output
if echo "$COMBINED" | grep -qi "TopicAuthorizationException\|Not authorized\|authorization\|TOPIC_AUTHORIZATION_FAILED"; then
  echo "[PASS] ACL correctly denied cross-tenant write."
  echo "       tenant-globex cannot write to audit.tenant-acme.events."
  echo ""
  echo "Exiting with code 1 as required (the violation was detected = expected failure)."
  exit 1
else
  echo "[UNEXPECTED] No authorization error found in output."
  echo "Either the ACLs are not set correctly, or the topic does not exist."
  echo "Full output above for debugging."
  exit 2
fi
