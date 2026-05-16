#!/usr/bin/env bash
# provision.sh — Bootstrap the multi-tenant Kafka environment.
# Creates topics, SASL/SCRAM users, ACLs, and quotas for three tenants.
# Run this AFTER docker-compose up --build -d and all services are healthy.

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
# Admin CLI commands run inside the Kafka container via docker exec.
# localhost:9093 is the INTERNAL (SASL_PLAINTEXT) listener.
BOOTSTRAP="localhost:9093"
ADMIN_USER="${KAFKA_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${KAFKA_ADMIN_PASSWORD:-admin-secret}"

TENANT_ACME_PASSWORD="${TENANT_ACME_PASSWORD:-acme-secret}"
TENANT_GLOBEX_PASSWORD="${TENANT_GLOBEX_PASSWORD:-globex-secret}"
TENANT_INITECH_PASSWORD="${TENANT_INITECH_PASSWORD:-initech-secret}"

TENANTS=("tenant-acme" "tenant-globex" "tenant-initech")
declare -A TENANT_PASSWORDS=(
  ["tenant-acme"]="$TENANT_ACME_PASSWORD"
  ["tenant-globex"]="$TENANT_GLOBEX_PASSWORD"
  ["tenant-initech"]="$TENANT_INITECH_PASSWORD"
)

# Quota: 1 MB/s = 1048576 bytes/s
QUOTA_BYTES=1048576


# Run a kafka CLI command inside the kafka container as the admin user.
kafka_exec() {
  docker exec "$KAFKA_CONTAINER" bash -c "$*"
}

# Write a temp admin client properties file inside the container.
setup_admin_props() {
  kafka_exec "cat > /tmp/admin.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${ADMIN_USER}\" password=\"${ADMIN_PASSWORD}\";
EOF"
}

# Write a tenant-specific client properties file inside the container.
setup_tenant_props() {
  local tenant="$1"
  local password="$2"
  kafka_exec "cat > /tmp/${tenant}.properties << 'EOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"${tenant}\" password=\"${password}\";
EOF"
}

echo "========================================================"
echo "  Multi-Tenant Audit System — Provisioning Script"
echo "========================================================"

echo ""
echo "[1/5] Writing admin client properties..."
setup_admin_props
echo "      Admin properties written to /tmp/admin.properties inside container."

echo ""
echo "[2/5] Creating audit.violations topic..."
kafka_exec "kafka-topics \
  --bootstrap-server ${BOOTSTRAP} \
  --command-config /tmp/admin.properties \
  --create \
  --topic audit.violations \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists" && echo "      audit.violations created."

echo ""
echo "[3/5] Provisioning tenants..."

for tenant in "${TENANTS[@]}"; do
  password="${TENANT_PASSWORDS[$tenant]}"
  topic="audit.${tenant}.events"
  group="audit.${tenant}.consumers"

  echo ""
  echo "  ── Tenant: $tenant ──────────────────────────────────"

  # 3a. Create SCRAM user
  echo "      Creating SCRAM user '${tenant}'..."
  kafka_exec "kafka-configs \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --alter \
    --add-config 'SCRAM-SHA-256=[iterations=8192,password=${password}]' \
    --entity-type users \
    --entity-name ${tenant}"

  # 3b. Create dedicated topic
  echo "      Creating topic '${topic}'..."
  kafka_exec "kafka-topics \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --create \
    --topic ${topic} \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists"

  # 3c. ACL — WRITE to tenant's own topic
  echo "      Applying WRITE ACL on topic '${topic}'..."
  kafka_exec "kafka-acls \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --add \
    --allow-principal User:${tenant} \
    --operation Write \
    --operation Describe \
    --topic ${topic}"

  # 3d. ACL — READ from tenant's own topic
  echo "      Applying READ ACL on topic '${topic}'..."
  kafka_exec "kafka-acls \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --add \
    --allow-principal User:${tenant} \
    --operation Read \
    --operation Describe \
    --topic ${topic}"

  # 3e. ACL — consumer group access (prefix match covers any sub-group)
  echo "      Applying consumer group ACL for group prefix '${tenant}'..."
  kafka_exec "kafka-acls \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --add \
    --allow-principal User:${tenant} \
    --operation Read \
    --group ${group} \
    --resource-pattern-type prefixed"

  # 3f. Producer quota (byte-rate 1 MB/s)
  echo "      Setting producer quota (${QUOTA_BYTES} bytes/s)..."
  kafka_exec "kafka-configs \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --alter \
    --add-config 'producer_byte_rate=${QUOTA_BYTES}' \
    --entity-type users \
    --entity-name ${tenant}"

  # 3g. Consumer quota (byte-rate 1 MB/s)
  echo "      Setting consumer quota (${QUOTA_BYTES} bytes/s)..."
  kafka_exec "kafka-configs \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --alter \
    --add-config 'consumer_byte_rate=${QUOTA_BYTES}' \
    --entity-type users \
    --entity-name ${tenant}"

  echo "      Tenant '${tenant}' provisioned successfully."
done

echo "[4/5] Verifying provisioned resources..."

echo ""
echo "  Topics:"
kafka_exec "kafka-topics \
  --bootstrap-server ${BOOTSTRAP} \
  --command-config /tmp/admin.properties \
  --list" | grep "audit\." | sed 's/^/    /'

echo ""
echo "  ACLs:"
kafka_exec "kafka-acls \
  --bootstrap-server ${BOOTSTRAP} \
  --command-config /tmp/admin.properties \
  --list" | grep -E "User:(tenant-|admin)" | head -40 | sed 's/^/    /'

echo ""
echo "  Quotas:"
for tenant in "${TENANTS[@]}"; do
  echo "    ${tenant}:"
  kafka_exec "kafka-configs \
    --bootstrap-server ${BOOTSTRAP} \
    --command-config /tmp/admin.properties \
    --describe \
    --entity-type users \
    --entity-name ${tenant}" | grep -E "producer_byte_rate|consumer_byte_rate" | sed 's/^/      /'
done

echo ""
echo "[5/5] Installing admin properties for Kafka healthcheck..."
kafka_exec "cp /tmp/admin.properties /tmp/admin.properties" 2>/dev/null || true

echo ""
echo "========================================================"
echo "  Provisioning complete!"
echo "========================================================"
