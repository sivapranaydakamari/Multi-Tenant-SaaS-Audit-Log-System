#!/bin/bash
# kafka-init.sh — Runs inside the cp-kafka container (kafka-init service).
# Creates kafka-broker and admin SASL/SCRAM users directly in ZooKeeper
# BEFORE the Kafka broker starts (bootstrap chicken-and-egg solution).
#
# Why ZooKeeper directly?  The broker API requires auth to set SCRAM users,
# but the broker can't start until these users exist. Using --zookeeper bypasses
# the broker entirely.

set -e

ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin-secret}"
ZK="zookeeper:2181"

echo "[kafka-init] Waiting for ZooKeeper at ${ZK}..."
for i in $(seq 1 20); do
  if cat < /dev/null > /dev/tcp/zookeeper/2181 2>/dev/null; then
    echo "[kafka-init] ZooKeeper is up (attempt $i)."
    break
  fi
  echo "[kafka-init] Not ready ($i/20), retrying in 3s..."
  sleep 3
done

# Extra grace period — ZK needs a moment after TCP open before accepting commands
sleep 2

echo "[kafka-init] Creating SCRAM user: kafka-broker (used for inter-broker auth)"
kafka-configs \
  --zookeeper "$ZK" \
  --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=kafka-broker-secret]' \
  --entity-type users \
  --entity-name kafka-broker

echo "[kafka-init] Creating SCRAM user: admin (super user for provisioning)"
kafka-configs \
  --zookeeper "$ZK" \
  --alter \
  --add-config "SCRAM-SHA-256=[iterations=8192,password=${ADMIN_PASSWORD}]" \
  --entity-type users \
  --entity-name admin

echo "[kafka-init] Both bootstrap SCRAM users created. Kafka can now start."
