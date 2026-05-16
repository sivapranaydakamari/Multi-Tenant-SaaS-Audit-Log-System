# Multi-Tenant Audit Log System

A production-style multi-tenant audit logging backend built with **Apache Kafka**, **Node.js/Express**, and **MinIO**. Demonstrates tenant isolation via Kafka ACLs, per-tenant byte-rate quotas, and automatic archival to S3-compatible storage.

---

**Services:**

| Service | Description |
|---------|-------------|
| `zookeeper` | Apache ZooKeeper — Kafka metadata store |
| `kafka-init` | Short-lived: creates bootstrap SCRAM users in ZK |
| `kafka` | Apache Kafka broker with SASL/SCRAM + ACLs |
| `minio` | S3-compatible object store for archival |
| `minio-init` | Short-lived: creates the `kafka-archive` bucket |
| `app` | Express gateway + archiver worker (Node.js) |

---

## Prerequisites

- Docker >= 24
- Docker Compose v2 (`docker compose` command)
- Bash (for scripts; Git Bash on Windows)

---

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env to set your own passwords (optional for local dev)
```

### 2. Start all services

```bash
docker compose up --build -d
```

Wait for all services to be healthy (≈2–3 minutes):

```bash
docker compose ps
```

All services should show `healthy` or `exited (0)` for init containers.

### 3. Bootstrap tenants

Run once after the cluster is healthy:

```bash
chmod +x provision.sh
./provision.sh
```

This creates:
- Topics: `audit.tenant-acme.events`, `audit.tenant-globex.events`, `audit.tenant-initech.events`, `audit.violations`
- SASL/SCRAM users per tenant
- ACLs restricting each tenant to its own topic
- Producer + consumer byte-rate quotas (1 MB/s each)

### 4. Send an audit event

```bash
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-acme" \
  -d '{
    "actor_id": "user-123",
    "action": "login",
    "timestamp": "2024-01-15T10:30:00Z",
    "details": { "ip": "192.168.1.1" }
  }'
# → 202 Accepted
```

### 5. Trigger a 401 (unknown tenant)

```bash
curl -X POST http://localhost:3000/events \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-unknown" \
  -d '{"actor_id":"x","action":"test","timestamp":"2024-01-15T10:00:00Z"}'
# → 401 Unauthorized  (violation logged to audit.violations)
```

---

## Security Tests

### ACL violation (cross-tenant write attempt)

```bash
chmod +x test_acl_violation.sh
./test_acl_violation.sh
# Exits with code 1 — Kafka rejects the write with TopicAuthorizationException
```

### Quota throttling demonstration

```bash
chmod +x test_quota_violation.sh
./test_quota_violation.sh
# Floods tenant-initech above its 1 MB/s quota; broker throttles the producer
```

---

## Archival to MinIO

The archiver runs inside the `app` container. Any message in a tenant topic that is older than `ARCHIVE_INTERVAL_SECONDS` (default: 300 s / 5 minutes) is uploaded to MinIO.

**Object key format:**
```
kafka-archive/{topic}/partition={n}/{offset_padded}.json
```

**Example:**
```
kafka-archive/audit.tenant-acme.events/partition=0/00000000000000000000.json
```

Browse archived objects via the MinIO Console: http://localhost:9001  
Username/password: `minioadmin` / `minioadmin` (or values from `.env`)

---

## Stopping

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers and delete all data
```

---

## Project Structure

```
.
├── docker-compose.yml          # Orchestration
├── provision.sh                # Tenant bootstrap script
├── test_acl_violation.sh       # ACL security test
├── test_quota_violation.sh     # Quota throttling test
├── .env.example                # Environment variable template
├── SECURITY.md                 # Security analysis
├── config/
│   └── kafka_jaas.conf         # Kafka broker JAAS login config
└── app/
    ├── Dockerfile
    ├── start.sh                # Launches gateway + archiver
    ├── package.json
    ├── gateway.js              # Express HTTP gateway (POST /events)
    └── archiver.js             # Kafka → MinIO archival worker
```
