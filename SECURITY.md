# Security Analysis — Multi-Tenant Audit Log System

This document analyses the security posture of the system, identifies its current gaps, and proposes strategies to harden it for a real enterprise deployment.

---

## Current Security Model

The system enforces tenant isolation at three layers:

1. **Authentication** — Every Kafka principal must authenticate with SASL/SCRAM-SHA-256. Anonymous access is rejected.
2. **Authorization** — Kafka ACLs restrict each tenant principal to its own topic (`audit.{tenant}.events`) and its own consumer group prefix. `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false` means anything not explicitly allowed is denied.
3. **Quotas** — Per-principal byte-rate quotas (1 MB/s produce + 1 MB/s consume) prevent a single tenant from starving the shared broker.

---

#### Credential Rotation Strategy

**Current state:** Tenant passwords are static secrets stored in environment variables and set once during provisioning.

**Recommended rotation strategy:**

1. **Short-lived secrets via a secrets manager.** Use HashiCorp Vault (or AWS Secrets Manager / GCP Secret Manager) to issue time-limited SCRAM passwords. The provisioning script becomes a vault-triggered hook: when a password lease expires, Vault calls a rotation script that runs `kafka-configs --alter` to set the new password and updates the secret in Vault.

2. **Rolling rotation without downtime.** Kafka SCRAM supports adding a new password while the old one is still valid (you alter the config with the new credential; existing connections continue using the cached credential until they reconnect). Steps:
   a. Generate new password and store it in the secrets manager.
   b. Apply it to Kafka with `kafka-configs --alter` (new credential takes effect immediately for new connections).
   c. Signal the tenant's application to reload its credentials (e.g., by restarting the pod or sending a SIGHUP).
   d. Remove the old credential.

3. **Rotation cadence.** Rotate every 30 days at minimum. Rotate immediately on any suspected compromise (see next section).

4. **Broker-to-broker and admin credentials.** The `admin` and `kafka-broker` super-user passwords must be rotated through the same process, but require extra care because they gate broker startup via JAAS. Store them in a Kubernetes Secret or Vault with strict RBAC — not in `.env` files.

---

#### Credential Leak Impact and Mitigation

**Impact of a leaked tenant credential (e.g., `tenant-acme` password):**

- An attacker with `tenant-acme`'s credentials can **produce** forged or poisoned audit events to `audit.tenant-acme.events` and **consume** all of that tenant's audit history.
- Because ACLs are enforced per-principal, the attacker **cannot** read or write to any other tenant's topic. The blast radius is bounded to the single tenant.
- The attacker could exhaust `tenant-acme`'s 1 MB/s quota, denying service for legitimate writes from that tenant (denial of service within the tenant).

**Mitigation steps:**

1. **Immediate revocation.** Run `kafka-configs --alter --delete-config SCRAM-SHA-256` to delete the compromised credential. This instantly prevents further authentication with that password.
2. **Audit the violations topic.** Check `audit.violations` for suspicious activity patterns (unusual source IPs, unusual action types, high-frequency calls).
3. **Inspect the tenant's event stream.** Review `audit.tenant-acme.events` for forged records. Because the audit log is append-only, injected events can be identified by cross-referencing with the authoritative application database.
4. **Rotate immediately.** Issue a new credential following the rotation strategy above.
5. **Notify the tenant.** Enterprise SLAs typically require customer notification within 72 hours of a confirmed breach (e.g., GDPR Article 33).

**Impact of a leaked admin credential:**

- Far more severe: an attacker can read **all** tenant topics, modify ACLs, or destroy topics entirely.
- Mitigation: treat admin credentials as ultra-sensitive. Do not expose them in application code. Restrict them to the provisioning pipeline only, using network-level controls (e.g., allow-list the provisioning host's IP at the firewall).

---

#### Gaps for Enterprise Multi-Tenancy

The following gaps exist in the current implementation and would need to be addressed before a production enterprise deployment:

1. **No TLS encryption in transit.** The system uses `SASL_PLAINTEXT`, meaning credentials and message payloads travel unencrypted on the network. In production, switch to `SASL_SSL` with a CA-signed certificate to prevent eavesdropping and man-in-the-middle attacks.

2. **No encryption at rest.** Kafka stores messages as plain files on disk. MinIO supports server-side encryption (SSE-S3 or SSE-KMS) and Kafka itself can be paired with filesystem-level encryption (e.g., dm-crypt / LUKS). Without this, physical access to the host yields all tenant data.

3. **Secrets stored in environment variables.** Passwords in `.env` files and `docker-compose.yml` are readable by anyone with access to the host or the CI/CD pipeline. Replace with a dedicated secrets manager and inject at runtime.

4. **Single Kafka broker — no high availability.** One broker means a single point of failure. A production cluster needs at least 3 brokers with a replication factor of 3 and min ISR of 2 to survive a single node loss without data loss.

5. **No message-level tenant data validation.** The gateway trusts the `X-Tenant-ID` header. A more robust design would also cryptographically sign each audit event at the gateway so consumers can verify authenticity and detect tampering.

6. **Quota enforcement is best-effort.** Kafka quotas throttle byte rates but do not hard-reject messages above the limit — clients are simply delayed. A sophisticated tenant could still cause latency spikes for co-located partitions. For strict isolation, consider physical topic-to-broker assignment using Kafka rack awareness or dedicated partitions.

7. **No audit log for the audit system itself.** The provisioning script and admin operations (ACL changes, quota changes) should themselves emit to a separate, admin-only audit log. Currently, admin actions are invisible once performed.

8. **Consumer group proliferation.** If a tenant uses a custom consumer group name outside their prefix, the ACL will deny it — but the error is silent. Implement a naming convention enforcement layer at the gateway or document it clearly in tenant onboarding.

9. **MinIO security.** The MinIO instance uses default credentials and no TLS. For production: enable HTTPS, rotate the root credentials, create per-tenant IAM policies in MinIO, and enable bucket versioning + object lock to make archived data immutable.

10. **No rate limiting at the HTTP gateway.** The REST gateway has no per-tenant or per-IP rate limit. A misbehaving client could flood the gateway process before Kafka's quota kicks in. Add middleware (e.g., `express-rate-limit`) to enforce HTTP-layer limits independently of Kafka quotas.
