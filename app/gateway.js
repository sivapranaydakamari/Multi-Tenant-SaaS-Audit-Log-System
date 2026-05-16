"use strict";


const express = require("express");
const { Kafka, logLevel } = require("kafkajs");

const app = express();
app.use(express.json());

const BROKERS = (process.env.KAFKA_BROKERS || "kafka:9093").split(",");
const ADMIN_USER = process.env.KAFKA_ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.KAFKA_ADMIN_PASSWORD || "admin-secret";
const APP_PORT = parseInt(process.env.APP_PORT || "3000", 10);

// Per-tenant SASL credentials loaded from environment
const TENANT_CREDENTIALS = {
  "tenant-acme": {
    username: "tenant-acme",
    password: process.env.TENANT_ACME_PASSWORD || "acme-secret",
  },
  "tenant-globex": {
    username: "tenant-globex",
    password: process.env.TENANT_GLOBEX_PASSWORD || "globex-secret",
  },
  "tenant-initech": {
    username: "tenant-initech",
    password: process.env.TENANT_INITECH_PASSWORD || "initech-secret",
  },
};


// One producer per tenant, created lazily and cached here.
const producerCache = {};

function makeSaslConfig(username, password) {
  return {
    mechanism: "scram-sha-256",
    username,
    password,
  };
}

function makeKafkaClient(username, password) {
  return new Kafka({
    clientId: `audit-gateway-${username}`,
    brokers: BROKERS,
    logLevel: logLevel.WARN,
    sasl: makeSaslConfig(username, password),
    ssl: false,
    // Retry a few times on startup; containers may still be settling
    retry: { retries: 5, initialRetryTime: 500 },
  });
}

async function getTenantProducer(tenantId) {
  if (producerCache[tenantId]) return producerCache[tenantId];

  const creds = TENANT_CREDENTIALS[tenantId];
  const kafka = makeKafkaClient(creds.username, creds.password);
  const producer = kafka.producer();
  await producer.connect();
  producerCache[tenantId] = producer;
  return producer;
}

// Admin producer for the violations topic
let violationsProducer = null;
async function getViolationsProducer() {
  if (violationsProducer) return violationsProducer;
  const kafka = makeKafkaClient(ADMIN_USER, ADMIN_PASSWORD);
  violationsProducer = kafka.producer();
  await violationsProducer.connect();
  return violationsProducer;
}


app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.post("/events", async (req, res) => {
  const tenantId = req.headers["x-tenant-id"];
  const sourceIp = req.ip || req.connection.remoteAddress;

  if (!tenantId || !TENANT_CREDENTIALS[tenantId]) {
    // Log violation for unknown / missing tenant
    const violation = {
      event: "unauthorized_access_attempt",
      attempted_tenant_id: tenantId || null,
      source_ip: sourceIp,
      timestamp: new Date().toISOString(),
      request_body: req.body,
    };

    try {
      const vProducer = await getViolationsProducer();
      await vProducer.send({
        topic: "audit.violations",
        messages: [{ value: JSON.stringify(violation) }],
      });
      console.warn("[VIOLATION]", JSON.stringify(violation));
    } catch (err) {
      console.error("[VIOLATION] Failed to write to audit.violations:", err.message);
    }

    return res.status(401).json({
      error: "Unauthorized",
      message: "Unknown or missing X-Tenant-ID header.",
    });
  }

  const { actor_id, action, timestamp, details } = req.body || {};
  if (!actor_id || !action || !timestamp) {
    return res.status(400).json({
      error: "Bad Request",
      message: "Body must include actor_id, action, and timestamp.",
    });
  }

  // ── Produce event to tenant's topic ──────────────────────────────────────
  const topic = `audit.${tenantId}.events`;
  const message = {
    tenant_id: tenantId,
    actor_id,
    action,
    timestamp,
    details: details || {},
    // Enrich with ingestion metadata
    _ingested_at: new Date().toISOString(),
    _source_ip: sourceIp,
  };

  try {
    const producer = await getTenantProducer(tenantId);
    await producer.send({
      topic,
      messages: [
        {
          // Use actor_id as partition key so all events from one actor land
          // on the same partition (ordered per-actor).
          key: actor_id,
          value: JSON.stringify(message),
        },
      ],
    });

    console.log(`[EVENT] tenant=${tenantId} actor=${actor_id} action=${action}`);
    return res.status(202).json({ accepted: true, topic });
  } catch (err) {
    console.error(`[ERROR] Failed to produce event for ${tenantId}:`, err.message);

    // If it's an authorization error, surface it clearly
    if (err.message && err.message.includes("TopicAuthorizationException")) {
      return res.status(403).json({ error: "Forbidden", message: err.message });
    }

    return res.status(500).json({ error: "Internal Server Error", message: err.message });
  }
});


async function start() {
  // Eagerly connect the violations producer so the first 401 response is fast.
  try {
    await getViolationsProducer();
    console.log("[GATEWAY] Violations producer connected.");
  } catch (err) {
    console.warn("[GATEWAY] Could not connect violations producer on startup:", err.message);
    // Non-fatal — will retry on first request
  }

  app.listen(APP_PORT, () => {
    console.log(`[GATEWAY] Listening on port ${APP_PORT}`);
    console.log(`[GATEWAY] Known tenants: ${Object.keys(TENANT_CREDENTIALS).join(", ")}`);
  });
}

start().catch((err) => {
  console.error("[FATAL]", err);
  process.exit(1);
});
