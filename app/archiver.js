"use strict";


const { Kafka, logLevel } = require("kafkajs");
const Minio = require("minio");

const BROKERS = (process.env.KAFKA_BROKERS || "kafka:9093").split(",");
const ADMIN_USER = process.env.KAFKA_ADMIN_USER || "admin";
const ADMIN_PASSWORD = process.env.KAFKA_ADMIN_PASSWORD || "admin-secret";

const MINIO_ENDPOINT = process.env.MINIO_ENDPOINT || "minio";
const MINIO_PORT = parseInt(process.env.MINIO_PORT || "9000", 10);
const MINIO_ACCESS_KEY = process.env.MINIO_ACCESS_KEY || "minioadmin";
const MINIO_SECRET_KEY = process.env.MINIO_SECRET_KEY || "minioadmin";
const MINIO_BUCKET = process.env.MINIO_BUCKET || "kafka-archive";

const ARCHIVE_INTERVAL_SECONDS = parseInt(
  process.env.ARCHIVE_INTERVAL_SECONDS || "300",
  10
);

const TENANT_TOPICS = [
  "audit.tenant-acme.events",
  "audit.tenant-globex.events",
  "audit.tenant-initech.events",
];

const CONSUMER_GROUP = "audit-archiver-worker";
const POLL_INTERVAL_MS = 60_000; // flush buffer every 1 minute


const kafka = new Kafka({
  clientId: "audit-archiver",
  brokers: BROKERS,
  logLevel: logLevel.WARN,
  sasl: { mechanism: "scram-sha-256", username: ADMIN_USER, password: ADMIN_PASSWORD },
  ssl: false,
  retry: { retries: 8, initialRetryTime: 1000 },
});

const minioClient = new Minio.Client({
  endPoint: MINIO_ENDPOINT,
  port: MINIO_PORT,
  useSSL: false,
  accessKey: MINIO_ACCESS_KEY,
  secretKey: MINIO_SECRET_KEY,
});

const messageBuffer = {};

function bufferKey(topic, partition) {
  return `${topic}::${partition}`;
}

function addToBuffer(topic, partition, message) {
  const key = bufferKey(topic, partition);
  if (!messageBuffer[key]) messageBuffer[key] = [];
  messageBuffer[key].push({
    offset: message.offset,
    // Kafka timestamp is a string in ms; convert to ISO for readability
    timestamp: new Date(Number(message.timestamp)).toISOString(),
    kafka_timestamp_ms: Number(message.timestamp),
    key: message.key ? message.key.toString() : null,
    value: (() => {
      try {
        return JSON.parse(message.value.toString());
      } catch {
        return message.value.toString();
      }
    })(),
  });
}


function formatOffset(offset) {
  return String(offset).padStart(20, "0");
}

async function ensureBucket() {
  const exists = await minioClient.bucketExists(MINIO_BUCKET);
  if (!exists) {
    await minioClient.makeBucket(MINIO_BUCKET);
    console.log(`[ARCHIVER] Created MinIO bucket: ${MINIO_BUCKET}`);
  }
}

async function uploadBatch(topic, partition, messages) {
  const startOffset = parseInt(messages[0].offset, 10);
  // Object key matches the required spec format
  const objectKey = `${topic}/partition=${partition}/${formatOffset(startOffset)}.json`;

  const payload = JSON.stringify(
    {
      topic,
      partition,
      start_offset: startOffset,
      archived_at: new Date().toISOString(),
      message_count: messages.length,
      messages,
    },
    null,
    2
  );

  const buf = Buffer.from(payload, "utf-8");
  await minioClient.putObject(MINIO_BUCKET, objectKey, buf, buf.length, {
    "Content-Type": "application/json",
  });

  console.log(
    `[ARCHIVER] Archived ${messages.length} msg(s) → ${MINIO_BUCKET}/${objectKey}`
  );
}

// ─── Flush buffer: find messages old enough and archive them ──────────────────

async function flushBuffer() {
  const cutoffMs = Date.now() - ARCHIVE_INTERVAL_SECONDS * 1000;
  console.log(
    `[ARCHIVER] Flush started — archiving messages before ${new Date(cutoffMs).toISOString()}`
  );

  for (const [key, messages] of Object.entries(messageBuffer)) {
    const [topic, partStr] = key.split("::");
    const partition = parseInt(partStr, 10);

    // Split into: old enough to archive vs too recent to touch
    const toArchive = messages.filter((m) => m.kafka_timestamp_ms < cutoffMs);
    const toKeep = messages.filter((m) => m.kafka_timestamp_ms >= cutoffMs);

    if (toArchive.length === 0) continue;

    try {
      await uploadBatch(topic, partition, toArchive);
      // Replace buffer with only the messages we kept
      messageBuffer[key] = toKeep;
    } catch (err) {
      console.error(
        `[ARCHIVER] Upload failed for ${topic}[${partition}]:`,
        err.message
      );
      // Leave buffer untouched so we retry next cycle
    }
  }

  console.log("[ARCHIVER] Flush complete.");
}

async function start() {
  await ensureBucket();

  const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();

  // Retry subscribing until all topics exist (they're created by provision.sh
  // after docker compose up, so the first attempt may be too early).
  for (;;) {
    try {
      await consumer.subscribe({ topics: TENANT_TOPICS, fromBeginning: true });
      break;
    } catch (err) {
      console.log(
        `[ARCHIVER] Topics not ready yet (${err.message}), retrying in 10s...`
      );
      await new Promise((r) => setTimeout(r, 10_000));
    }
  }
  console.log(
    `[ARCHIVER] Subscribed to: ${TENANT_TOPICS.join(", ")}`
  );

  // Buffer every incoming message — eachMessage runs continuously
  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      addToBuffer(topic, partition, message);
    },
  });

  // Initial flush after the interval has passed; then repeat
  setTimeout(async function tick() {
    try {
      await flushBuffer();
    } catch (err) {
      console.error("[ARCHIVER] Flush error:", err.message);
    }
    setTimeout(tick, POLL_INTERVAL_MS);
  }, ARCHIVE_INTERVAL_SECONDS * 1000);

  console.log(
    `[ARCHIVER] First archive flush in ${ARCHIVE_INTERVAL_SECONDS}s, ` +
    `then every ${POLL_INTERVAL_MS / 1000}s.`
  );
}

start().catch((err) => {
  console.error("[FATAL]", err);
  process.exit(1);
});
