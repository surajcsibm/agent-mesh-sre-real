/**
 * kafka.ts — KafkaJS bridge for Agent Mesh SRE
 *
 * Set KAFKA_MODE=real in .env.local to connect to a real broker.
 * Default is MOCK — all produce/consume calls are no-ops and the demo runs on the
 * in-memory event bus only.
 *
 * Required env vars (KAFKA_MODE=real):
 *   KAFKA_BOOTSTRAP_SERVERS   e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
 *                             or 64.227.25.232:31090 for the local demo cluster
 *   KAFKA_SASL_USERNAME       (optional) Confluent API Key / SASL username
 *   KAFKA_SASL_PASSWORD       (optional) Confluent API Secret / SASL password
 *   KAFKA_SASL_MECHANISM      (optional) plain | scram-sha-256 | scram-sha-512
 *   KAFKA_SSL_ENABLED         (optional) "false" to disable TLS for plaintext
 *                             brokers (e.g. the unauthenticated DO demo cluster).
 *                             Defaults to true if unset.
 *   KAFKA_SSL_CA_B64          (optional) base64-encoded CA cert for TLS verification
 */

import type { AuditRecord, LessonRecord } from "./types";
import { safeErr } from "./log-safe";

// ── Mode detection ────────────────────────────────────────────────────────────
// Case-insensitive: KAFKA_MODE=real, REAL, Real all activate real mode.

export const KAFKA_MODE: "MOCK" | "REAL" =
  process.env.KAFKA_MODE?.toLowerCase() === "real" ? "REAL" : "MOCK";

// ── Topic registry ────────────────────────────────────────────────────────────

export const TOPICS = {
  REQUESTS:      "ops.requests.v1",
  METRICS:       "ops.kafka.metrics.v1",
  INCIDENTS:     "ops.incidents.v1",
  AUDIT:         "ops.actions.audit.v1",
  LESSONS:       "ops.lessons.v1",
  NOTIFICATIONS: "ops.notifications.v1",
  PAYMENTS:      "demo.payments.events",
} as const;

export type TopicName = typeof TOPICS[keyof typeof TOPICS];

// ── Message envelope ──────────────────────────────────────────────────────────

export interface KafkaMessage<T = unknown> {
  topic: TopicName;
  key?: string;
  value: T;
  headers?: Record<string, string>;
  timestamp?: number;
}

// ── Producer interface ────────────────────────────────────────────────────────

export interface MeshProducer {
  send<T>(msg: KafkaMessage<T>): Promise<void>;
  sendAudit(record: AuditRecord): Promise<void>;
  sendLesson(record: LessonRecord): Promise<void>;
  disconnect(): Promise<void>;
}

// ── Consumer interface ────────────────────────────────────────────────────────

export interface MeshConsumer {
  subscribe(topics: TopicName[], onMessage: (msg: KafkaMessage) => void): Promise<void>;
  disconnect(): Promise<void>;
}

// ── MOCK implementations (default) ───────────────────────────────────────────

class MockProducer implements MeshProducer {
  async send<T>(msg: KafkaMessage<T>) {
    if (process.env.NODE_ENV === "development") {
      console.log(`[Kafka MOCK] → ${msg.topic}`, typeof msg.value === "object" ? JSON.stringify(msg.value).slice(0, 120) : msg.value);
    }
  }
  async sendAudit(record: AuditRecord) {
    await this.send({ topic: TOPICS.AUDIT, key: record.id, value: record });
  }
  async sendLesson(record: LessonRecord) {
    await this.send({ topic: TOPICS.LESSONS, key: record.id, value: record });
  }
  async disconnect() {}
}

class MockConsumer implements MeshConsumer {
  async subscribe(_topics: TopicName[], _onMessage: (msg: KafkaMessage) => void) {
    // MOCK: no-op — the in-memory event bus drives all UI updates
  }
  async disconnect() {}
}

// ── REAL KafkaJS implementation ───────────────────────────────────────────────

function buildKafkaJSConfig(clientId: string) {
  const sslEnabled = process.env.KAFKA_SSL_ENABLED?.toLowerCase() !== "false";

  const sslConfig = !sslEnabled
    ? false
    : process.env.KAFKA_SSL_CA_B64
      ? {
          rejectUnauthorized: true,
          ca: [Buffer.from(process.env.KAFKA_SSL_CA_B64, "base64").toString("utf-8")],
        }
      : true;

  return {
    clientId,
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS!],
    ssl: sslConfig,
    // Only include the sasl block when credentials are actually provided —
    // an unconditional block with undefined username/password breaks
    // connections to unauthenticated brokers (e.g. the local/DO demo cluster).
    ...(process.env.KAFKA_SASL_USERNAME
      ? {
          sasl: {
            mechanism: (process.env.KAFKA_SASL_MECHANISM ?? "plain") as
              | "plain"
              | "scram-sha-256"
              | "scram-sha-512",
            username: process.env.KAFKA_SASL_USERNAME,
            password: process.env.KAFKA_SASL_PASSWORD ?? "",
          } as any,
        }
      : {}),
  };
}

async function buildRealProducer(): Promise<MeshProducer> {
  const { Kafka, logLevel } = await import("kafkajs");

  const kafka = new Kafka({
    ...buildKafkaJSConfig("agent-mesh-sre-producer"),
    logLevel: logLevel.WARN,
  });

  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
  });

  await producer.connect();
  console.log("[Kafka REAL] Producer connected →", process.env.KAFKA_BOOTSTRAP_SERVERS);

  return {
    async send<T>(msg: KafkaMessage<T>) {
      await producer.send({
        topic: msg.topic,
        messages: [{
          key: msg.key ?? null,
          value: JSON.stringify(msg.value),
          headers: {
            "content-type": "application/json",
            "source": "agent-mesh-sre",
            ...(msg.headers ?? {}),
          },
          timestamp: String(msg.timestamp ?? Date.now()),
        }],
      });
    },
    async sendAudit(record: AuditRecord) {
      await producer.send({
        topic: TOPICS.AUDIT,
        messages: [{
          key: record.id,
          value: JSON.stringify(record),
          headers: { "agent": record.agent, "event-type": record.type },
        }],
      });
    },
    async sendLesson(record: LessonRecord) {
      await producer.send({
        topic: TOPICS.LESSONS,
        messages: [{
          key: record.id,
          value: JSON.stringify(record),
          headers: { "scenario": record.scenarioId },
        }],
      });
    },
    async disconnect() {
      await producer.disconnect();
    },
  };
}

async function buildRealConsumer(groupId = "agent-mesh-sre-monitor"): Promise<MeshConsumer> {
  const { Kafka, logLevel } = await import("kafkajs");

  const resolvedConfig = buildKafkaJSConfig("agent-mesh-sre-consumer");
  // TEMP-DIAGNOSTIC: structural-only, never logs actual secret values —
  // just whether ssl/sasl resolved truthy, to find why this specific caller
  // (the first-ever consumer connection in this codebase) fails with a TLS
  // handshake error while the identically-configured producer never has.
  console.log("[Kafka REAL] Consumer config check:", {
    sslType: typeof resolvedConfig.ssl,
    sslTruthy: !!resolvedConfig.ssl,
    hasSasl: "sasl" in resolvedConfig,
    brokerCount: resolvedConfig.brokers?.length,
    KAFKA_SSL_ENABLED_raw: JSON.stringify(process.env.KAFKA_SSL_ENABLED),
  });

  const kafka = new Kafka({
    ...resolvedConfig,
    logLevel: logLevel.WARN,
  });

  const consumer = kafka.consumer({ groupId });

  return {
    async subscribe(topics: TopicName[], onMessage: (msg: KafkaMessage) => void) {
      await consumer.connect();
      console.log(`[Kafka REAL] Consumer (${groupId}) connected`);

      for (const topic of topics) {
        await consumer.subscribe({ topic, fromBeginning: false });
      }

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const value = message.value ? JSON.parse(message.value.toString()) : null;
            onMessage({
              topic: topic as TopicName,
              key: message.key?.toString(),
              value,
              headers: Object.fromEntries(
                Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString() ?? ""])
              ),
              timestamp: message.timestamp ? Number(message.timestamp) : Date.now(),
            });
          } catch (err) {
            console.error(`[Kafka REAL] Failed to parse message on ${topic}[${partition}]`, safeErr(err));
          }
        },
      });
    },
    async disconnect() {
      await consumer.disconnect();
    },
  };
}

// ── Singleton factory ─────────────────────────────────────────────────────────

declare global {
  var __meshProducer: MeshProducer | undefined;
  var __meshConsumer: MeshConsumer | undefined;
}

export async function getMeshProducer(): Promise<MeshProducer> {
  if (!globalThis.__meshProducer) {
    globalThis.__meshProducer = KAFKA_MODE === "REAL"
      ? await buildRealProducer()
      : new MockProducer();
  }
  return globalThis.__meshProducer;
}

export async function getMeshConsumer(groupId?: string): Promise<MeshConsumer> {
  if (!globalThis.__meshConsumer) {
    globalThis.__meshConsumer = KAFKA_MODE === "REAL"
      ? await buildRealConsumer(groupId)
      : new MockConsumer();
  }
  return globalThis.__meshConsumer;
}

// ── Convenience: fire-and-forget produce ─────────────────────────────────────

export function kafkaProduce<T>(topic: TopicName, value: T, key?: string): void {
  getMeshProducer()
    .then((p) => p.send({ topic, key, value }))
    .catch((err) => console.error(`[Kafka] produce error on ${topic}:`, safeErr(err)));
}

export function kafkaProduceAudit(record: AuditRecord): void {
  getMeshProducer()
    .then((p) => p.sendAudit(record))
    .catch((err) => console.error("[Kafka] audit produce error:", safeErr(err)));
}

export function kafkaProduceLesson(record: LessonRecord): void {
  getMeshProducer()
    .then((p) => p.sendLesson(record))
    .catch((err) => console.error("[Kafka] lesson produce error:", safeErr(err)));
}
