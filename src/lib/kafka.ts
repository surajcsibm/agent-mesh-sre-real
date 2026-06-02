/**
 * kafka.ts — KafkaJS bridge for Agent Mesh SRE
 *
 * Set KAFKA_MODE=REAL in .env.local to connect to a real broker (Confluent Cloud).
 * Default is MOCK — all produce/consume calls are no-ops and the demo runs on the
 * in-memory event bus only.
 *
 * Confluent Cloud required env vars (KAFKA_MODE=REAL):
 *   KAFKA_BOOTSTRAP_SERVERS   e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
 *   KAFKA_SASL_USERNAME       Confluent API Key
 *   KAFKA_SASL_PASSWORD       Confluent API Secret
 */

import type { AuditRecord, LessonRecord } from "./types";

// ── Mode detection ────────────────────────────────────────────────────────────

export const KAFKA_MODE: "MOCK" | "REAL" =
  process.env.KAFKA_MODE === "REAL" ? "REAL" : "MOCK";

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

async function buildRealProducer(): Promise<MeshProducer> {
  const { Kafka, logLevel } = await import("kafkajs");

const sslConfig = process.env.KAFKA_SSL_CA_B64
    ? {
        rejectUnauthorized: true,
        ca: [Buffer.from(process.env.KAFKA_SSL_CA_B64, "base64").toString("utf-8")],
      }
    : true;

  
  const kafka = new Kafka({
    clientId: "agent-mesh-sre-producer",
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS!],
    ssl: sslConfig,
    sasl: {
      mechanism: (process.env.KAFKA_SASL_MECHANISM ?? "plain") as
        | "plain"
        | "scram-sha-256"
        | "scram-sha-512",
      username: process.env.KAFKA_SASL_USERNAME!,
      password: process.env.KAFKA_SASL_PASSWORD!,
    },
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

const sslConfig = process.env.KAFKA_SSL_CA_B64
    ? {
        rejectUnauthorized: true,
        ca: [Buffer.from(process.env.KAFKA_SSL_CA_B64, "base64").toString("utf-8")],
      }
    : true;

  const kafka = new Kafka({
    clientId: "agent-mesh-sre-consumer",
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS!],
    ssl: sslConfig,
    sasl: {
      mechanism: (process.env.KAFKA_SASL_MECHANISM ?? "plain") as
        | "plain"
        | "scram-sha-256"
        | "scram-sha-512",
      username: process.env.KAFKA_SASL_USERNAME!,
      password: process.env.KAFKA_SASL_PASSWORD!,
    },
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
            console.error(`[Kafka REAL] Failed to parse message on ${topic}[${partition}]`, err);
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
    .catch((err) => console.error(`[Kafka] produce error on ${topic}:`, err));
}

export function kafkaProduceAudit(record: AuditRecord): void {
  getMeshProducer()
    .then((p) => p.sendAudit(record))
    .catch((err) => console.error("[Kafka] audit produce error:", err));
}

export function kafkaProduceLesson(record: LessonRecord): void {
  getMeshProducer()
    .then((p) => p.sendLesson(record))
    .catch((err) => console.error("[Kafka] lesson produce error:", err));
}
