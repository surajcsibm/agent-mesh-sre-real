/**
 * KafkaJS-backed Kafka client used in real mode.
 *
 * Talks to the Strimzi-managed cluster created by the Setup Wizard, using
 * the credentials surfaced through `runtime-mode`. Connects with TLS to
 * the cluster CA + SASL/SCRAM-SHA-512 against the agent-mesh-controller
 * KafkaUser.
 *
 * The mesh runtime never imports kafkajs directly — it goes through the
 * `KafkaBackend` interface so the in-memory simulator and the real cluster
 * are interchangeable.
 */
import "server-only";
import { Admin, Consumer, Kafka, Producer, RecordMetadata, logLevel } from "kafkajs";
import { getRuntime } from "../runtime-mode";

export type KafkaBackendStatus = {
  connected: boolean;
  bootstrap?: string;
  username?: string;
  topics: string[];
  lastError?: string;
  startedAt?: string;
};

export type KafkaInbound = (msg: {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: string | null;
  timestamp: number;
  headers: Record<string, string>;
}) => void;

const APP_FIELD_HEADER = "x-ams-source";
/** Header value used by the simulator → real producer to mark records that
 *  originated server-side. Lets us suppress echo loops when the consumer
 *  picks them back up. */
const APP_FIELD_VALUE = "agent-mesh-sim";

export class KafkaBackend {
  private kafka: Kafka;
  private producer: Producer | null = null;
  private consumer: Consumer | null = null;
  private admin: Admin | null = null;
  private inbound: KafkaInbound | null = null;
  private subscribed: string[] = [];
  private status: KafkaBackendStatus = { connected: false, topics: [] };

  constructor(opts: { brokers: string[]; ssl?: boolean; ca?: string; sasl?: { mechanism: "scram-sha-512" | "scram-sha-256" | "plain"; username: string; password: string }; clientId?: string }) {
    this.kafka = new Kafka({
      clientId: opts.clientId ?? "agent-mesh-sre",
      brokers: opts.brokers,
      ssl: opts.ssl ? (opts.ca ? { ca: [opts.ca], rejectUnauthorized: false } : true) : false,
      sasl: opts.sasl,
      logLevel: logLevel.NOTHING,
      connectionTimeout: 8_000,
      requestTimeout: 12_000,
      retry: { retries: 4, initialRetryTime: 250 },
    });
    this.status.bootstrap = opts.brokers.join(",");
    this.status.username = opts.sasl?.username;
  }

  static fromRuntime(): KafkaBackend | null {
    const r = getRuntime();
    if (!r.kafka) return null;
    const bootstrap = r.kafka.bootstrapInternal || r.kafka.bootstrapExternal;
    if (!bootstrap) return null;
    if (!r.kafka.username || !r.kafka.password) return null;
    return new KafkaBackend({
      brokers: bootstrap.split(",").map((s) => s.trim()),
      ssl: true,
      ca: r.kafka.caCertPem,
      sasl: {
        mechanism: r.kafka.saslMechanism ?? "scram-sha-512",
        username: r.kafka.username,
        password: r.kafka.password,
      },
    });
  }

  getStatus(): KafkaBackendStatus {
    return { ...this.status, topics: [...this.subscribed] };
  }

  /** Connect producer + admin and prepare for produce()/subscribe(). */
  async connect(): Promise<void> {
    this.producer = this.kafka.producer({ allowAutoTopicCreation: false, idempotent: false });
    await this.producer.connect();
    this.admin = this.kafka.admin();
    await this.admin.connect();
    this.status.connected = true;
    this.status.startedAt = new Date().toISOString();
    this.status.lastError = undefined;
  }

  async disconnect(): Promise<void> {
    try { await this.consumer?.disconnect(); } catch { /* ignore */ }
    try { await this.producer?.disconnect(); } catch { /* ignore */ }
    try { await this.admin?.disconnect(); } catch { /* ignore */ }
    this.consumer = null;
    this.producer = null;
    this.admin = null;
    this.subscribed = [];
    this.status.connected = false;
  }

  /** Subscribe a single shared consumer to all given topics, fan messages
   *  out via the registered inbound handler. */
  async subscribe(groupId: string, topics: string[], inbound: KafkaInbound): Promise<void> {
    this.inbound = inbound;
    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
      retry: { retries: 4, initialRetryTime: 250 },
    });
    await this.consumer.connect();
    for (const t of topics) {
      try {
        await this.consumer.subscribe({ topic: t, fromBeginning: false });
        this.subscribed.push(t);
      } catch (e) {
        this.status.lastError = `subscribe ${t}: ${errMsg(e)}`;
      }
    }
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!this.inbound) return;
        const headers = headersToObj(message.headers);
        // Suppress echo: if this record came from us we already streamed it
        if (headers[APP_FIELD_HEADER] === APP_FIELD_VALUE) return;
        this.inbound({
          topic,
          partition,
          offset: Number(message.offset),
          key: message.key ? message.key.toString("utf8") : null,
          value: message.value ? message.value.toString("utf8") : null,
          timestamp: Number(message.timestamp),
          headers,
        });
      },
    });
  }

  /** Produce one record to the real cluster. Throws on failure so the
   *  caller can decide whether to log/ignore (mesh logs, never throws). */
  async produce(
    topic: string,
    key: string,
    value: unknown,
    headers: Record<string, string> = {}
  ): Promise<RecordMetadata[]> {
    if (!this.producer) throw new Error("KafkaBackend.connect() not called");
    return this.producer.send({
      topic,
      messages: [
        {
          key,
          value: typeof value === "string" ? value : JSON.stringify(value),
          headers: {
            [APP_FIELD_HEADER]: APP_FIELD_VALUE,
            "content-type": "application/json",
            ...headers,
          },
        },
      ],
    });
  }

  /** Best-effort fetch of partition counts and end offsets for a topic. */
  async describeTopic(topic: string): Promise<{ partitions: number; logEndOffsets: number[] }> {
    if (!this.admin) throw new Error("KafkaBackend.connect() not called");
    const md = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const partitions = md.topics[0]?.partitions.length ?? 1;
    const offsets = await this.admin.fetchTopicOffsets(topic);
    const logEndOffsets = new Array(partitions).fill(0);
    for (const o of offsets) {
      logEndOffsets[o.partition] = Number(o.offset);
    }
    return { partitions, logEndOffsets };
  }
}

function headersToObj(h: unknown): Record<string, string> {
  if (!h || typeof h !== "object") return {};
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(h as Record<string, unknown>)) {
    if (Buffer.isBuffer(v)) out[k] = v.toString("utf8");
    else if (typeof v === "string") out[k] = v;
  }
  return out;
}

function errMsg(e: unknown): string {
  if (e instanceof Error) return e.message;
  return String(e);
}
