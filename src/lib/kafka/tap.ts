/**
 * KafkaTap — a server-side singleton that bridges real Strimzi-managed Kafka
 * topics with the in-process `BrokerSim`.
 *
 * When the runtime mode flips to `real` and the Setup Wizard surfaces credentials,
 * the mesh runtime calls `getKafkaTap().enable(broker)`. From that point on:
 *
 *   - Every record produced by an agent (via `mesh.broker.append`) is also
 *     published to the same-named topic on the real cluster, so a
 *     `kafka-console-consumer` from a terminal will see exactly what the
 *     agents are doing.
 *   - Every record arriving on the real cluster (e.g. produced by the
 *     fast-producer demo workload, or by the Setup Wizard's payments
 *     producer) is mirrored back into the simulator and emitted as a
 *     `topic-record` wire event so the UI animates in real time.
 *
 * Echo-suppression header (`x-ams-source`) prevents publish→subscribe loops.
 *
 * Failure modes are non-fatal: if the real consumer/producer fails, the
 * simulator continues to drive the demo. The runtime mode UI surfaces the
 * connection error so operators can investigate.
 */
import "server-only";
import type { BrokerSim } from "../broker";
import { getEventBus } from "../event-bus";
import type { KafkaRecord, TopicName, WireEvent } from "../types";
import { KafkaBackend } from "./backend";

export const TAP_TOPICS: TopicName[] = [
  "ops.requests.v1",
  "ops.kafka.metrics.v1",
  "ops.incidents.v1",
  "ops.actions.audit.v1",
  "ops.lessons.v1",
  "ops.notifications.v1",
];

/** demo data-plane topic — produced by the fast-producer workload */
export const DEMO_TOPIC = "demo.payments.events";

export type TapStatus = {
  enabled: boolean;
  bootstrap?: string;
  username?: string;
  topics: string[];
  connectedSince?: string;
  lastError?: string;
  inboundCount: number;
  outboundCount: number;
  outboundFailures: number;
};

export class KafkaTap {
  private backend: KafkaBackend | null = null;
  private status: TapStatus = {
    enabled: false,
    topics: [],
    inboundCount: 0,
    outboundCount: 0,
    outboundFailures: 0,
  };
  private broker: BrokerSim | null = null;
  /** Coalesce outbound writes so a burst doesn't backpressure the agent loop. */
  private outboundQueue: { topic: TopicName | string; key: string; value: unknown }[] = [];
  private flushing = false;

  attachBroker(broker: BrokerSim): void {
    this.broker = broker;
  }

  getStatus(): TapStatus {
    return { ...this.status, topics: [...this.status.topics] };
  }

  /** Enable the tap. Connects producer + consumer, registers inbound handler.
   *  Idempotent — repeated calls reset and reconnect. */
  async enable(): Promise<void> {
    await this.disable();
    const backend = KafkaBackend.fromRuntime();
    if (!backend) {
      this.status.lastError = "no kafka credentials in runtime";
      return;
    }
    try {
      await backend.connect();
      const groupId = `agent-mesh-tap-${Math.random().toString(36).slice(2, 8)}`;
      await backend.subscribe(groupId, [...TAP_TOPICS, DEMO_TOPIC], (msg) => this.onInbound(msg));
      this.backend = backend;
      this.status.enabled = true;
      this.status.bootstrap = backend.getStatus().bootstrap;
      this.status.username = backend.getStatus().username;
      this.status.topics = [...TAP_TOPICS, DEMO_TOPIC];
      this.status.connectedSince = new Date().toISOString();
      this.status.lastError = undefined;
    } catch (e) {
      this.status.enabled = false;
      this.status.lastError = e instanceof Error ? e.message : String(e);
      throw e;
    }
  }

  async disable(): Promise<void> {
    if (this.backend) {
      try { await this.backend.disconnect(); } catch { /* ignore */ }
    }
    this.backend = null;
    this.status.enabled = false;
    this.status.topics = [];
    this.status.connectedSince = undefined;
  }

  /** Called by `mesh.broker.append` shim to mirror simulator writes onto the
   *  real cluster. Errors are swallowed and counted, never thrown. */
  publish(topic: TopicName | string, key: string, value: unknown): void {
    if (!this.status.enabled || !this.backend) return;
    this.outboundQueue.push({ topic, key, value });
    if (!this.flushing) void this.flush();
  }

  private async flush(): Promise<void> {
    if (this.flushing) return;
    this.flushing = true;
    try {
      while (this.outboundQueue.length > 0 && this.backend) {
        const batch = this.outboundQueue.splice(0, 64);
        for (const m of batch) {
          try {
            await this.backend.produce(m.topic, m.key, m.value);
            this.status.outboundCount += 1;
          } catch (e) {
            this.status.outboundFailures += 1;
            this.status.lastError = e instanceof Error ? e.message : String(e);
          }
        }
      }
    } finally {
      this.flushing = false;
    }
  }

  /** Real Kafka -> simulator + UI fan-out. */
  private onInbound(msg: {
    topic: string;
    partition: number;
    offset: number;
    key: string | null;
    value: string | null;
    timestamp: number;
    headers: Record<string, string>;
  }): void {
    this.status.inboundCount += 1;
    if (!this.broker) return;
    const topic = msg.topic as TopicName;
    let parsed: unknown = msg.value;
    if (msg.value) {
      try { parsed = JSON.parse(msg.value); } catch { /* keep as string */ }
    }
    // Append into simulator without re-publishing: we are the sink here, not the source.
    const isOpsTopic = (TAP_TOPICS as string[]).includes(topic);
    if (isOpsTopic) {
      this.broker.append(topic, msg.key ?? "k", parsed, { source: "real-kafka", ...msg.headers });
    }
    // Always emit a wire event so the canvas animates in real time, even
    // for non-ops topics like demo.payments.events.
    const rec: KafkaRecord = {
      topic,
      partition: msg.partition,
      offset: msg.offset,
      ts: Date.now(),
      key: msg.key ?? "",
      value: parsed,
      timestamp: msg.timestamp || Date.now(),
      headers: msg.headers,
    };
    const wire: WireEvent = { kind: "topic-record", payload: rec };
    getEventBus().publish(wire as unknown as import('../types').BusEvent);
  }
}

const g = globalThis as unknown as { __ams_kafka_tap?: KafkaTap };

export function getKafkaTap(): KafkaTap {
  if (!g.__ams_kafka_tap) g.__ams_kafka_tap = new KafkaTap();
  return g.__ams_kafka_tap;
}
