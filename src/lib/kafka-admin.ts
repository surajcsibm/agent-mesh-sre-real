
/**
 * kafka-admin.ts
 * Lightweight KafkaJS admin client for the Monitor polling loop.
 *
 * Reads ISR state, consumer group lag, and controller epoch — data that
 * the Aiven REST API does not expose via its service/topic endpoints.
 *
 * In MOCK mode returns a zero-baseline struct. The polling loop in mesh.ts
 * merges this with the live broker simulation state directly.
 */
import "server-only";
import { getRuntime } from "./runtime-mode";

export interface KafkaAdminMetrics {
  controllerEpoch: number;
  brokersOnline: number;
  underReplicatedPartitions: number;
  /** groupId → { totalLag, memberCount, state } */
  consumerGroups: Record<string, { lag: number; memberCount: number; state: string }>;
  /** Timestamp this snapshot was collected (ms). */
  collectedAt: number;
}

// ── Real metrics via KafkaJS admin ────────────────────────────────────────────

const TOPICS_TO_PROBE = [
  "demo.payments.events",
  "ops.kafka.metrics.v1",
  "ops.incidents.v1",
  "ops.requests.v1",
];

const GROUPS_TO_PROBE = [
  "payments-consumer",
  "share-group-1",
];

async function collectReal(): Promise<KafkaAdminMetrics> {
  const rt = getRuntime();
  const kafka_ = rt.kafka;
  if (!kafka_?.bootstrapInternal) {
    throw new Error("[KafkaAdmin] Kafka connection not configured in runtime");
  }

  const { Kafka, logLevel } = await import("kafkajs");
  const kafka = new Kafka({
    clientId: "agent-mesh-sre-monitor-poll",
    brokers: kafka_.bootstrapInternal.split(",").map((s) => s.trim()),
    ssl: kafka_.caCertPem
      ? { ca: [kafka_.caCertPem], rejectUnauthorized: false }
      : false,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ...(kafka_.username
      ? {
          sasl: {
            mechanism: (kafka_.saslMechanism ?? "scram-sha-256") as
              | "scram-sha-256"
              | "scram-sha-512"
              | "plain",
            username: kafka_.username,
            password: kafka_.password ?? "",
          } as any,
        }
      : {}),
    logLevel: logLevel.NOTHING,
    connectionTimeout: 8_000,
    requestTimeout: 10_000,
    retry: { retries: 2, initialRetryTime: 500 },
  });

  const admin = kafka.admin();
  try {
    await admin.connect();

    // ── Cluster info ────────────────────────────────────────────────────────
    const [clusterRes, metaRes] = await Promise.allSettled([
      admin.describeCluster(),
      admin.fetchTopicMetadata({ topics: TOPICS_TO_PROBE }),
    ]);

    let controllerEpoch = -1;
    let brokersOnline = 1;
    if (clusterRes.status === "fulfilled") {
      brokersOnline = clusterRes.value.brokers.length;
      controllerEpoch = clusterRes.value.controller ?? -1;
    }

    // ── ISR / under-replication ──────────────────────────────────────────────
    let underReplicatedPartitions = 0;
    if (metaRes.status === "fulfilled") {
      for (const topic of metaRes.value.topics) {
        for (const p of topic.partitions) {
          if (p.isr.length < p.replicas.length) underReplicatedPartitions++;
        }
      }
    }

    // ── Consumer group lag ──────────────────────────────────────────────────
    const consumerGroups: KafkaAdminMetrics["consumerGroups"] = {};

    // Build end-offset cache for lag calculation
    const endOffsets: Record<string, Record<number, number>> = {};
    await Promise.allSettled(
      TOPICS_TO_PROBE.map(async (topic) => {
        const offsets = await admin.fetchTopicOffsets(topic);
        endOffsets[topic] = {};
        for (const o of offsets) {
          endOffsets[topic][o.partition] = Number(o.offset);
        }
      })
    );

    for (const groupId of GROUPS_TO_PROBE) {
      try {
        const committedRes = await admin.fetchOffsets({
          groupId,
          topics: TOPICS_TO_PROBE,
        });
        let totalLag = 0;
        for (const { topic, partitions } of committedRes) {
          for (const { partition, offset } of partitions) {
            const committed = Number(offset);
            if (committed >= 0) {
              const end = endOffsets[topic]?.[partition] ?? committed;
              totalLag += Math.max(0, end - committed);
            }
          }
        }
        const descRes = await admin
          .describeGroups([groupId])
          .catch(() => ({ groups: [] as Array<{ state: string; members: unknown[] }> }));
        const grp = descRes.groups[0];
        consumerGroups[groupId] = {
          lag: totalLag,
          memberCount: grp?.members?.length ?? 1,
          state: grp?.state ?? "unknown",
        };
      } catch {
        // Group may not exist yet — skip silently
      }
    }

    return {
      controllerEpoch,
      brokersOnline,
      underReplicatedPartitions,
      consumerGroups,
      collectedAt: Date.now(),
    };
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// ── Baseline zero struct for MOCK / fallback ─────────────────────────────────

export function zeroAdminMetrics(): KafkaAdminMetrics {
  return {
    controllerEpoch: -1,
    brokersOnline: 1,
    underReplicatedPartitions: 0,
    consumerGroups: {},
    collectedAt: Date.now(),
  };
}

// ── Public entry point ────────────────────────────────────────────────────────

export async function getKafkaAdminMetrics(): Promise<KafkaAdminMetrics> {
  const rt = getRuntime();
  if (rt.mode !== "real") return zeroAdminMetrics();
  try {
    return await collectReal();
  } catch (e) {
    console.warn("[KafkaAdmin] Admin metrics failed:", e instanceof Error ? e.message : e);
    return zeroAdminMetrics();
  }
}

