/**
 * kafka-admin-cfk.ts
 * Kafka admin operations using KafkaJS — works with any Kafka cluster
 * (Confluent for Kubernetes, Confluent Cloud, Aiven, Redpanda, etc.)
 *
 * Replaces aiven-admin.ts — no proprietary REST API calls.
 * All operations go through the standard Kafka protocol via KafkaJS Admin.
 *
 * Required env vars (same for all providers):
 *   KAFKA_BOOTSTRAP         e.g. kafka.confluent.svc.cluster.local:9092
 *   KAFKA_SASL_MECHANISM    plain | scram-sha-256 | scram-sha-512 | (empty = no auth)
 *   KAFKA_USERNAME          (empty = no auth)
 *   KAFKA_PASSWORD          (empty = no auth)
 *   KAFKA_CA_CERT_BASE64    (empty = no TLS / use system trust store)
 */
import "server-only";
import { Kafka, Admin, ConfigResourceTypes } from "kafkajs";
import type { ITopicConfig } from "kafkajs";

// ── KafkaJS client singleton ──────────────────────────────────────────────────

function buildAdmin(): Admin {
  const brokers = (process.env.KAFKA_BOOTSTRAP ?? "localhost:9092").split(",");
  const mechanism = (process.env.KAFKA_SASL_MECHANISM ?? "").toLowerCase();
  const username = process.env.KAFKA_USERNAME ?? "";
  const password = process.env.KAFKA_PASSWORD ?? "";
  const caCertB64 = process.env.KAFKA_CA_CERT_BASE64 ?? "";

  // Build SSL config
  const ssl = caCertB64
    ? { ca: Buffer.from(caCertB64, "base64").toString("utf-8") }
    : mechanism && mechanism !== "plain"
    ? true   // SCRAM over TLS — use system trust store
    : false; // No TLS (plain internal CFK without mTLS)

  // Build SASL config — only if credentials provided
  const sasl =
    username && password && mechanism
      ? mechanism === "plain"
        ? { mechanism: "plain" as const, username, password }
        : mechanism === "scram-sha-256"
        ? { mechanism: "scram-sha-256" as const, username, password }
        : mechanism === "scram-sha-512"
        ? { mechanism: "scram-sha-512" as const, username, password }
        : undefined
      : undefined;

  const kafka = new Kafka({
    clientId: "agent-mesh-sre-admin",
    brokers,
    ssl: ssl || undefined,
    sasl,
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
    retry: { retries: 3, initialRetryTime: 300 },
  });

  return kafka.admin();
}

// No module-level caching of the Admin client — Vercel serverless functions
// can share a warm instance across concurrent invocations, and a cached
// client races between one call's disconnect() and another's in-flight
// request, producing "write after end" errors. Each call gets its own
// fully isolated client instead.
async function withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
  const admin = buildAdmin();
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.disconnect();
  }
}

// ── Topic Management ──────────────────────────────────────────────────────────

export async function listTopics(): Promise<string[]> {
  return withAdmin(async (admin) => {
    const topics = await admin.listTopics();
    return topics.filter((t) => !t.startsWith("__") && !t.startsWith("_"));
  });
}

export async function createTopic(opts: {
  name: string;
  partitions: number;
  replication: number;
  retentionMs?: number;
  cleanupPolicy?: "delete" | "compact";
}): Promise<void> {
  await withAdmin(async (admin) => {
    const topicConfig: ITopicConfig = {
      topic: opts.name,
      numPartitions: opts.partitions,
      replicationFactor: opts.replication,
      configEntries: [
        ...(opts.retentionMs !== undefined && opts.retentionMs !== -1
          ? [{ name: "retention.ms", value: String(opts.retentionMs) }]
          : []),
        ...(opts.cleanupPolicy
          ? [{ name: "cleanup.policy", value: opts.cleanupPolicy }]
          : []),
      ],
    };
    await admin.createTopics({ topics: [topicConfig], waitForLeaders: true });
  });
}

export async function deleteTopic(topicName: string): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.deleteTopics({ topics: [topicName] });
  });
}

export async function describeTopic(topicName: string): Promise<{
  partitions: number;
  replication: number;
  retentionMs: number;
  cleanupPolicy: string;
}> {
  return withAdmin(async (admin) => {
    // Get partition/replication info
    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    const topic = metadata.topics[0];
    const partitions = topic?.partitions?.length ?? 1;
    const replication = topic?.partitions?.[0]?.replicas?.length ?? 1;

    // Get topic configs
    const configs = await admin.describeConfigs({
      includeSynonyms: false,
      resources: [{ type: ConfigResourceTypes.TOPIC, name: topicName }],
    });
    const entries = configs.resources[0]?.configEntries ?? [];
    const get = (name: string) =>
      entries.find((e) => e.configName === name)?.configValue ?? "";

    return {
      partitions,
      replication,
      retentionMs: Number(get("retention.ms") || -1),
      cleanupPolicy: get("cleanup.policy") || "delete",
    };
  });
}

export async function increaseTopicPartitions(
  topicName: string,
  newPartitionCount: number
): Promise<void> {
  // Kafka can only ever increase partition count for a topic — decreasing
  // is not supported by the protocol (messages are hash-distributed across
  // partitions; removing one means either data loss or a full migration no
  // Kafka client/CLI exposes as a live operation). Callers must guarantee
  // newPartitionCount is >= the topic's current partition count; the Admin
  // API itself will reject the call otherwise.
  await withAdmin(async (admin) => {
    await admin.createPartitions({
      topicPartitions: [{ topic: topicName, count: newPartitionCount }],
    });
  });
}

export async function updateTopicRetention(
  topicName: string,
  retentionMs: number
): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.alterConfigs({
      validateOnly: false,
      resources: [
        {
          type: ConfigResourceTypes.TOPIC,
          name: topicName,
          configEntries: [
            { name: "retention.ms", value: String(retentionMs) },
          ],
        },
      ],
    });
  });
}

// ── Consumer Group Operations ─────────────────────────────────────────────────

export async function listConsumerGroups(): Promise<string[]> {
  return withAdmin(async (admin) => {
    const { groups } = await admin.listGroups();
    return groups.map((g) => g.groupId);
  });
}

export async function describeConsumerGroup(groupId: string): Promise<{
  groupId: string;
  state: string;
  memberCount: number;
  lag: number;
}> {
  return withAdmin(async (admin) => {
    const described = await admin.describeGroups([groupId]);
    const group = described.groups[0];

    // Get lag by fetching offsets
    let totalLag = 0;
    try {
      const offsets = await admin.fetchOffsets({ groupId, topics: [] });
      const topicNames = Array.from(new Set(offsets.map((o) => o.topic)));
      for (const topic of topicNames) {
        const topicOffsets = await admin.fetchTopicOffsets(topic);
        const groupTopicOffsets = offsets.filter((o) => o.topic === topic);
        for (const partition of topicOffsets) {
          const committed = groupTopicOffsets
            .find((o) => o.topic === topic)
            ?.partitions?.find((p) => p.partition === partition.partition);
          if (committed && committed.offset !== "-1") {
            const lag = Number(partition.high) - Number(committed.offset);
            if (lag > 0) totalLag += lag;
          }
        }
      }
    } catch {
      // Lag calculation is best-effort
    }

    return {
      groupId,
      state: group?.state ?? "unknown",
      memberCount: group?.members?.length ?? 0,
      lag: totalLag,
    };
  });
}

// ── Broker / Service Info ─────────────────────────────────────────────────────

export async function getServiceInfo(): Promise<{
  plan: string;
  state: string;
  nodeCount: number;
  kafkaVersion: string;
  bootstrap: string;
}> {
  return withAdmin(async (admin) => {
    const cluster = await admin.describeCluster();
    return {
      plan: "confluent-for-kubernetes",
      state: "running",
      nodeCount: cluster.brokers.length,
      kafkaVersion: "8.2.0",
      bootstrap: process.env.KAFKA_BOOTSTRAP ?? "localhost:9092",
    };
  });
}

// ── Schema Registry ───────────────────────────────────────────────────────────
// Calls Confluent Schema Registry REST API directly.
// SR is deployed at http://schemaregistry.confluent.svc.cluster.local:8081
// exposed externally via port-forward or a LoadBalancer service.

function srBase(): string {
  return (
    process.env.SCHEMA_REGISTRY_URL ??
    "http://schemaregistry.confluent.svc.cluster.local:8081"
  );
}

export async function listSchemas(): Promise<string[]> {
  try {
    const res = await fetch(`${srBase()}/subjects`, { cache: "no-store" });
    if (!res.ok) return [];
    return await res.json();
  } catch {
    return [];
  }
}

export async function updateSchemaCompatibility(
  subject: string,
  compatibility: "BACKWARD" | "FORWARD" | "FULL" | "NONE"
): Promise<void> {
  const res = await fetch(`${srBase()}/config/${encodeURIComponent(subject)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/vnd.schemaregistry.v1+json" },
    body: JSON.stringify({ compatibility }),
  });
  if (!res.ok)
    throw new Error(
      `Schema Registry updateCompatibility failed: ${res.status} ${await res.text()}`
    );
}

// ── Metrics (KafkaJS-based, no proprietary REST) ──────────────────────────────

export async function getServiceMetrics(): Promise<{
  diskUsedPercent: number | null;
  cpuUsedPercent: number | null;
  memUsedPercent: number | null;
}> {
  // KafkaJS does not expose JMX metrics — return null so the poll loop
  // falls back to topic-size-based estimation (already handled in monitor-poll.ts)
  return { diskUsedPercent: null, cpuUsedPercent: null, memUsedPercent: null };
}

export async function getTopicSizeAndConsumerGroups(topicName: string): Promise<{
  totalSizeBytes: number;
  partitions: Array<{
    id: number;
    sizeBytes: number;
    latestOffset: number;
    earliestOffset: number;
  }>;
  consumerGroups: Array<{ groupId: string; lag: number }>;
}> {
  return withAdmin(async (admin) => {
    const offsets = await admin.fetchTopicOffsets(topicName);
    const partitions = offsets.map((p) => ({
      id: p.partition,
      sizeBytes: 0, // not available via Kafka protocol without JMX
      latestOffset: Number(p.high),
      earliestOffset: Number(p.low),
    }));

    // Get all consumer groups and their lag on this topic
    const { groups } = await admin.listGroups();
    const cgLags: Array<{ groupId: string; lag: number }> = [];

    await Promise.allSettled(
      groups.map(async ({ groupId }) => {
        try {
          const committed = await admin.fetchOffsets({
            groupId,
            topics: [topicName],
          });
          const topicCommitted = committed.find((o) => o.topic === topicName);
          if (!topicCommitted) return;
          let lag = 0;
          for (const p of topicCommitted.partitions) {
            const high = offsets.find(
              (o) => o.partition === p.partition
            )?.high;
            if (high && p.offset !== "-1") {
              lag += Math.max(0, Number(high) - Number(p.offset));
            }
          }
          if (lag > 0) cgLags.push({ groupId, lag });
        } catch {
          // group may not consume this topic
        }
      })
    );

    return { totalSizeBytes: 0, partitions, consumerGroups: cgLags };
  });
}

export async function getClusterTopicSizes(topicNames: string[]): Promise<{
  topicBytes: Record<string, number>;
  totalBytes: number;
  allConsumerGroups: Record<string, number>;
}> {
  const topicBytes: Record<string, number> = {};
  const allConsumerGroups: Record<string, number> = {};

  await Promise.allSettled(
    topicNames.map(async (name) => {
      try {
        const d = await getTopicSizeAndConsumerGroups(name);
        topicBytes[name] = d.totalSizeBytes;
        for (const cg of d.consumerGroups) {
          allConsumerGroups[cg.groupId] =
            (allConsumerGroups[cg.groupId] ?? 0) + cg.lag;
        }
      } catch {
        topicBytes[name] = 0;
      }
    })
  );

  const totalBytes = Object.values(topicBytes).reduce((s, v) => s + v, 0);
  return { topicBytes, totalBytes, allConsumerGroups };
}

// ── Ensure all 7 required topics exist ───────────────────────────────────────

export const REQUIRED_TOPICS: Array<{
  name: string;
  partitions: number;
  replication: number;
  retentionMs: number;
  cleanupPolicy: "delete" | "compact";
}> = [
  { name: "ops.requests.v1",      partitions: 6,  replication: 1, retentionMs: 7   * 86400_000, cleanupPolicy: "delete"  },
  { name: "ops.kafka.metrics.v1", partitions: 6,  replication: 1, retentionMs: 1   * 86400_000, cleanupPolicy: "delete"  },
  { name: "ops.incidents.v1",     partitions: 6,  replication: 1, retentionMs: 30  * 86400_000, cleanupPolicy: "delete"  },
  { name: "ops.actions.audit.v1", partitions: 12, replication: 1, retentionMs: 365 * 86400_000, cleanupPolicy: "delete"  },
  { name: "ops.lessons.v1",       partitions: 3,  replication: 1, retentionMs: -1,              cleanupPolicy: "compact" },
  { name: "ops.notifications.v1", partitions: 3,  replication: 1, retentionMs: 7   * 86400_000, cleanupPolicy: "delete"  },
  { name: "demo.payments.events", partitions: 24, replication: 1, retentionMs: 3   * 86400_000, cleanupPolicy: "delete"  },
];

export async function ensureRequiredTopics(): Promise<{
  created: string[];
  existing: string[];
  errors: string[];
}> {
  const existing = await listTopics();
  const existingSet = new Set(existing);
  const created: string[] = [];
  const errors: string[] = [];

  for (const topic of REQUIRED_TOPICS) {
    if (existingSet.has(topic.name)) continue;
    try {
      await createTopic(topic);
      created.push(topic.name);
    } catch (e) {
      errors.push(
        `${topic.name}: ${e instanceof Error ? e.message : String(e)}`
      );
    }
  }

  return {
    created,
    existing: existing.filter((t) => REQUIRED_TOPICS.some((r) => r.name === t)),
    errors,
  };
}
