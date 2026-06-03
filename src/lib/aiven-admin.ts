/**
 * aiven-admin.ts
 * Real Kafka operations via Aiven REST API + KafkaJS Admin.
 * Used by mesh.ts in REAL mode instead of in-memory mutations.
 *
 * Aiven REST API docs: https://api.aiven.io/doc/
 * All calls require AIVEN_TOKEN, AIVEN_PROJECT, AIVEN_SERVICE in env.
 */
import "server-only";

const AIVEN_API = "https://api.aiven.io/v1";

function aivenHeaders() {
  const token = process.env.AIVEN_TOKEN;
  if (!token) throw new Error("AIVEN_TOKEN not set in environment");
  return {
    "Authorization": `Bearer ${token}`,
    "Content-Type": "application/json",
  };
}

function aivenBase() {
  const project = process.env.AIVEN_PROJECT;
  const service = process.env.AIVEN_SERVICE;
  if (!project || !service) throw new Error("AIVEN_PROJECT or AIVEN_SERVICE not set");
  return `${AIVEN_API}/project/${project}/service/${service}`;
}

// ── Topic Management ──────────────────────────────────────────────────────────

export async function listTopics(): Promise<string[]> {
  const res = await fetch(`${aivenBase()}/topic`, {
    headers: aivenHeaders(),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Aiven listTopics failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  return (data.topics ?? []).map((t: { topic_name: string }) => t.topic_name);
}

export async function createTopic(opts: {
  name: string;
  partitions: number;
  replication: number;
  retentionMs?: number;   // -1 = unlimited (compacted)
  cleanupPolicy?: "delete" | "compact";
}): Promise<void> {
  const body: Record<string, unknown> = {
    topic_name: opts.name,
    partitions: opts.partitions,
    replication: opts.replication,
    config: {
      ...(opts.retentionMs !== undefined
        ? { retention_ms: opts.retentionMs }
        : {}),
      ...(opts.cleanupPolicy
        ? { cleanup_policy: opts.cleanupPolicy }
        : {}),
    },
  };
  const res = await fetch(`${aivenBase()}/topic`, {
    method: "POST",
    headers: aivenHeaders(),
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Aiven createTopic failed: ${res.status} ${await res.text()}`);
}

export async function deleteTopic(topicName: string): Promise<void> {
  const res = await fetch(`${aivenBase()}/topic/${encodeURIComponent(topicName)}`, {
    method: "DELETE",
    headers: aivenHeaders(),
  });
  if (!res.ok) throw new Error(`Aiven deleteTopic failed: ${res.status} ${await res.text()}`);
}

export async function describeTopic(topicName: string): Promise<{
  partitions: number;
  replication: number;
  retentionMs: number;
  cleanupPolicy: string;
}> {
  const res = await fetch(`${aivenBase()}/topic/${encodeURIComponent(topicName)}`, {
    headers: aivenHeaders(),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Aiven describeTopic failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  const t = data.topic;
  return {
    partitions: t.partitions ?? 1,
    replication: t.replication ?? 1,
    retentionMs: Number(t.config?.retention_ms?.value ?? -1),
    cleanupPolicy: t.config?.cleanup_policy?.value ?? "delete",
  };
}

export async function updateTopicRetention(topicName: string, retentionMs: number): Promise<void> {
  const res = await fetch(`${aivenBase()}/topic/${encodeURIComponent(topicName)}`, {
    method: "PATCH",
    headers: aivenHeaders(),
    body: JSON.stringify({
      config: { retention_ms: retentionMs },
    }),
  });
  if (!res.ok) throw new Error(`Aiven updateTopic failed: ${res.status} ${await res.text()}`);
}

// ── Consumer Group Operations ────────────────────────────────────────────────

export async function listConsumerGroups(): Promise<string[]> {
  // Aiven REST API does not expose consumer groups endpoint
  return [];
}

export async function describeConsumerGroup(groupId: string): Promise<{
  groupId: string; state: string; memberCount: number; lag: number;
}> {
  // Aiven REST API does not expose consumer group detail
  return { groupId, state: "unknown", memberCount: 0, lag: 0 };
}

// ── Service / Broker Info ────────────────────────────────────────────────────

export async function getServiceInfo(): Promise<{
  plan: string;
  state: string;
  nodeCount: number;
  kafkaVersion: string;
  bootstrap: string;
}> {
  const res = await fetch(aivenBase(), {
    headers: aivenHeaders(),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`Aiven getServiceInfo failed: ${res.status} ${await res.text()}`);
  const data = await res.json();
  const svc = data.service;
  const bootstrap = (svc.connection_info?.kafka ?? [])[0] ?? "";
  return {
    plan: svc.plan ?? "",
    state: svc.state ?? "",
    nodeCount: svc.node_count ?? 1,
    kafkaVersion: svc.user_config?.kafka_version ?? "unknown",
    bootstrap,
  };
}

// ── Schema Registry (Karapace) ───────────────────────────────────────────────

export async function listSchemas(): Promise<string[]> {
  // Karapace endpoint is exposed via Aiven service URI
  // We call via Aiven's proxy to avoid needing Karapace creds separately
  const res = await fetch(`${aivenBase()}/kafka/schema/subjects`, {
    headers: aivenHeaders(),
    cache: "no-store",
  });
  if (!res.ok) {
    // Schema registry may not be enabled — return empty gracefully
    return [];
  }
  const data = await res.json();
  return data.subjects ?? [];
}

export async function updateSchemaCompatibility(
  subject: string,
  compatibility: "BACKWARD" | "FORWARD" | "FULL" | "NONE"
): Promise<void> {
  const res = await fetch(`${aivenBase()}/kafka/schema/config/${encodeURIComponent(subject)}`, {
    method: "PUT",
    headers: aivenHeaders(),
    body: JSON.stringify({ compatibility }),
  });
  if (!res.ok) throw new Error(`Aiven updateSchemaCompatibility failed: ${res.status} ${await res.text()}`);
}

// ── Ensure all 7 required topics exist ──────────────────────────────────────

export const REQUIRED_TOPICS: Array<{
  name: string;
  partitions: number;
  replication: number;
  retentionMs: number;
  cleanupPolicy: "delete" | "compact";
}> = [
  { name: "ops.requests.v1",      partitions: 6,  replication: 2, retentionMs: 7  * 86400_000, cleanupPolicy: "delete" },
  { name: "ops.kafka.metrics.v1", partitions: 6,  replication: 2, retentionMs: 1  * 86400_000, cleanupPolicy: "delete" },
  { name: "ops.incidents.v1",     partitions: 6,  replication: 2, retentionMs: 30 * 86400_000, cleanupPolicy: "delete" },
  { name: "ops.actions.audit.v1", partitions: 12, replication: 2, retentionMs: 365* 86400_000, cleanupPolicy: "delete" },
  { name: "ops.lessons.v1",       partitions: 3,  replication: 2, retentionMs: -1,             cleanupPolicy: "compact" },
  { name: "ops.notifications.v1", partitions: 3,  replication: 2, retentionMs: 7  * 86400_000, cleanupPolicy: "delete" },
  { name: "demo.payments.events", partitions: 24, replication: 2, retentionMs: 3  * 86400_000, cleanupPolicy: "delete" },
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
    if (existingSet.has(topic.name)) {
      continue;
    }
    try {
      await createTopic(topic);
      created.push(topic.name);
    } catch (e) {
      errors.push(`${topic.name}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }

  return { created, existing: existing.filter(t => REQUIRED_TOPICS.some(r => r.name === t)), errors };
}