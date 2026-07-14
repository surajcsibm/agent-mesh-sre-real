/**
 * real-kafka-client.ts
 * 
 * Direct Kafka protocol client for REAL cluster interactions.
 * This module provides actual Kafka operations - NOT simulations.
 * 
 * Supports:
 * - Confluent Cloud (SASL/PLAIN over TLS)
 * - Aiven (SASL/SCRAM-SHA-256 over TLS)
 * - RedPanda Cloud (SASL/SCRAM-SHA-256 over TLS)
 * - Strimzi on K8s (SASL/SCRAM-SHA-512 with custom CA)
 * 
 * Required env vars:
 *   KAFKA_MODE=real
 *   KAFKA_BOOTSTRAP=host:port
 *   KAFKA_USERNAME=...
 *   KAFKA_PASSWORD=...
 *   KAFKA_SASL_MECHANISM=plain|scram-sha-256|scram-sha-512
 *   KAFKA_SSL_ENABLED=true|false
 *   KAFKA_CA_CERT_BASE64=... (optional, for custom CA)
 */
import "server-only";
import { Kafka, Admin, Producer, Consumer, logLevel, SASLOptions } from "kafkajs";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface RealKafkaConfig {
  bootstrap: string;
  username?: string;
  password?: string;
  saslMechanism: "plain" | "scram-sha-256" | "scram-sha-512";
  sslEnabled: boolean;
  caCertPem?: string;
}

export interface ClusterMetrics {
  brokerCount: number;
  controllerEpoch: number;
  topics: Array<{
    name: string;
    partitions: number;
    replicationFactor: number;
  }>;
  consumerGroups: Array<{
    groupId: string;
    state: string;
    memberCount: number;
    lag: number;
  }>;
  underReplicatedPartitions: number;
  collectedAt: number;
}

export interface ProduceResult {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
}

// ── Configuration ─────────────────────────────────────────────────────────────

function getConfig(): RealKafkaConfig {
  const bootstrap = process.env.KAFKA_BOOTSTRAP || process.env.KAFKA_BOOTSTRAP_SERVERS;
  if (!bootstrap) {
    throw new Error("KAFKA_BOOTSTRAP or KAFKA_BOOTSTRAP_SERVERS must be set for real mode");
  }

  const sslRaw = process.env.KAFKA_SSL_ENABLED?.trim().toLowerCase();
  const sslEnabled = sslRaw === "true";

  const caCertB64 = process.env.KAFKA_CA_CERT_BASE64 || process.env.KAFKA_SSL_CA_B64;
  const caCertPem = caCertB64 
    ? Buffer.from(caCertB64, "base64").toString("utf-8")
    : undefined;

  return {
    bootstrap,
    username: process.env.KAFKA_USERNAME || process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_PASSWORD || process.env.KAFKA_SASL_PASSWORD,
    saslMechanism: (process.env.KAFKA_SASL_MECHANISM?.toLowerCase() || "scram-sha-256") as RealKafkaConfig["saslMechanism"],
    sslEnabled,
    caCertPem,
  };
}

function buildKafkaClient(clientId: string): Kafka {
  const cfg = getConfig();
  
  // Build SSL config
  const ssl = !cfg.sslEnabled
    ? false
    : cfg.caCertPem
      ? { ca: [cfg.caCertPem], rejectUnauthorized: true }
      : true; // Use system trust store

  // Build SASL config
  let sasl: SASLOptions | undefined;
  if (cfg.username && cfg.password) {
    if (cfg.saslMechanism === "plain") {
      sasl = { mechanism: "plain", username: cfg.username, password: cfg.password };
    } else if (cfg.saslMechanism === "scram-sha-256") {
      sasl = { mechanism: "scram-sha-256", username: cfg.username, password: cfg.password };
    } else if (cfg.saslMechanism === "scram-sha-512") {
      sasl = { mechanism: "scram-sha-512", username: cfg.username, password: cfg.password };
    }
  }

  return new Kafka({
    clientId,
    brokers: cfg.bootstrap.split(",").map(b => b.trim()),
    ssl: ssl || undefined,
    sasl,
    logLevel: logLevel.WARN,
    connectionTimeout: 10_000,
    requestTimeout: 30_000,
    retry: { retries: 3, initialRetryTime: 500 },
  });
}

// ── Admin Operations ──────────────────────────────────────────────────────────

async function withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
  const kafka = buildKafkaClient("agent-mesh-sre-admin");
  const admin = kafka.admin();
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.disconnect();
  }
}

/**
 * Collect comprehensive cluster metrics from a real Kafka cluster.
 * This is the primary function for real monitoring.
 */
export async function collectRealClusterMetrics(): Promise<ClusterMetrics> {
  return withAdmin(async (admin) => {
    const collectedAt = Date.now();
    
    // Get cluster info
    const cluster = await admin.describeCluster();
    const brokerCount = cluster.brokers.length;
    const controllerEpoch = cluster.controller ?? -1;

    // Get all topics
    const topicList = await admin.listTopics();
    const userTopics = topicList.filter(t => !t.startsWith("__") && !t.startsWith("_"));
    
    // Get topic metadata
    const topicMeta = await admin.fetchTopicMetadata({ topics: userTopics.slice(0, 20) }); // Limit for performance
    const topics = topicMeta.topics.map(t => ({
      name: t.name,
      partitions: t.partitions.length,
      replicationFactor: t.partitions[0]?.replicas?.length ?? 1,
    }));

    // Count under-replicated partitions
    let underReplicatedPartitions = 0;
    for (const topic of topicMeta.topics) {
      for (const p of topic.partitions) {
        if (p.isr.length < p.replicas.length) {
          underReplicatedPartitions++;
        }
      }
    }

    // Get consumer groups
    const { groups } = await admin.listGroups();
    const consumerGroups: ClusterMetrics["consumerGroups"] = [];

    // Describe groups and calculate lag (limit to first 10 for performance)
    for (const { groupId } of groups.slice(0, 10)) {
      try {
        const described = await admin.describeGroups([groupId]);
        const group = described.groups[0];
        
        // Calculate lag
        let totalLag = 0;
        try {
          const offsets = await admin.fetchOffsets({ groupId, topics: userTopics.slice(0, 5) });
          for (const { topic, partitions } of offsets) {
            const topicOffsets = await admin.fetchTopicOffsets(topic);
            for (const p of partitions) {
              if (p.offset !== "-1") {
                const high = topicOffsets.find(o => o.partition === p.partition)?.high;
                if (high) {
                  totalLag += Math.max(0, Number(high) - Number(p.offset));
                }
              }
            }
          }
        } catch {
          // Lag calculation is best-effort
        }

        consumerGroups.push({
          groupId,
          state: group?.state ?? "unknown",
          memberCount: group?.members?.length ?? 0,
          lag: totalLag,
        });
      } catch {
        // Skip groups we can't describe
      }
    }

    return {
      brokerCount,
      controllerEpoch,
      topics,
      consumerGroups,
      underReplicatedPartitions,
      collectedAt,
    };
  });
}

/**
 * Create a topic on the real cluster.
 */
export async function createRealTopic(opts: {
  name: string;
  partitions: number;
  replicationFactor: number;
  retentionMs?: number;
  cleanupPolicy?: "delete" | "compact";
}): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.createTopics({
      topics: [{
        topic: opts.name,
        numPartitions: opts.partitions,
        replicationFactor: opts.replicationFactor,
        configEntries: [
          ...(opts.retentionMs ? [{ name: "retention.ms", value: String(opts.retentionMs) }] : []),
          ...(opts.cleanupPolicy ? [{ name: "cleanup.policy", value: opts.cleanupPolicy }] : []),
        ],
      }],
      waitForLeaders: true,
    });
  });
}

/**
 * Delete a topic from the real cluster.
 */
export async function deleteRealTopic(topicName: string): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.deleteTopics({ topics: [topicName] });
  });
}

/**
 * List all topics on the real cluster.
 */
export async function listRealTopics(): Promise<string[]> {
  return withAdmin(async (admin) => {
    const topics = await admin.listTopics();
    return topics.filter(t => !t.startsWith("__") && !t.startsWith("_"));
  });
}

/**
 * Describe a consumer group on the real cluster.
 */
export async function describeRealConsumerGroup(groupId: string): Promise<{
  groupId: string;
  state: string;
  memberCount: number;
  lag: number;
  members: Array<{ memberId: string; clientId: string; clientHost: string }>;
}> {
  return withAdmin(async (admin) => {
    const described = await admin.describeGroups([groupId]);
    const group = described.groups[0];

    // Calculate lag
    let totalLag = 0;
    try {
      const topics = await admin.listTopics();
      const userTopics = topics.filter(t => !t.startsWith("__")).slice(0, 10);
      const offsets = await admin.fetchOffsets({ groupId, topics: userTopics });
      
      for (const { topic, partitions } of offsets) {
        const topicOffsets = await admin.fetchTopicOffsets(topic);
        for (const p of partitions) {
          if (p.offset !== "-1") {
            const high = topicOffsets.find(o => o.partition === p.partition)?.high;
            if (high) {
              totalLag += Math.max(0, Number(high) - Number(p.offset));
            }
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
      members: (group?.members ?? []).map(m => ({
        memberId: m.memberId,
        clientId: m.clientId,
        clientHost: m.clientHost,
      })),
    };
  });
}

// ── Producer Operations ───────────────────────────────────────────────────────

let cachedProducer: Producer | null = null;

async function getProducer(): Promise<Producer> {
  if (!cachedProducer) {
    const kafka = buildKafkaClient("agent-mesh-sre-producer");
    cachedProducer = kafka.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
    });
    await cachedProducer.connect();
    console.log("[RealKafka] Producer connected to", getConfig().bootstrap);
  }
  return cachedProducer;
}

/**
 * Produce a message to a real Kafka topic.
 */
export async function produceRealMessage<T>(
  topic: string,
  value: T,
  key?: string,
  headers?: Record<string, string>
): Promise<ProduceResult> {
  const producer = await getProducer();
  const result = await producer.send({
    topic,
    messages: [{
      key: key ?? null,
      value: JSON.stringify(value),
      headers: {
        "content-type": "application/json",
        "source": "agent-mesh-sre",
        ...headers,
      },
      timestamp: String(Date.now()),
    }],
  });

  const record = result[0];
  return {
    topic,
    partition: record.partition,
    offset: record.baseOffset ?? "0",
    timestamp: String(Date.now()),
  };
}

/**
 * Produce multiple messages to a real Kafka topic.
 */
export async function produceRealBatch<T>(
  topic: string,
  messages: Array<{ key?: string; value: T; headers?: Record<string, string> }>
): Promise<ProduceResult[]> {
  const producer = await getProducer();
  const result = await producer.send({
    topic,
    messages: messages.map(m => ({
      key: m.key ?? null,
      value: JSON.stringify(m.value),
      headers: {
        "content-type": "application/json",
        "source": "agent-mesh-sre",
        ...m.headers,
      },
      timestamp: String(Date.now()),
    })),
  });

  return result.map(r => ({
    topic,
    partition: r.partition,
    offset: r.baseOffset ?? "0",
    timestamp: String(Date.now()),
  }));
}

// ── Consumer Operations ───────────────────────────────────────────────────────

export interface ConsumeOptions {
  groupId: string;
  topics: string[];
  fromBeginning?: boolean;
  onMessage: (msg: {
    topic: string;
    partition: number;
    offset: string;
    key: string | null;
    value: unknown;
    headers: Record<string, string>;
    timestamp: string;
  }) => Promise<void> | void;
  onError?: (error: Error) => void;
}

/**
 * Start consuming from real Kafka topics.
 * Returns a disconnect function.
 */
export async function consumeRealMessages(opts: ConsumeOptions): Promise<() => Promise<void>> {
  const kafka = buildKafkaClient(`agent-mesh-sre-consumer-${opts.groupId}`);
  const consumer = kafka.consumer({ groupId: opts.groupId });
  
  await consumer.connect();
  console.log(`[RealKafka] Consumer ${opts.groupId} connected`);

  for (const topic of opts.topics) {
    await consumer.subscribe({ topic, fromBeginning: opts.fromBeginning ?? false });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value ? JSON.parse(message.value.toString()) : null;
        const headers: Record<string, string> = {};
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            headers[k] = v?.toString() ?? "";
          }
        }
        
        await opts.onMessage({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value,
          headers,
          timestamp: message.timestamp,
        });
      } catch (err) {
        opts.onError?.(err as Error);
      }
    },
  });

  return async () => {
    await consumer.disconnect();
    console.log(`[RealKafka] Consumer ${opts.groupId} disconnected`);
  };
}

// ── Scenario Triggers ─────────────────────────────────────────────────────────

/**
 * Trigger a lag spike by producing many messages rapidly.
 * This creates REAL lag on the cluster.
 */
export async function triggerRealLagSpike(
  topic: string,
  messageCount: number = 1000
): Promise<{ produced: number; topic: string }> {
  const producer = await getProducer();
  const messages = Array.from({ length: messageCount }, (_, i) => ({
    key: `lag-spike-${Date.now()}-${i}`,
    value: JSON.stringify({
      type: "lag-spike-payload",
      index: i,
      ts: Date.now(),
      payload: "x".repeat(500), // ~500 bytes per message
    }),
  }));

  await producer.send({ topic, messages });
  console.log(`[RealKafka] Produced ${messageCount} messages to ${topic} for lag spike`);
  
  return { produced: messageCount, topic };
}

/**
 * Get the current lag for a consumer group on a specific topic.
 */
export async function getRealConsumerLag(
  groupId: string,
  topic: string
): Promise<{ groupId: string; topic: string; lag: number; partitions: Array<{ partition: number; lag: number }> }> {
  return withAdmin(async (admin) => {
    const topicOffsets = await admin.fetchTopicOffsets(topic);
    const groupOffsets = await admin.fetchOffsets({ groupId, topics: [topic] });
    
    const topicGroup = groupOffsets.find(o => o.topic === topic);
    const partitions: Array<{ partition: number; lag: number }> = [];
    let totalLag = 0;

    for (const p of topicOffsets) {
      const committed = topicGroup?.partitions?.find(gp => gp.partition === p.partition);
      const committedOffset = committed?.offset !== "-1" ? Number(committed?.offset ?? 0) : 0;
      const highOffset = Number(p.high);
      const lag = Math.max(0, highOffset - committedOffset);
      partitions.push({ partition: p.partition, lag });
      totalLag += lag;
    }

    return { groupId, topic, lag: totalLag, partitions };
  });
}

// ── Health Check ──────────────────────────────────────────────────────────────

/**
 * Check if the real Kafka cluster is reachable.
 */
export async function checkRealClusterHealth(): Promise<{
  healthy: boolean;
  brokerCount: number;
  controllerId: number;
  error?: string;
}> {
  try {
    const kafka = buildKafkaClient("agent-mesh-sre-health");
    const admin = kafka.admin();
    await admin.connect();
    
    try {
      const cluster = await admin.describeCluster();
      return {
        healthy: true,
        brokerCount: cluster.brokers.length,
        controllerId: cluster.controller ?? -1,
      };
    } finally {
      await admin.disconnect();
    }
  } catch (err) {
    return {
      healthy: false,
      brokerCount: 0,
      controllerId: -1,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

// ── Cleanup ───────────────────────────────────────────────────────────────────

export async function disconnectAll(): Promise<void> {
  if (cachedProducer) {
    await cachedProducer.disconnect();
    cachedProducer = null;
    console.log("[RealKafka] Producer disconnected");
  }
}
