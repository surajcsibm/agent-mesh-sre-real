/**
 * In-memory simulation of an Apache Kafka cluster running in KRaft mode.
 *
 * Models:
 *  - Topics with partitions and per-partition log-end offsets
 *  - Producers: append() returns the new offset
 *  - Consumers: subscribed groups with last-committed-offsets
 *  - Replay: a killed consumer's last-committed offset persists, so when a
 *    new consumer starts it can resume from that offset
 *
 * This is intentionally NOT a wire-faithful Kafka clone; it's just enough to
 * make the proposal's narrative observable on stage:
 *
 *    "When the agent restarts, it replays from its last committed offset
 *     and catches up with zero data loss."
 */

import type { ClusterStatus, KafkaRecord, TopicName } from "./types";

interface PartitionLog<T = unknown> {
  records: KafkaRecord<T>[];
}

interface ConsumerCheckpoint {
  /** Per-partition last-committed offset. Index 0 = partition 0. */
  committed: number[];
}

export class BrokerSim {
  private partitions: Record<TopicName, PartitionLog[]> = {} as never;
  private partitionCount: Record<TopicName, number> = {
    "ops.requests.v1": 3,
    "ops.kafka.metrics.v1": 6,
    "ops.incidents.v1": 3,
    "ops.actions.audit.v1": 3,
    "ops.lessons.v1": 1,
    "ops.notifications.v1": 1,
  };
  private cluster: ClusterStatus = {
    mode: "KRaft",
    controllerId: 1,
    controllerEpoch: 14,
    brokers: [
      { id: 1, rack: "us-east-1a", status: "online" },
      { id: 2, rack: "us-east-1b", status: "online" },
      { id: 3, rack: "us-east-1c", status: "online" },
    ],
    schemaRegistry: { connected: true, specs: 6 },
    security: { mTLS: true, saslScram: true, aclsActive: 18 },
    brokersOnline: 3,
    mtls: true,
    sasl: true,
    aclCount: 18,
  };
  /** consumerGroup -> topic -> committed offsets */
  private commits: Record<string, Partial<Record<TopicName, ConsumerCheckpoint>>> = {};

  constructor() {
    for (const [topic, count] of Object.entries(this.partitionCount) as [TopicName, number][]) {
      this.partitions[topic] = Array.from({ length: count }, () => ({ records: [] }));
    }
  }

  /* ------------------------------------------------------------------ */
  /* Cluster                                                            */
  /* ------------------------------------------------------------------ */

  getClusterStatus(): ClusterStatus {
    return JSON.parse(JSON.stringify(this.cluster));
  }

  forceControllerFailover(): { from: number; to: number; epoch: number } {
    const from = this.cluster.controllerId ?? 1;
    const candidates = (this.cluster.brokers ?? []).map((b) => b.id).filter((id) => id !== from);
    const to = candidates[Math.floor(Math.random() * candidates.length)] ?? from;
    this.cluster.controllerId = to;
    this.cluster.controllerEpoch += 1;
    return { from, to, epoch: this.cluster.controllerEpoch };
  }

  /* ------------------------------------------------------------------ */
  /* Producer                                                           */
  /* ------------------------------------------------------------------ */

  append<T>(topic: TopicName, key: string, value: T, headers: Record<string, string> = {}): KafkaRecord<T> {
    const pCount = this.partitionCount[topic];
    const partition = this.hash(key) % pCount;
    const log = this.partitions[topic][partition];
    const record: KafkaRecord<T> = {
      topic,
      partition,
      offset: log.records.length,
      ts: Date.now(),
      key,
      value,
      timestamp: Date.now(),
      headers: { schema: `${topic}#1`, ...headers },
    };
    log.records.push(record);
    return record;
  }

  /* ------------------------------------------------------------------ */
  /* Consumer state                                                     */
  /* ------------------------------------------------------------------ */

  ensureGroup(group: string, topic: TopicName): void {
    if (!this.commits[group]) this.commits[group] = {};
    if (!this.commits[group][topic]) {
      this.commits[group][topic] = {
        committed: Array(this.partitionCount[topic]).fill(0),
      };
    }
  }

  /**
   * Return all records appended after the consumer's last-committed offsets,
   * up to a maxBatch size, and (optionally) advance the committed offsets.
   * If commit=false, the consumer can choose to commit later via commitOffsets.
   */
  poll(group: string, topic: TopicName, maxBatch = 200, commit = false): KafkaRecord[] {
    this.ensureGroup(group, topic);
    const cp = this.commits[group][topic]!;
    const out: KafkaRecord[] = [];
    for (let p = 0; p < this.partitionCount[topic]; p++) {
      const log = this.partitions[topic][p].records;
      while (cp.committed[p] < log.length && out.length < maxBatch) {
        out.push(log[cp.committed[p]]);
        cp.committed[p] += 1;
        if (!commit) {
          // We've read but the in-memory cursor moves; if commit=false we still need
          // to remember for the next poll. To honour Kafka semantics where a crash
          // before commit returns the same records, we keep a separate "read" cursor.
          // For simplicity here, "in-flight" tracking is done by the agent itself.
        }
      }
    }
    if (commit) {
      // commits are already advanced
    } else {
      // Roll back the committed cursor - the agent must commit explicitly
      const seen = out.length;
      // Reset committed offsets back: track in-flight per partition
      // Easiest: store snapshot before, restore
      // But we already mutated. Recompute by subtracting per-partition counts.
      const perPartition: Record<number, number> = {};
      for (const r of out) perPartition[r.partition] = (perPartition[r.partition] ?? 0) + 1;
      for (const [pStr, n] of Object.entries(perPartition)) {
        const p = Number(pStr);
        cp.committed[p] -= n;
      }
      // We expose the records but require the agent to commitOffsets later.
      // (seen is just to silence lints.)
      void seen;
    }
    return out;
  }

  commitOffsets(group: string, topic: TopicName, records: KafkaRecord[]): void {
    this.ensureGroup(group, topic);
    const cp = this.commits[group][topic]!;
    const max: Record<number, number> = {};
    for (const r of records) {
      max[r.partition] = Math.max(max[r.partition] ?? -1, r.offset);
    }
    for (const [pStr, off] of Object.entries(max)) {
      const p = Number(pStr);
      cp.committed[p] = Math.max(cp.committed[p], off + 1);
    }
  }

  getCommittedOffsets(group: string, topic: TopicName): number[] {
    this.ensureGroup(group, topic);
    return [...this.commits[group][topic]!.committed];
  }

  getLogEndOffsets(topic: TopicName): number[] {
    return this.partitions[topic].map((p) => p.records.length);
  }

  /** Sum of (log end - committed) per topic = consumer lag in messages. */
  getLag(group: string, topic: TopicName): number {
    this.ensureGroup(group, topic);
    const cp = this.commits[group][topic]!;
    return this.partitions[topic].reduce(
      (acc, log, p) => acc + Math.max(0, log.records.length - cp.committed[p]),
      0
    );
  }

  /** Recent records across all partitions, newest first. */
  recent(topic: TopicName, count = 50): KafkaRecord[] {
    const merged: KafkaRecord[] = [];
    for (const p of this.partitions[topic]) merged.push(...p.records);
    merged.sort((a, b) => (b.timestamp ?? 0) - (a.timestamp ?? 0));
    return merged.slice(0, count);
  }

  /** Reset every topic + commit (used by /api/reset). */
  resetAll(): void {
    for (const topic of Object.keys(this.partitions) as TopicName[]) {
      for (const log of this.partitions[topic]) log.records = [];
    }
    this.commits = {};
    this.cluster.controllerId = 1;
    this.cluster.controllerEpoch = 14;
    for (const b of (this.cluster.brokers ?? [])) b.status = "online";
  }

  private hash(s: string): number {
    let h = 0;
    for (let i = 0; i < s.length; i++) {
      h = (h * 31 + s.charCodeAt(i)) | 0;
    }
    return Math.abs(h);
  }
}
