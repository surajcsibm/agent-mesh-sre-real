/**
 * monitor-poll.ts
 * Autonomous Monitor Agent polling loop.
 *
 * Runs every POLL_MS (30 s) collecting a MetricSnapshot from:
 *   • Aiven REST API  (disk metrics, topic sizes, consumer group lag from partitions)
 *   • KafkaJS admin   (ISR, controller epoch, consumer group state)
 *   • Mesh broker sim (MOCK mode — reads the in-memory broker state directly)
 *
 * Maintains a sliding window of WINDOW_SIZE snapshots (≈ 5 min of history).
 * Evaluates all 11 trigger conditions each cycle. When a condition fires it:
 *   1. Checks the per-scenario cooldown (COOLDOWN_MS = 5 min)
 *   2. Checks the deduplication set (no two concurrent runs of the same scenario)
 *   3. Emits a "monitor-detect" SSE event with cause + confidence
 *   4. For the 4 core MRAL scenarios, calls triggerScenario() to run the full loop
 *   5. For extended scenarios 05–11, emits an anomaly audit record (full MRAL TBD)
 */
import "server-only";
import { getRuntime } from "./runtime-mode";
import { eventBus } from "./event-bus";
import { safeErr } from "./log-safe";

// ── Types ─────────────────────────────────────────────────────────────────────

export type PollScenarioId =
  | "lag-spike"
  | "controller-failover"
  | "share-group"
  | "benign-rebalance"
  | "schema-mismatch"
  | "disk-saturation"
  | "under-replication"
  | "producer-timeout"
  | "consumer-session-timeout"
  | "compaction-lag"
  | "partition-imbalance";

export interface MetricSnapshot {
  ts: number;
  controllerEpoch: number;
  brokersOnline: number;
  /** groupId → metrics */
  consumerGroups: Record<
    string,
    { lag: number; memberCount: number; state: string }
  >;
  underReplicatedPartitions: number;
  /** null = not available (Aiven plan limitation) */
  diskUsedPercent: number | null;
  /** topicName → total partition log bytes */
  topicSizeBytes: Record<string, number>;
  /** subject → version count (for schema mismatch detection) */
  schemaVersionCounts: Record<string, number>;
}

export interface TriggerResult {
  scenarioId: PollScenarioId;
  triggered: boolean;
  confidence: number;
  cause: string;
  gate: "approval" | "auto" | "suppress";
}

export interface PollLoopState {
  running: boolean;
  cycleCount: number;
  lastPollAt: number | null;
  lastError: string | null;
  snapshotCount: number;
  cooldowns: Record<PollScenarioId, number>; // ts of last trigger
  detectedThisCycle: TriggerResult[];
}

// ── Constants ─────────────────────────────────────────────────────────────────

const POLL_MS = 30_000;
const WINDOW_SIZE = 10; // 5 min history
const COOLDOWN_MS = 5 * 60_000;

// ── Module-level state ────────────────────────────────────────────────────────

declare global {
  var __monitorPoll: {
    timer: NodeJS.Timeout | null;
    state: PollLoopState;
    history: MetricSnapshot[];
    recentDetections: Array<TriggerResult & { ts: number }>;
    activeScenarios: Set<PollScenarioId>;
  } | undefined;
}

// Real Share-Group queue-depth, populated by a small in-cluster poller
// (share-group-poller Deployment) publishing to ops.kafka.metrics.v1, since
// classic KafkaJS consumer-group APIs (describeGroups/fetchOffsets) do not
// apply to KIP-932 share groups at all — confirmed this session; they use a
// completely different broker coordinator. This is a long-lived background
// subscriber (unlike every other real-mode call in this codebase, which uses
// a fresh client per call) because KafkaJS consumers are push/subscription-
// based — there is no clean "fetch the one latest message" primitive to call
// synchronously each poll cycle, so a cached value updated by an ongoing
// subscription is the natural fit here instead.
let latestShareGroupStatus: { totalLag: number; ts: number } | null = null;
let shareGroupSubscriberStarted = false;

async function startShareGroupStatusSubscriber(): Promise<void> {
  if (shareGroupSubscriberStarted) return;
  shareGroupSubscriberStarted = true;
  try {
    const { getMeshConsumer } = await import("./kafka");
    const consumer = await getMeshConsumer("agent-mesh-sre-monitor-sharegroup");
    await consumer.subscribe(["ops.kafka.metrics.v1"], (msg) => {
      const v = msg.value as { type?: string; group?: string; totalLag?: number; ts?: number } | null;
      if (v?.type === "share-group-status" && typeof v.totalLag === "number") {
        latestShareGroupStatus = { totalLag: v.totalLag, ts: v.ts ?? Date.now() };
      }
    });
    console.log("[MonitorPoll] Share-group status subscriber connected");
  } catch (e) {
    shareGroupSubscriberStarted = false;
    console.warn("[MonitorPoll] Share-group status subscriber failed to start:", (e as Error).message);
  }
}

function getPollGlobal() {
  if (!globalThis.__monitorPoll) {
    globalThis.__monitorPoll = {
      timer: null,
      state: {
        running: false,
        cycleCount: 0,
        lastPollAt: null,
        lastError: null,
        snapshotCount: 0,
        cooldowns: {} as Record<PollScenarioId, number>,
        detectedThisCycle: [],
      },
      history: [],
      recentDetections: [] as Array<TriggerResult & { ts: number }>,
      activeScenarios: new Set(),
    };
  }
  return globalThis.__monitorPoll;
}

// ── Metric collection ─────────────────────────────────────────────────────────

async function collectSnapshot(): Promise<MetricSnapshot> {
  const rt = getRuntime();
  const snap: MetricSnapshot = {
    ts: Date.now(),
    controllerEpoch: -1,
    brokersOnline: 1,
    consumerGroups: {},
    underReplicatedPartitions: 0,
    diskUsedPercent: null,
    topicSizeBytes: {},
    schemaVersionCounts: {},
  };

  // Real Share-Group data — merged in real mode only, from the cached value
  // kept updated by the background subscriber above. Only overwrites the mock
  // baseline if we have a genuinely fresh reading (within 2 poll cycles' worth
  // of time), so a dead/disconnected poller falls back to mock state instead
  // of freezing on a stale number forever.
  if (getRuntime().mode === "real" && latestShareGroupStatus && Date.now() - latestShareGroupStatus.ts < POLL_MS * 2) {
    snap.consumerGroups["demo-share-group"] = {
      lag: latestShareGroupStatus.totalLag,
      memberCount: 1,
      state: "stable",
    };
  }

  // ── ALWAYS read the in-memory broker simulation as baseline ──────────────
  // Ensures controllerEpoch and consumerGroups reflect scenario trigger state
  // (lag-spike sets lag=24000, failover increments epoch, etc.) regardless of
  // KAFKA_MODE — so the polling loop can detect and autonomously re-fire them.
  try {
    const meshMod = await import("./mesh");
    const meshSnap = meshMod.getSnapshot();
    snap.controllerEpoch = meshSnap.broker.controllerEpoch;
    snap.brokersOnline   = meshSnap.broker.brokersOnline;
    for (const [id, cg] of Object.entries(meshSnap.broker.consumerGroups)) {
      const g = cg as { lag: number; members: number; rebalanceState: string };
      snap.consumerGroups[id] = {
        lag:         g.lag,
        memberCount: g.members,
        state:       g.rebalanceState,
      };
    }
  } catch (e) {
    console.warn("[MonitorPoll] Mesh state read error:", (e as Error).message);
  }

  if (rt.mode === "real") {
    // ── Primary: Use real-kafka-client for comprehensive metrics ─────────────
    try {
      const { collectRealClusterMetrics } = await import("./real-kafka-client");
      const realMetrics = await collectRealClusterMetrics();
      
      snap.controllerEpoch = realMetrics.controllerEpoch;
      snap.brokersOnline = realMetrics.brokerCount;
      snap.underReplicatedPartitions = realMetrics.underReplicatedPartitions;
      
      for (const cg of realMetrics.consumerGroups) {
        snap.consumerGroups[cg.groupId] = {
          lag: cg.lag,
          memberCount: cg.memberCount,
          state: cg.state,
        };
      }
      
      console.log(`[MonitorPoll] REAL metrics: ${realMetrics.brokerCount} brokers, ${realMetrics.consumerGroups.length} consumer groups`);
    } catch (e) {
      console.warn("[MonitorPoll] real-kafka-client error:", (e as Error).message);
      
      // ── Fallback: KafkaJS admin — supplements ISR + real consumer groups ────
      try {
        const { getKafkaAdminMetrics } = await import("./kafka-admin");
        const admin = await getKafkaAdminMetrics();
        if (admin.controllerEpoch > 0) snap.controllerEpoch = admin.controllerEpoch;
        if (admin.brokersOnline  > 1) snap.brokersOnline   = admin.brokersOnline;
        snap.underReplicatedPartitions = admin.underReplicatedPartitions;
        for (const [id, cg] of Object.entries(admin.consumerGroups)) {
          snap.consumerGroups[id] = cg;
        }
      } catch (e2) {
        console.warn("[MonitorPoll] KafkaAdmin fallback error:", (e2 as Error).message);
      }
    }

    // ── Supplemental: Topic sizes + schema registry ──────────────────────────
    try {
      const { getServiceMetrics, getClusterTopicSizes, listTopics, listSchemas } =
        await import("./kafka-admin-cfk");

      const diskMetrics = await getServiceMetrics();
      snap.diskUsedPercent = diskMetrics.diskUsedPercent;

      const topics = await listTopics();
      const topicData = await getClusterTopicSizes(topics);
      snap.topicSizeBytes = topicData.topicBytes;

      for (const [groupId, lag] of Object.entries(topicData.allConsumerGroups)) {
        if (!snap.consumerGroups[groupId]) {
          snap.consumerGroups[groupId] = { lag, memberCount: 1, state: "unknown" };
        } else if (lag > snap.consumerGroups[groupId].lag) {
          snap.consumerGroups[groupId].lag = lag;
        }
      }

      const subjects = await listSchemas();
      snap.schemaVersionCounts = Object.fromEntries(subjects.map((s) => [s, 1]));
    } catch (e) {
      // Topic sizes and schema registry are optional
    }
  }

  return snap;
}

// ── Trigger condition evaluators ──────────────────────────────────────────────

function evalLagSpike(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "lag-spike",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "approval",
  };
  const cg = snap.consumerGroups["payments-consumer"];
  if (!cg || cg.lag < 5_000) return base;

  if (history.length >= 2) {
    const prev = history[history.length - 1].consumerGroups["payments-consumer"];
    const prev2 =
      history.length >= 3
        ? history[history.length - 2].consumerGroups["payments-consumer"]
        : null;
    const growing = prev && cg.lag > prev.lag;
    const sustained = prev2 && prev && prev.lag > prev2.lag && cg.lag > prev.lag;

    if (cg.lag > 10_000 && sustained) {
      const delta = cg.lag - (prev2?.lag ?? 0);
      return {
        ...base,
        triggered: true,
        confidence: 0.88,
        cause: `payments-consumer lag ${cg.lag.toLocaleString()} msgs, growing +${delta.toLocaleString()} over 3 samples`,
      };
    }
    if (cg.lag > 20_000 && growing) {
      return {
        ...base,
        triggered: true,
        confidence: 0.82,
        cause: `payments-consumer lag ${cg.lag.toLocaleString()} msgs exceeds critical threshold`,
      };
    }
  }
  // Single sample over hard threshold
  if (cg.lag > 30_000) {
    return {
      ...base,
      triggered: true,
      confidence: 0.80,
      cause: `payments-consumer lag ${cg.lag.toLocaleString()} msgs — hard threshold exceeded`,
    };
  }
  return base;
}

function evalControllerFailover(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "controller-failover",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "auto",
  };
  if (history.length < 1 || snap.controllerEpoch <= 0) return base;
  const prev = history[history.length - 1];
  if (prev.controllerEpoch > 0 && snap.controllerEpoch > prev.controllerEpoch) {
    return {
      ...base,
      triggered: true,
      confidence: 0.97,
      cause: `KRaft controller epoch ${prev.controllerEpoch} → ${snap.controllerEpoch}`,
    };
  }
  return base;
}

function evalShareGroup(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "share-group",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "approval",
  };
  const sg = snap.consumerGroups["demo-share-group"];
  // TEMP-DEMO-THRESHOLD: lowered from 5_000/10_000 to 3/5 for live testing
  // tonight against real (but low-volume) cluster traffic. REVERT before
  // July 22 — restore the original thresholds below once testing is done.
  // ORIGINAL: if (!sg || sg.lag < 5_000) return base;
  if (!sg || sg.lag < 3) return base;

  const prev = history.length
    ? history[history.length - 1].consumerGroups["demo-share-group"]
    : null;
  const growing = !prev || sg.lag >= prev.lag;
  // ORIGINAL: if (sg.lag > 10_000 && growing) {
  if (sg.lag > 5 && growing) {
    return {
      ...base,
      triggered: true,
      confidence: 0.84,
      cause: `demo-share-group queue depth ${sg.lag.toLocaleString()} records, growing`,
    };
  }
  return base;
}

function evalBenignRebalance(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  // SUPPRESS: rebalance in progress but lag not growing
  const base: TriggerResult = {
    scenarioId: "benign-rebalance",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "suppress",
  };
  const cg = snap.consumerGroups["payments-consumer"];
  if (!cg) return base;

  const rebalancing =
    cg.state === "preparing-rebalance" || cg.state === "rebalancing";
  if (!rebalancing) return base;

  const prevLag =
    history.length
      ? (history[history.length - 1].consumerGroups["payments-consumer"]?.lag ?? cg.lag)
      : cg.lag;
  const lagStable = Math.abs(cg.lag - prevLag) < 500;

  if (lagStable && cg.lag < 5_000) {
    return {
      ...base,
      triggered: true,
      confidence: 0.91,
      cause: `Rebalance (${cg.state}) with stable lag ${cg.lag} — KIP-848 false positive suppressed`,
    };
  }
  return base;
}

function evalSchemaMismatch(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "schema-mismatch",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "approval",
  };
  if (!history.length) return base;
  const prev = history[history.length - 1];
  for (const [subject, count] of Object.entries(snap.schemaVersionCounts)) {
    const prevCount = prev.schemaVersionCounts[subject] ?? 0;
    if (count > prevCount + 1) {
      return {
        ...base,
        triggered: true,
        confidence: 0.78,
        cause: `Schema subject '${subject}' jumped ${prevCount}→${count} versions in one cycle`,
      };
    }
  }
  return base;
}

function evalDiskSaturation(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "disk-saturation",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "auto",
  };

  if (snap.diskUsedPercent !== null) {
    if (snap.diskUsedPercent >= 90) {
      return {
        ...base,
        triggered: true,
        confidence: 0.95,
        cause: `Disk ${snap.diskUsedPercent.toFixed(1)}% used — critical threshold`,
      };
    }
    if (snap.diskUsedPercent >= 80) {
      const prevPct = history.length
        ? (history[history.length - 1].diskUsedPercent ?? 0)
        : 0;

      if (snap.diskUsedPercent > prevPct) {
        return {
          ...base,
          triggered: true,
          confidence: 0.82,
          cause: `Disk ${snap.diskUsedPercent.toFixed(1)}% used (↑ from ${prevPct.toFixed(1)}%) — warning`,
        };
      }
    }
    return base;
  }

  // Fallback: estimate disk pressure from topic log sizes
  const totalBytes = Object.values(snap.topicSizeBytes).reduce((s, v) => s + v, 0);
  if (totalBytes > 500 * 1024 * 1024) {
    // > 500 MB of logs — proxy signal on free-tier cluster
    return {
      ...base,
      triggered: true,
      confidence: 0.60,
      cause: `Topic log total ${(totalBytes / 1024 / 1024).toFixed(0)} MB — disk pressure estimated`,
    };
  }
  return base;
}

function evalUnderReplication(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "under-replication",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "approval",
  };
  if (snap.underReplicatedPartitions <= 0) return base;

  // Require 2 consecutive samples (≥30s sustained) to avoid restart spikes
  const prevUrp = history.length
    ? (history[history.length - 1].underReplicatedPartitions ?? 0)
    : 0;
  if (prevUrp > 0) {
    return {
      ...base,
      triggered: true,
      confidence: 0.93,
      cause: `${snap.underReplicatedPartitions} under-replicated partition(s) sustained ≥ 30s`,
    };
  }
  return base;
}

function evalProducerTimeout(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "producer-timeout",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "auto",
  };
  if (!history.length) return base;

  // Proxy: broker count drops unexpectedly
  const prevBrokers = history[history.length - 1].brokersOnline;
  if (snap.brokersOnline < prevBrokers && snap.brokersOnline < 2) {
    return {
      ...base,
      triggered: true,
      confidence: 0.72,
      cause: `Broker count ${prevBrokers}→${snap.brokersOnline} — producer timeouts likely`,
    };
  }

  // Proxy: offsetHigh for payments topic stalled while consumer lag grows
  const payBytes = snap.topicSizeBytes["demo.payments.events"] ?? 0;
  const prevPayBytes =
    history[history.length - 1].topicSizeBytes["demo.payments.events"] ?? 0;
  const cgLag =
    snap.consumerGroups["payments-consumer"]?.lag ?? 0;
  const prevCgLag =
    history[history.length - 1].consumerGroups["payments-consumer"]?.lag ?? 0;

  if (payBytes > 0 && payBytes === prevPayBytes && cgLag > prevCgLag + 1_000) {
    return {
      ...base,
      triggered: true,
      confidence: 0.68,
      cause: `Topic log stalled (${payBytes} bytes) while consumer lag grew +${cgLag - prevCgLag} — possible producer timeout`,
    };
  }
  return base;
}

function evalConsumerSessionTimeout(
  snap: MetricSnapshot,
  _history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "consumer-session-timeout",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "auto",
  };
  for (const [groupId, cg] of Object.entries(snap.consumerGroups)) {
    const dead = cg.state.toLowerCase() === "dead";
    if (dead) {
      return {
        ...base,
        triggered: true,
        confidence: 0.92,
        cause: `Consumer group '${groupId}' state = Dead (session timeout)`,
      };
    }
  }
  // Member count drop detection
  if (_history.length) {
    const prev = _history[_history.length - 1].consumerGroups;
    for (const [groupId, cg] of Object.entries(snap.consumerGroups)) {
      const prevMembers = prev[groupId]?.memberCount ?? cg.memberCount;
      if (cg.memberCount < prevMembers && cg.memberCount === 0) {
        return {
          ...base,
          triggered: true,
          confidence: 0.88,
          cause: `Consumer group '${groupId}' dropped from ${prevMembers}→0 members`,
        };
      }
    }
  }
  return base;
}

function evalCompactionLag(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  const base: TriggerResult = {
    scenarioId: "compaction-lag",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "auto",
  };
  if (history.length < 2) return base;

  const lessonsNow = snap.topicSizeBytes["ops.lessons.v1"] ?? 0;
  const lessonsPrev2 =
    history[history.length - 2].topicSizeBytes["ops.lessons.v1"] ?? 0;

  if (lessonsNow > 1024 * 1024 && lessonsNow > lessonsPrev2 * 1.1) {
    const growthKB = ((lessonsNow - lessonsPrev2) / 1024).toFixed(0);
    return {
      ...base,
      triggered: true,
      confidence: 0.76,
      cause: `ops.lessons.v1 grew ${growthKB} KB in 2 samples — log compaction lagging`,
    };
  }
  return base;
}

function evalPartitionImbalance(
  snap: MetricSnapshot,
  history: MetricSnapshot[]
): TriggerResult {
  // SUPPRESS: detect but do not act (benign after restart)
  const base: TriggerResult = {
    scenarioId: "partition-imbalance",
    triggered: false,
    confidence: 0,
    cause: "",
    gate: "suppress",
  };
  if (!history.length) return base;
  const prev = history[history.length - 1];
  if (snap.brokersOnline !== prev.brokersOnline && snap.brokersOnline > 1) {
    return {
      ...base,
      triggered: true,
      confidence: 0.80,
      cause: `Broker count changed ${prev.brokersOnline}→${snap.brokersOnline} — leader rebalance likely (suppressed)`,
    };
  }
  return base;
}

// ── All evaluators ────────────────────────────────────────────────────────────

const EVALUATORS: Array<
  (s: MetricSnapshot, h: MetricSnapshot[]) => TriggerResult
> = [
  evalLagSpike,
  evalControllerFailover,
  evalShareGroup,
  evalBenignRebalance,
  evalSchemaMismatch,
  evalDiskSaturation,
  evalUnderReplication,
  evalProducerTimeout,
  evalConsumerSessionTimeout,
  evalCompactionLag,
  evalPartitionImbalance,
];

// ── Core MRAL scenarios supported by triggerScenario() ───────────────────────

const CORE_SCENARIOS = new Set<PollScenarioId>([
  "lag-spike",
  "controller-failover",
  "share-group",
  "benign-rebalance",
]);

// Of the 4 core scenarios, only these 3 can be genuinely triggered for real
// right now. "share-group" stays audit-log-only: real share-group state uses
// a completely different broker coordinator/protocol than classic consumer
// groups, so the existing consumer-group-based collection in kafka-admin.ts
// cannot read it at all — this needs the separate in-cluster poller +
// ops.kafka.metrics.v1 consumption path, not yet built.
const READY_FOR_REAL_TRIGGER = new Set<PollScenarioId>([
  "lag-spike",
  "controller-failover",
  "benign-rebalance",
  "share-group",
]);


// ── Evaluator for extended scenarios signalled via /api/mesh/inject-signal ────
function evalSignalledScenarios(
  snap: MetricSnapshot,
  _history: MetricSnapshot[]
): TriggerResult[] {
  // Extended scenarios don't have metric thresholds — they fire when the
  // inject-signal endpoint marks them on broker.signalledScenarios.
  // Each signal is valid for 90 seconds after injection.
  const EXTENDED: Array<{ id: PollScenarioId; gate: "approval" | "auto" | "suppress" }> = [
    { id: "schema-mismatch",          gate: "approval" },
    { id: "disk-saturation",          gate: "auto" },
    { id: "under-replication",        gate: "approval" },
    { id: "producer-timeout",         gate: "auto" },
    { id: "consumer-session-timeout", gate: "auto" },
    { id: "compaction-lag",           gate: "auto" },
  ];
  const results: TriggerResult[] = [];
  try {
    const meshMod = require("./mesh");
    const broker = meshMod.getSnapshot()?.broker as Record<string, unknown>;
    const signals = broker?.signalledScenarios as Record<string, number> | undefined;
    if (!signals) return results;
    const now = Date.now();
    for (const { id, gate } of EXTENDED) {
      const signalTs = signals[id];
      if (signalTs && now - signalTs < 90_000) {
        results.push({
          scenarioId: id, triggered: true, confidence: 0.90,
          cause: `Scenario triggered by operator — Monitor detected signal`,
          gate,
        });
      }
    }
  } catch { /* mesh not available */ }
  return results;
}

// ── Poll cycle ────────────────────────────────────────────────────────────────

export async function runPollCycle(): Promise<void> {
  const g = getPollGlobal();
  const { state, history, activeScenarios } = g!;

  state.cycleCount++;
  state.lastPollAt = Date.now();
  state.detectedThisCycle = [];

  let snap: MetricSnapshot;
  try {
    snap = await collectSnapshot();
  } catch (e) {
    state.lastError = (e as Error).message;
    return;
  }

  // Push to sliding window
  history.push(snap);
  if (history.length > WINDOW_SIZE) history.shift();
  state.snapshotCount = history.length;
  state.lastError = null;

  const historyWindow = history.slice(0, -1); // everything before this snap
  const now = Date.now();

  // Evaluate all conditions (including extended signal-based scenarios)
  const allResults = [
    ...EVALUATORS.map(e => e(snap, historyWindow)),
    ...evalSignalledScenarios(snap, historyWindow),
  ];
  for (const result of allResults) {
    if (!result.triggered) continue;

    state.detectedThisCycle.push(result);
    // Persist for 2 min so rings stay visible between poll cycles
    g!.recentDetections.push({ ...result, ts: now });

    const { scenarioId, cause, confidence, gate } = result;

    // ── Suppress gate: emit audit but no MRAL ─────────────────────────────
    if (gate === "suppress") {
      eventBus.publish({
        type: "audit",
        record: {
          id: `poll-${Date.now()}-${scenarioId}`,
          ts: now,
          type: "reasoning",
          agent: "monitor",
          summary: `[POLL] Suppress — ${cause}`,
          detail: { scenarioId, confidence, gate, source: "monitor-poll" },
        },
      });
      continue;
    }

    // ── Cooldown check ─────────────────────────────────────────────────────
    const lastTriggered = state.cooldowns[scenarioId] ?? 0;
    if (now - lastTriggered < COOLDOWN_MS) continue;

    // ── Deduplication: skip if this scenario is already running ───────────
    if (activeScenarios.has(scenarioId)) continue;

    // ── Emit detection SSE event ───────────────────────────────────────────
    eventBus.publish({
      type: "audit",
      record: {
        id: `poll-${now}-${scenarioId}`,
        ts: now,
        type: "consume",
        agent: "monitor",
        summary: `[POLL] Autonomous trigger — ${scenarioId} (conf ${(confidence * 100).toFixed(0)}%) — ${cause}`,
        detail: { scenarioId, confidence, gate, cause, source: "monitor-poll" },
        topic: "ops.kafka.metrics.v1",
      },
    });

    // ── Fire scenario ──────────────────────────────────────────────────────
    state.cooldowns[scenarioId] = now;
    activeScenarios.add(scenarioId);

    {
      // Poll loop's role: detection, audit logging, cooldown tracking.
      // For the 3 scenarios real detection can genuinely drive
      // (READY_FOR_REAL_TRIGGER), this ALSO fires the same auto-trigger-scenario
      // event anomaly-sim.ts uses — useMeshStream.ts already handles it
      // correctly with zero changes needed. Everything else (extended
      // scenarios, and share-group until its real poller exists) stays
      // audit-log-only, same as before.
      eventBus.publish({
        type: "audit",
        record: {
          id: `poll-anom-${now}-${scenarioId}`,
          ts: now,
          type: "reasoning",
          agent: "monitor",
          summary: `[ANOMALY DETECTED] ${scenarioId}: ${cause}`,
          detail: { scenarioId, confidence, gate, cause, extended: true },
        },
      });
      // Publish a toast
      eventBus.publish({
        type: "toast",
        message: `Monitor detected: ${scenarioId} — ${cause.slice(0, 80)}`,
        kind: gate === "approval" ? "warning" : "info",
      });

      if (READY_FOR_REAL_TRIGGER.has(scenarioId)) {
        eventBus.publish({ type: "auto-trigger-scenario", scenarioId, real: { confidence, cause } } as never);
        try {
          const { deferNextAnomalyCycle } = await import("./anomaly-sim");
          deferNextAnomalyCycle();
        } catch (e) {
          console.warn("[MonitorPoll] deferNextAnomalyCycle failed:", (e as Error).message);
        }
      }
    }

    // Remove from active set after a short yield (fire-and-forget scenarios
    // clean themselves up; for extended scenarios we remove immediately)
    if (!CORE_SCENARIOS.has(scenarioId)) {
      activeScenarios.delete(scenarioId);
    } else {
      // The core scenario runner will finish asynchronously — remove after timeout
      setTimeout(() => activeScenarios.delete(scenarioId), 5 * 60_000);
    }
  }
}

// ── Public API ────────────────────────────────────────────────────────────────

export function startMonitorPolling(): void {
  const g = getPollGlobal();
  if (g!.state.running) return;
  g!.state.running = true;

  startShareGroupStatusSubscriber().catch((e) =>
    console.warn("[MonitorPoll] Share-group subscriber start error:", (e as Error).message)
  );

  // Fire one cycle immediately, then repeat
  runPollCycle().catch((e) =>
    console.error("[MonitorPoll] First cycle error:", safeErr(e))
  );
  g!.timer = setInterval(() => {
    runPollCycle().catch((e) =>
      console.error("[MonitorPoll] Cycle error:", safeErr(e))
    );
  }, POLL_MS);

  console.log("[MonitorPoll] Started — polling every", POLL_MS / 1000, "s");
}

export function stopMonitorPolling(): void {
  const g = getPollGlobal();
  if (g!.timer) {
    clearInterval(g!.timer);
    g!.timer = null;
  }
  g!.state.running = false;
  console.log("[MonitorPoll] Stopped");
}

export function getMonitorPollState(): PollLoopState & {
  historyLength: number;
} {
  const g = getPollGlobal();
  const TWO_MIN = 2 * 60_000;
  const now = Date.now();
  const recentWindow = (g?.recentDetections ?? []).filter((d) => now - d.ts < TWO_MIN);
  // Deduplicate: keep latest entry per scenarioId
  const seen = new Map<string, typeof recentWindow[0]>();
  for (const d of recentWindow) seen.set(d.scenarioId, d);
  const deduped = Array.from(seen.values());
  return { ...g!.state, detectedThisCycle: deduped, historyLength: g!.history.length };
}

export function getLatestSnapshot(): MetricSnapshot | null {
  const g = getPollGlobal();
  return g!.history.length ? g!.history[g!.history.length - 1] : null;
}

