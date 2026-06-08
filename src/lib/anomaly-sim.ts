/**
 * anomaly-sim.ts  (v2 — full autonomous operation)
 *
 * Injects realistic Kafka failure conditions into broker state on a schedule.
 * For every scenario:
 *   1. Patches broker state (so the Monitor poll loop detects and logs it).
 *   2. Emits "auto-trigger-scenario" SSE → useMeshStream calls runClientScenario:
 *        - Full MRAL animation on canvas
 *        - Approval gate modal for gated scenarios (consumer-lag, share-group,
 *          schema-mismatch, under-replication)
 *        - Notification popup in right panel
 *        - Email sent via POST /api/notify (if NOTIFICATION_EMAIL is set)
 *   3. After 90 s heal delay: restores broker state + emits "auto-topic-heal"
 *      for any topics that were made unhealthy by this scenario.
 *
 * Interval: 90–150 s between anomaly injections (randomised).
 */
import "server-only";
import { patchBrokerState } from "./mesh";
import { eventBus }         from "./event-bus";

interface AffectedTopic {
  topicName:     string;
  currentStatus: "degraded" | "critical";
  lagTotal:      number;
  partitions:    number;
}

interface Anomaly {
  id:             string;
  gate:           "approval" | "auto" | "suppress";
  inject:         () => void;
  heal:           () => void;
  affectedTopics: AffectedTopic[];
}

// ── Helper ────────────────────────────────────────────────────────────────────

function setSignal(id: string, ts?: number | null) {
  patchBrokerState((b) => {
    const rec = b as Record<string, unknown>;
    if (!rec.signalledScenarios) rec.signalledScenarios = {};
    const sigs = rec.signalledScenarios as Record<string, number>;
    if (ts === null) delete sigs[id];
    else sigs[id] = ts ?? Date.now();
  });
}

// ── Anomaly catalogue ─────────────────────────────────────────────────────────

const ANOMALIES: Anomaly[] = [
  {
    id: "lag-spike", gate: "approval",
    inject: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 24_000, rebalanceState: "stable", members: 3 };
    }),
    heal: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 0, rebalanceState: "stable", members: 3 };
    }),
    affectedTopics: [
      { topicName: "ops.kafka.metrics.v1", currentStatus: "critical", lagTotal: 18500, partitions: 6 },
    ],
  },
  {
    id: "controller-failover", gate: "auto",
    inject: () => patchBrokerState((b) => { b.controllerEpoch += 1; }),
    heal:   () => { /* epoch advancing is normal */ },
    affectedTopics: [],
  },
  {
    id: "share-group", gate: "approval",
    inject: () => patchBrokerState((b) => {
      b.consumerGroups["share-group-1"] = { lag: 15_000, rebalanceState: "stable", members: 2 };
    }),
    heal: () => patchBrokerState((b) => {
      b.consumerGroups["share-group-1"] = { lag: 0, rebalanceState: "stable", members: 2 };
    }),
    affectedTopics: [
      { topicName: "ops.requests.v1", currentStatus: "degraded", lagTotal: 8500, partitions: 3 },
    ],
  },
  {
    id: "benign-rebalance", gate: "suppress",
    inject: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 400, rebalanceState: "rebalancing", members: 3 };
    }),
    heal: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 0, rebalanceState: "stable", members: 3 };
    }),
    affectedTopics: [],
  },
  {
    id: "schema-mismatch", gate: "approval",
    inject: () => setSignal("schema-mismatch"),
    heal:   () => setSignal("schema-mismatch", null),
    affectedTopics: [
      { topicName: "ops.actions.audit.v1", currentStatus: "degraded", lagTotal: 7800, partitions: 12 },
    ],
  },
  {
    id: "disk-saturation", gate: "auto",
    inject: () => setSignal("disk-saturation"),
    heal:   () => setSignal("disk-saturation", null),
    affectedTopics: [
      { topicName: "ops.lessons.v1", currentStatus: "degraded", lagTotal: 3200, partitions: 3 },
    ],
  },
  {
    id: "under-replication", gate: "approval",
    inject: () => setSignal("under-replication"),
    heal:   () => setSignal("under-replication", null),
    affectedTopics: [
      { topicName: "ops.incidents.v1", currentStatus: "degraded", lagTotal: 5100, partitions: 3 },
    ],
  },
  {
    id: "producer-timeout", gate: "auto",
    inject: () => setSignal("producer-timeout"),
    heal:   () => setSignal("producer-timeout", null),
    affectedTopics: [
      { topicName: "ops.kafka.metrics.v1", currentStatus: "degraded", lagTotal: 4900, partitions: 6 },
    ],
  },
  {
    id: "consumer-session-timeout", gate: "auto",
    inject: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 0, rebalanceState: "dead", members: 0 };
    }),
    heal: () => patchBrokerState((b) => {
      b.consumerGroups["payments-consumer"] = { lag: 0, rebalanceState: "stable", members: 3 };
    }),
    affectedTopics: [
      { topicName: "ops.kafka.metrics.v1", currentStatus: "degraded", lagTotal: 3800, partitions: 6 },
    ],
  },
  {
    id: "compaction-lag", gate: "auto",
    inject: () => setSignal("compaction-lag"),
    heal:   () => setSignal("compaction-lag", null),
    affectedTopics: [
      { topicName: "ops.lessons.v1", currentStatus: "degraded", lagTotal: 2400, partitions: 3 },
    ],
  },
  {
    id: "partition-imbalance", gate: "suppress",
    inject: () => setSignal("partition-imbalance"),
    heal:   () => setSignal("partition-imbalance", null),
    affectedTopics: [],
  },
];

// ── Global singleton ──────────────────────────────────────────────────────────

declare global {
  var __anomalySim: {
    running: boolean; timer: NodeJS.Timeout | null; healTimer: NodeJS.Timeout | null;
    cycleCount: number; lastInjectedId: string | null; lastInjectedAt: number | null;
    nextIndex: number;
  } | undefined;
}

function getSim() {
  if (!globalThis.__anomalySim) globalThis.__anomalySim = {
    running: false, timer: null, healTimer: null,
    cycleCount: 0, lastInjectedId: null, lastInjectedAt: null, nextIndex: 0,
  };
  return globalThis.__anomalySim;
}

// ── Cycle ─────────────────────────────────────────────────────────────────────

const HEAL_DELAY  = 90_000;
const MIN_INTERVAL = 90_000;
const MAX_INTERVAL = 150_000;
const rand = () => MIN_INTERVAL + Math.floor(Math.random() * (MAX_INTERVAL - MIN_INTERVAL));

function runCycle() {
  const sim = getSim();
  if (!sim.running) return;

  const anomaly = ANOMALIES[sim.nextIndex % ANOMALIES.length];
  sim.nextIndex++;
  sim.cycleCount++;
  sim.lastInjectedId = anomaly.id;
  sim.lastInjectedAt = Date.now();

  // 1. Patch broker state (poll loop detection)
  try { anomaly.inject(); } catch (e) { console.warn("[AnomalySim] inject error:", e); }

  console.log(`[AnomalySim] Injected: ${anomaly.id} (${anomaly.gate})`);

  // 2. Audit log entry
  eventBus.publish({
    type: "audit",
    record: {
      id: `anomaly-${Date.now()}-${anomaly.id}`, ts: Date.now(),
      type: "consume", agent: "monitor",
      summary: `[ANOMALY] Monitor detected: ${anomaly.id} — autonomous MRAL will fire in ≤30s`,
      detail: { anomalyId: anomaly.id, gate: anomaly.gate, source: "anomaly-sim" },
      topic: "ops.kafka.metrics.v1",
    },
  });

  // 3. Auto-trigger full client-side MRAL (animation + approval + notify + email)
  //    Suppress-gate scenarios still run their MRAL (they show the "no action" decision)
  eventBus.publish({ type: "auto-trigger-scenario", scenarioId: anomaly.id } as never);

  // 4. Heal after delay + trigger topic healing
  if (sim.healTimer) clearTimeout(sim.healTimer);
  sim.healTimer = setTimeout(() => {
    try { anomaly.heal(); } catch { /* best-effort */ }

    // Emit auto-topic-heal for each affected topic
    for (const t of anomaly.affectedTopics) {
      eventBus.publish({
        type: "auto-topic-heal",
        topicName:     t.topicName,
        currentStatus: t.currentStatus,
        lagTotal:      t.lagTotal,
        partitions:    t.partitions,
      } as never);
    }

    if (anomaly.affectedTopics.length > 0) {
      console.log(`[AnomalySim] Healed: ${anomaly.id} + topic healing triggered`);
    } else {
      console.log(`[AnomalySim] Healed: ${anomaly.id}`);
    }
  }, HEAL_DELAY);

  // 5. Schedule next cycle
  if (sim.timer) clearTimeout(sim.timer);
  sim.timer = setTimeout(runCycle, rand());
}

// ── Public API ────────────────────────────────────────────────────────────────

export function startAnomalySimulation(): void {
  const sim = getSim();
  if (sim.running) return;
  sim.running = true;
  sim.timer = setTimeout(runCycle, 35_000); // first anomaly after 35s (poll loop gets baseline)
  console.log("[AnomalySim] Started — first anomaly in 35s, then every 90–150s");
}

export function stopAnomalySimulation(): void {
  const sim = getSim();
  if (sim.timer)     clearTimeout(sim.timer);
  if (sim.healTimer) clearTimeout(sim.healTimer);
  sim.running = false;
  sim.timer = sim.healTimer = null;
  console.log("[AnomalySim] Stopped");
}

export function getAnomalySimState() {
  const sim = getSim();
  return { running: sim.running, cycleCount: sim.cycleCount,
           lastInjectedId: sim.lastInjectedId, lastInjectedAt: sim.lastInjectedAt };
}
