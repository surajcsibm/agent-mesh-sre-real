"use client";
/**
 * Client-side scenario simulator.
 *
 * On Vercel each API invocation runs in a separate serverless instance, so
 * the SSE stream and the scenario trigger don't share the same globalThis
 * event bus.  This module re-implements the MRAL animation entirely in the
 * browser so the demo looks identical whether running locally or on Vercel.
 *
 * Usage:
 *   runClientScenario(scenarioKey, dispatch)
 */

import type {
  AgentState, BrokerState, MralPhase, AuditRecord,
  NotificationRecord, LessonRecord, ApprovalRequest, MCPToolCall,
} from "./types";

// ── Types mirrored from useMeshStream.ts ─────────────────────────────────────

type DispatchFn = (action: SimAction) => void;

export type SimAction =
  | { type: "state"; payload: Partial<SimStatePayload> & { agents: AgentState[]; mralPhase: MralPhase } }
  | { type: "audit"; record: AuditRecord }
  | { type: "toast"; message: string; kind: string; id: number }
  | { type: "dismissToast"; id: number }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string; id: string }
  | { type: "clearParticle"; id: string }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord }
  | { type: "connected"; value: boolean };

interface SimStatePayload {
  agents: AgentState[];
  mralPhase: MralPhase;
  broker: BrokerState;
  pendingApprovals: ApprovalRequest[];
  incidentQueueDepth: number;
  scenarioRunning: boolean;
  auditLog?: AuditRecord[];
  lessons?: LessonRecord[];
  notifications?: NotificationRecord[];
}

// ── Helpers ───────────────────────────────────────────────────────────────────

let _toastId = 1000;
let _particleId = 1000;

function uid() { return `sim-${Date.now()}-${Math.random().toString(36).slice(2)}`; }

function baseAgents(): AgentState[] {
  return [
    { id: "intake",       name: "Intake Agent",       role: "MCP Gateway",       status: "online", mralPhase: "idle", color: "#22d3ee", lastReasoning: null, lastAction: null, lastLesson: null, consumerOffset: {} },
    { id: "monitor",      name: "Monitor Agent",       role: "SRE Brain",         status: "online", mralPhase: "idle", color: "#a78bfa", lastReasoning: null, lastAction: null, lastLesson: null, consumerOffset: {} },
    { id: "writer",       name: "Writer Agent",        role: "Postmortem Author",  status: "online", mralPhase: "idle", color: "#34d399", lastReasoning: null, lastAction: null, lastLesson: null, consumerOffset: {} },
    { id: "notification", name: "Notification Agent",  role: "Outbound Routing",  status: "online", mralPhase: "idle", color: "#fbbf24", lastReasoning: null, lastAction: null, lastLesson: null, consumerOffset: {} },
  ];
}

function mockBroker(lagOverride = 0): BrokerState {
  return {
    mode: "MOCK", controllerEpoch: 14, brokersOnline: 3,
    mtls: true, sasl: true, aclCount: 18,
    topics: {
      "ops.requests.v1":       { partitions: 3, lag: 0,            offsetHigh: 100 },
      "ops.kafka.metrics.v1":  { partitions: 6, lag: lagOverride,  offsetHigh: 500 },
      "ops.incidents.v1":      { partitions: 3, lag: 0,            offsetHigh: 40  },
      "ops.actions.audit.v1":  { partitions: 3, lag: 0,            offsetHigh: 60  },
      "ops.lessons.v1":        { partitions: 1, lag: 0,            offsetHigh: 12  },
      "ops.notifications.v1":  { partitions: 1, lag: 0,            offsetHigh: 24  },
    },
    consumerGroups: {
      "payments-consumer": { lag: lagOverride, rebalanceState: lagOverride > 0 ? "Rebalancing" : "Stable", members: 2 },
      "sre-monitor":       { lag: 0,           rebalanceState: "Stable",       members: 1 },
    },
  };
}

function patch(agents: AgentState[], id: string, updates: Partial<AgentState>): AgentState[] {
  return agents.map(a => a.id === id ? { ...a, ...updates } : a);
}

function auditRec(agent: string, summary: string, topic?: string): AuditRecord {
  return { id: uid(), ts: Date.now(), type: "publish", kind: "publish", agent: agent as AgentState["id"], summary, detail: {}, topic };
}

function particle(dispatch: DispatchFn, edgeId: string, from: string, to: string) {
  const id = `p-${++_particleId}`;
  dispatch({ type: "particle", edgeId, fromNode: from, toNode: to, id });
  setTimeout(() => dispatch({ type: "clearParticle", id }), 1200);
}

function toast(dispatch: DispatchFn, message: string, kind = "info") {
  const id = ++_toastId;
  dispatch({ type: "toast", message, kind, id });
  setTimeout(() => dispatch({ type: "dismissToast", id }), 4500);
}

// ── Email trigger ─────────────────────────────────────────────────────────────

function sendEmail(scenarioId: string, scenarioLabel: string, lagBefore: number, lagAfter: number, action: string, approvedBy = "vp-engineering@stage") {
  fetch("/api/notify", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenarioId, scenarioLabel, lagBefore, lagAfter, action, approvedBy }),
  }).catch(() => { /* non-fatal: SMTP may not be configured */ });
}

// ── Schedule helper ────────────────────────────────────────────────────────────

type Step = { ms: number; fn: () => void };

function schedule(steps: Step[]): () => void {
  const timers: ReturnType<typeof setTimeout>[] = [];
  for (const s of steps) {
    timers.push(setTimeout(s.fn, s.ms));
  }
  return () => timers.forEach(clearTimeout);
}

// ── Scenarios ─────────────────────────────────────────────────────────────────

function runLagSpike(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const broker = mockBroker(0);

  return schedule([
    // t=0  intake publishes request
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("intake", "Published simulate-lag-spike to ops.requests.v1", "ops.requests.v1") });
      particle(dispatch, "e-req", "intake", "monitor");
    }},
    { ms: 800, fn: () => {
      agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(1800), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    }},

    // t=1.5s  monitor detects lag spike
    { ms: 1500, fn: () => {
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(4200), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Consumer lag spike detected on payments-consumer (4,200 msgs behind)", "ops.kafka.metrics.v1") });
      particle(dispatch, "e-metrics", "monitor", "monitor");
    }},

    // t=3s  reasoning complete, awaiting approval
    { ms: 3000, fn: () => {
      const toolCall: MCPToolCall = {
        jsonrpc: "2.0", id: uid(), method: "tools/call",
        params: { name: "kafka.scaleConsumers", arguments: { group: "payments-consumer", delta: 2, reason: "Lag spike: 4200 msgs" } },
      };
      const approval: ApprovalRequest = {
        id: uid(), ts: Date.now(), createdAt: Date.now(),
        agent: "monitor", proposedBy: "monitor-agent",
        toolCall, scenarioId: "lag-spike",
        reason: "Scale payments-consumer from N→N+2 to drain 4,200-msg backlog",
        status: "pending",
      };
      agents = patch(agents, "monitor", { status: "awaiting-approval", mralPhase: "awaiting",
        lastReasoning: {
          rootCause: "payments-consumer lag spike: 4,200 messages behind across 3 partitions",
          confidence: 0.94,
          kafkaFeatureCited: "KIP-848 Share Groups",
          rebalanceState: "Rebalancing",
          controllerEpoch: 14,
          crossCorrelation: { brokers: "healthy", jvmHeap: "68%", networkInRate: "↑ 2.1×", rebalanceInProgress: true },
          recommendedAction: "kafka.scaleConsumers(group=payments-consumer, delta=2)",
          requiresApproval: true,
          rationale: "Lag growth rate 420 msg/s exceeds SLO threshold (150 msg/s). Rebalance in progress adds risk; adding 2 consumers will absorb spike within ~10s.",
          lessonsCited: ["lesson-003"],
        }
      });
      dispatch({ type: "state", payload: { agents, mralPhase: "awaiting", broker: mockBroker(4200), pendingApprovals: [approval], incidentQueueDepth: 1, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Awaiting approval: kafka.scaleConsumers (policy-gated, human-in-the-loop)") });
      toast(dispatch, "⏳ Approval required: kafka.scaleConsumers — check the approval panel", "warning");
    }},

    // t=5s  auto-approve for demo
    { ms: 5000, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(4200), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Approved by vp-engineering@stage · executing kafka.scaleConsumers") });
      toast(dispatch, "✅ Approved — scaling consumers", "info");
      particle(dispatch, "e-inc", "monitor", "writer");
    }},

    // t=6.5s  lag draining, writer activated
    { ms: 6500, fn: () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle",
        lastAction: { approved: true, approvedBy: "vp-engineering@stage", outcome: "success", detail: "Scaled payments-consumer N→N+2; lag draining at 680 msg/s", lagBefore: 4200, lagAfter: 180, toolCalled: "kafka.scaleConsumers", clusterMutation: "ConsumerGroupScaleOut" }
      });
      agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(180), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("writer", "Drafting incident postmortem for lag-spike scenario", "ops.actions.audit.v1") });
      particle(dispatch, "e-aud", "writer", "notification");
    }},

    // t=8s  notification agent fires + send real email
    { ms: 8000, fn: () => {
      agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("notification", "Posting to #sre-alerts Slack & opening ITSM ticket", "ops.notifications.v1") });
      const slackNotif: NotificationRecord = { id: uid(), ts: Date.now(), channel: "slack", title: "Lag spike resolved", message: "✅ payments-consumer lag spike resolved · lag 4200→0 · scaled N→N+2", scenarioId: "lag-spike" };
      dispatch({ type: "notification", record: slackNotif });
      toast(dispatch, "📢 Slack + ITSM notification sent — sending email summary…", "success");
      // Trigger real email via server-side API (works on Vercel)
      sendEmail("lag-spike", "Consumer Lag Spike", 4200, 0, "kafka.scaleConsumers");
    }},

    // t=9.5s  learn phase
    { ms: 9500, fn: () => {
      agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
      dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Recording lesson: scale-out threshold adjusted → 150 msg/s") });
      particle(dispatch, "e-learn", "monitor", "monitor");
    }},

    // t=11s  all idle
    { ms: 11000, fn: () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle",
        lastLesson: { id: uid(), ts: Date.now(), scenarioId: "lag-spike", actionTaken: "kafka.scaleConsumers(delta=2)", effective: true, lagBefore: 4200, lagAfter: 0, adjustedThreshold: 150, notes: "Scale-out resolved lag in 9s. Threshold tightened from 300→150 msg/s for faster response." }
      });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
      toast(dispatch, "✅ Scenario complete — lesson recorded", "success");
    }},
  ]);
}

function runControllerFailover(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  return schedule([
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-req", "intake", "monitor");
      toast(dispatch, "⚡ Controller failover triggered", "info");
    }},
    { ms: 1200, fn: () => {
      agents = patch(agents, "intake",  { status: "online" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "KRaft controller failover detected: epoch 14→15, broker-1→broker-2") });
    }},
    { ms: 3000, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Acknowledged controller failover — no partition reassignment needed") });
      particle(dispatch, "e-inc", "monitor", "writer");
    }},
    { ms: 5000, fn: () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "writer",  { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-aud", "writer", "notification");
    }},
    { ms: 7000, fn: () => {
      agents = patch(agents, "writer",       { status: "online" });
      agents = patch(agents, "notification", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Controller failover", message: "ℹ️ KRaft controller failover epoch 14→15 acknowledged, cluster healthy", scenarioId: "controller-failover" } });
      toast(dispatch, "✅ Controller failover handled — sending email summary…", "success");
      sendEmail("controller-failover", "KRaft Controller Failover", 0, 0, "kafka.acknowledgeFailover");
    }},
    { ms: 9000, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
    }},
  ]);
}

function runShareGroupRebalance(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  return schedule([
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-req", "intake", "monitor");
      toast(dispatch, "🔄 Share-group rebalance initiated", "info");
    }},
    { ms: 1500, fn: () => {
      agents = patch(agents, "intake",  { status: "online" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(600), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "KIP-848 share-group rebalance detected: payments-share-group checkpoint needed") });
    }},
    { ms: 3500, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(600), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Checkpointing share-group offsets via kafka.shareGroupCheckpoint") });
      particle(dispatch, "e-inc", "monitor", "writer");
    }},
    { ms: 5500, fn: () => {
      agents = patch(agents, "monitor", { status: "online" });
      agents = patch(agents, "writer",  { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-aud", "writer", "notification");
    }},
    { ms: 7500, fn: () => {
      agents = patch(agents, "writer",       { status: "online" });
      agents = patch(agents, "notification", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Share-group rebalance", message: "✅ payments-share-group rebalance complete — offsets checkpointed", scenarioId: "share-group-rebalance" } });
      sendEmail("share-group-rebalance", "Share-Group Rebalance", 600, 0, "kafka.shareGroupCheckpoint");
    }},
    { ms: 9500, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
      toast(dispatch, "✅ Share-group rebalance handled", "success");
    }},
  ]);
}

function runPartitionImbalance(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  return schedule([
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-req", "intake", "monitor");
      toast(dispatch, "⚠️ Partition imbalance detected", "warning");
    }},
    { ms: 1000, fn: () => {
      agents = patch(agents, "intake",  { status: "online" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Partition imbalance: broker-3 holds 62% of leaders (threshold: 40%)") });
    }},
    { ms: 2500, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Triggering preferred-leader election to rebalance partition leadership") });
      particle(dispatch, "e-inc", "monitor", "writer");
    }},
    { ms: 5000, fn: () => {
      agents = patch(agents, "monitor", { status: "online" });
      agents = patch(agents, "writer",  { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-aud", "writer", "notification");
    }},
    { ms: 7000, fn: () => {
      agents = patch(agents, "writer",       { status: "online" });
      agents = patch(agents, "notification", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Partition imbalance resolved", message: "✅ Partition leadership rebalanced — broker distribution: 33%/34%/33%", scenarioId: "partition-imbalance" } });
      sendEmail("partition-imbalance", "Partition Imbalance", 0, 0, "kafka.rebalancePartitions");
    }},
    { ms: 9000, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
      toast(dispatch, "✅ Partition imbalance resolved", "success");
    }},
  ]);
}

// ── Public API ────────────────────────────────────────────────────────────────

export type ScenarioKey = "lag-spike" | "controller-failover" | "share-group-rebalance" | "partition-imbalance";

export function runClientScenario(key: ScenarioKey, dispatch: DispatchFn): () => void {
  switch (key) {
    case "lag-spike":              return runLagSpike(dispatch);
    case "controller-failover":    return runControllerFailover(dispatch);
    case "share-group-rebalance":  return runShareGroupRebalance(dispatch);
    case "partition-imbalance":    return runPartitionImbalance(dispatch);
    default:                       return () => {};
  }
}
