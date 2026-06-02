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

// ── Broker mode (set by useMeshStream once initial SSE state arrives) ─────────
let _brokerMode: "MOCK" | "REAL" = "MOCK";
export function setBrokerMode(m: "MOCK" | "REAL") { _brokerMode = m; }

// ── Types mirrored from useMeshStream.ts ─────────────────────────────────────

type DispatchFn = (action: SimAction) => void;

/** A single live-feed event captured during a scenario run. */
export interface LiveFeedEvent {
  type: string;
  agent: string;
  summary: string;
  ts: number;
}

/** Scenario-end summary surfaced to Dashboard as a popup modal. */
export interface EmailSummaryData {
  scenarioLabel: string;
  scenarioId: string;
  action: string;
  lagBefore: number;
  lagAfter: number;
  approved: boolean;
  approvedBy?: string;
  sent: boolean;
  emailError?: string;
  // Rich preview fields — mirror the email content so the popup looks identical
  reasoning?: {
    rootCause: string;
    confidence: number;
    kafkaFeature: string;
    rationale: string;
    lessonsCited?: string[];
  };
  lesson?: {
    notes: string;
    adjustedThreshold?: number;
  };
  slackMessage?: string;
  itsmTicket?: string;
  /** Key events captured during the scenario — shown in popup and email */
  liveEvents?: LiveFeedEvent[];
}

export type SimAction =
  | { type: "state"; payload: Partial<SimStatePayload> & { agents: AgentState[]; mralPhase: MralPhase } }
  | { type: "audit"; record: AuditRecord }
  | { type: "toast"; message: string; kind: string; id: number }
  | { type: "dismissToast"; id: number }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string; id: string }
  | { type: "clearParticle"; id: string }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord }
  | { type: "connected"; value: boolean }
  | { type: "emailSummary"; data: EmailSummaryData | null };

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

// ── Approval gate ─────────────────────────────────────────────────────────────
// When the simulation creates a pending approval it registers a callback here.
// useMeshStream calls resolvePendingApproval() when the user clicks Approve/Reject.

let _pendingApprovalCallback: ((approved: boolean) => void) | null = null;

/** Called by useMeshStream's approve() so the sim can branch on the decision. */
export function resolvePendingApproval(approved: boolean) {
  if (_pendingApprovalCallback) {
    _pendingApprovalCallback(approved);
    _pendingApprovalCallback = null;
  }
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
  // When connected to a real Kafka cluster, preserve REAL mode and single-node topology
  if (_brokerMode === "REAL") {
    return {
      mode: "REAL", controllerEpoch: 1, brokersOnline: 1,
      mtls: false, sasl: true, aclCount: 0,
      topics: {
        "ops.requests.v1":      { partitions: 1, lag: 0,           offsetHigh: 100 },
        "ops.kafka.metrics.v1": { partitions: 1, lag: lagOverride, offsetHigh: 500 },
        "ops.incidents.v1":     { partitions: 1, lag: 0,           offsetHigh: 40  },
        "ops.actions.audit.v1": { partitions: 1, lag: 0,           offsetHigh: 60  },
        "ops.lessons.v1":       { partitions: 1, lag: 0,           offsetHigh: 12  },
        "ops.notifications.v1": { partitions: 1, lag: 0,           offsetHigh: 24  },
        "demo.payments.events": { partitions: 1, lag: 0,           offsetHigh: 0   },
      },
      consumerGroups: {
        "payments-consumer": { lag: lagOverride, rebalanceState: lagOverride > 0 ? "Rebalancing" : "Stable", members: 1 },
        "sre-monitor":       { lag: 0,           rebalanceState: "Stable",       members: 1 },
      },
    };
  }
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

function auditRec(agent: string, summary: string, type = "publish", topic?: string): AuditRecord {
  return { id: uid(), ts: Date.now(), type: type as AuditRecord["type"], kind: type, agent: agent as AgentState["id"], summary, detail: {}, topic };
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

interface EmailMeta {
  approvedBy?: string;
  approved?: boolean;            // default true; pass false for rejection emails
  reasoning?: EmailSummaryData["reasoning"];
  lesson?: EmailSummaryData["lesson"];
  slackMessage?: string;
  itsmTicket?: string;
  liveEvents?: LiveFeedEvent[];
}

function sendEmail(
  dispatch: DispatchFn,
  scenarioId: string,
  scenarioLabel: string,
  lagBefore: number,
  lagAfter: number,
  action: string,
  meta: EmailMeta = {},
) {
  const approved   = meta.approved !== false;  // default true
  const approvedBy = meta.approvedBy ?? "operator";

  fetch("/api/notify", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      scenarioId, scenarioLabel, lagBefore, lagAfter, action, approvedBy,
      approved,
      reasoning:    meta.reasoning,
      lesson:       meta.lesson,
      slackMessage: meta.slackMessage,
      itsmTicket:   meta.itsmTicket,
      liveEvents:   meta.liveEvents,
    }),
  })
    .then((r) => r.json())
    .then((data: { ok: boolean; error?: string }) => {
      dispatch({
        type: "emailSummary",
        data: {
          scenarioLabel, scenarioId, action,
          lagBefore, lagAfter, approved, approvedBy,
          sent: data.ok, emailError: data.ok ? undefined : data.error,
          reasoning:    meta.reasoning,
          lesson:       meta.lesson,
          slackMessage: meta.slackMessage,
          itsmTicket:   meta.itsmTicket,
          liveEvents:   meta.liveEvents,
        },
      });
    })
    .catch(() => {
      dispatch({
        type: "emailSummary",
        data: {
          scenarioLabel, scenarioId, action,
          lagBefore, lagAfter, approved, approvedBy,
          sent: false, emailError: "network_error",
          reasoning: meta.reasoning, lesson: meta.lesson,
          liveEvents: meta.liveEvents,
        },
      });
    });
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
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  // Collect key feed events for the summary popup + email
  const liveEvents: LiveFeedEvent[] = [];
  function evt(type: string, agent: string, summary: string) {
    liveEvents.push({ type, agent, summary, ts: Date.now() });
  }

  // Helper: schedule a step and track the timer for cleanup
  function t(ms: number, fn: () => void) {
    allTimers.push(setTimeout(fn, ms));
  }

  // Steps 1–3: up to the approval gate (timing slowed for readability)
  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Published simulate-lag-spike to ops.requests.v1", "publish", "ops.requests.v1") });
    evt("publish", "intake", "Published simulate-lag-spike to ops.requests.v1");
    particle(dispatch, "e-req", "intake", "monitor");
  });

  t(1800, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(1800), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Metrics event consumed from ops.kafka.metrics.v1 — forwarding to monitor", "consume", "ops.kafka.metrics.v1") });
  });

  t(3200, () => {
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(4200), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Consumer lag spike detected on payments-consumer (4,200 msgs behind)", "reasoning", "ops.kafka.metrics.v1") });
    evt("reasoning", "monitor", "Consumer lag spike detected on payments-consumer (4,200 msgs behind)");
    particle(dispatch, "e-metrics", "monitor", "monitor");
  });

  t(5000, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Cross-correlating: brokers healthy · JVM heap 68% · network ↑ 2.1× · rebalance in progress", "reasoning") });
    evt("reasoning", "monitor", "Cross-correlating: brokers healthy · JVM heap 68% · network ↑ 2.1×");
  });

  // t≈6.5s  reasoning complete — pause and wait for human decision
  t(6500, () => {
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
    dispatch({ type: "audit", record: auditRec("monitor", "Awaiting approval: kafka.scaleConsumers (policy-gated, human-in-the-loop)", "approval") });
    evt("approval", "monitor", "Awaiting approval: kafka.scaleConsumers(payments-consumer, +2 replicas)");
    toast(dispatch, "⏳ Approval required: kafka.scaleConsumers — check the approval panel", "warning");

    // Register callback — will fire when user clicks Approve or Reject
    _pendingApprovalCallback = (approved: boolean) => {
      if (approved) {
        // ── APPROVED PATH ──────────────────────────────────────────────────────
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(4200), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Approved by operator · executing kafka.scaleConsumers(group=payments-consumer, delta=2)", "approval") });
          evt("approval", "monitor", "Approved by operator — executing kafka.scaleConsumers");
          toast(dispatch, "✅ Approved — scaling consumers", "info");
          particle(dispatch, "e-inc", "monitor", "writer");
        }, 0));

        allTimers.push(setTimeout(() => {
          dispatch({ type: "audit", record: auditRec("monitor", "kafka.scaleConsumers succeeded · lag draining at 680 msg/s · ConsumerGroupScaleOut mutation applied", "tool-call") });
          evt("tool-call", "monitor", "kafka.scaleConsumers succeeded · ConsumerGroupScaleOut applied");
        }, 2000));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle",
            lastAction: { approved: true, approvedBy: "operator", outcome: "success", detail: "Scaled payments-consumer N→N+2; lag draining at 680 msg/s", lagBefore: 4200, lagAfter: 180, toolCalled: "kafka.scaleConsumers", clusterMutation: "ConsumerGroupScaleOut" }
          });
          agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(180), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("writer", "Consuming incident record from ops.incidents.v1 — drafting postmortem", "consume", "ops.incidents.v1") });
          particle(dispatch, "e-aud", "writer", "notification");
        }, 3500));

        allTimers.push(setTimeout(() => {
          dispatch({ type: "audit", record: auditRec("writer", "Incident postmortem drafted · publishing audit record to ops.actions.audit.v1", "publish", "ops.actions.audit.v1") });
        }, 5500));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("notification", "Consuming audit record · posting to #sre-alerts Slack & opening ITSM ticket", "notification", "ops.notifications.v1") });
          const slackNotif: NotificationRecord = { id: uid(), ts: Date.now(), channel: "slack", title: "Lag spike resolved", message: "✅ payments-consumer lag spike resolved · lag 4200→0 · scaled N→N+2", scenarioId: "lag-spike" };
          dispatch({ type: "notification", record: slackNotif });
          toast(dispatch, "📢 Slack + ITSM notification sent — sending email summary…", "success");
          evt("notification", "notification", "Slack #sre-alerts posted · ITSM ticket opened");
          sendEmail(dispatch, "lag-spike", "Consumer Lag Spike", 4200, 0, "kafka.scaleConsumers", {
            approvedBy: "operator",
            approved: true,
            reasoning: {
              rootCause: "payments-consumer lag spike: 4,200 messages behind across 3 partitions",
              confidence: 0.94, kafkaFeature: "KIP-848 Share Groups",
              rationale: "Lag growth rate 420 msg/s exceeded SLO threshold (150 msg/s). Adding 2 consumers resolved lag in ~10s.",
              lessonsCited: ["lesson-003"],
            },
            lesson: { notes: "Scale-out resolved lag in 9s. Threshold tightened 300→150 msg/s.", adjustedThreshold: 150 },
            slackMessage: "✅ payments-consumer lag spike resolved · lag 4,200→0 · scaled N→N+2",
            itsmTicket: `INC-${Date.now().toString().slice(-5)} closed — kafka.scaleConsumers resolved consumer lag spike`,
            liveEvents: [...liveEvents],
          });
        }, 7000));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
          dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Recording lesson: scale-out effective · threshold adjusted 300→150 msg/s", "lesson") });
          particle(dispatch, "e-learn", "monitor", "monitor");
        }, 9500));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle",
            lastLesson: { id: uid(), ts: Date.now(), scenarioId: "lag-spike", actionTaken: "kafka.scaleConsumers(delta=2)", effective: true, lagBefore: 4200, lagAfter: 0, adjustedThreshold: 150, notes: "Scale-out resolved lag in 9s. Threshold tightened from 300→150 msg/s for faster response." }
          });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          toast(dispatch, "✅ Scenario complete — lesson recorded", "success");
        }, 12000));

      } else {
        // ── REJECTED PATH ─────────────────────────────────────────────────────
        agents = baseAgents(); // reset all agents to online/idle
        dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
        dispatch({ type: "audit", record: auditRec("monitor", "Action REJECTED by operator — kafka.scaleConsumers not executed, scenario aborted", "approval") });
        evt("approval", "monitor", "REJECTED — kafka.scaleConsumers not executed");
        // Send rejection notification email and show popup
        sendEmail(dispatch, "lag-spike", "Consumer Lag Spike", 4200, 4200, "kafka.scaleConsumers", {
          approvedBy: "operator",
          approved: false,
          reasoning: {
            rootCause: "payments-consumer lag spike: 4,200 messages behind across 3 partitions",
            confidence: 0.94, kafkaFeature: "KIP-848 Share Groups",
            rationale: "Lag growth rate 420 msg/s exceeded SLO threshold (150 msg/s). Action was rejected by operator — no cluster changes made.",
            lessonsCited: ["lesson-003"],
          },
          liveEvents: [...liveEvents],
        });
      }
    };
  });

  return () => allTimers.forEach(clearTimeout);
}

function runControllerFailover(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  return schedule([
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-req", "intake", "monitor");
      toast(dispatch, "⚡ Controller failover triggered", "info");
      dispatch({ type: "audit", record: auditRec("intake", "Published controller-failover event to ops.requests.v1", "publish", "ops.requests.v1") });
    }},
    { ms: 2000, fn: () => {
      agents = patch(agents, "intake",  { status: "online" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "KRaft controller failover detected: epoch 14→15, broker-1→broker-2", "reasoning") });
    }},
    { ms: 4000, fn: () => {
      dispatch({ type: "audit", record: auditRec("monitor", "Election completed in 220ms · no partition reassignment needed · cluster healthy", "reasoning") });
    }},
    { ms: 5500, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Acknowledged controller failover — publishing incident record to ops.incidents.v1", "tool-call") });
      particle(dispatch, "e-inc", "monitor", "writer");
    }},
    { ms: 8000, fn: () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "writer",  { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("writer", "Consuming incident · drafting failover postmortem · publishing to ops.actions.audit.v1", "consume") });
      particle(dispatch, "e-aud", "writer", "notification");
    }},
    { ms: 11000, fn: () => {
      agents = patch(agents, "writer",       { status: "online" });
      agents = patch(agents, "notification", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Controller failover", message: "ℹ️ KRaft controller failover epoch 14→15 acknowledged, cluster healthy", scenarioId: "controller-failover" } });
      dispatch({ type: "audit", record: auditRec("notification", "Slack #sre-alerts posted · ITSM ticket opened · email summary dispatched", "notification") });
      toast(dispatch, "✅ Controller failover handled — sending email summary…", "success");
      sendEmail(dispatch, "controller-failover", "KRaft Controller Failover", 0, 0, "kafka.ackControllerFailover", {
        approvedBy: "system",
        reasoning: {
          rootCause: "KRaft controller leadership transferred: broker-1 → broker-2 (epoch 14→15)",
          confidence: 0.99, kafkaFeature: "KRaft Consensus Protocol",
          rationale: "Election completed in 220ms. No partition reassignment required. Cluster healthy.",
        },
        lesson: { notes: "KRaft failover handled automatically. No consumer impact. Election latency within SLO." },
        slackMessage: "ℹ️ KRaft controller failover epoch 14→15 acknowledged — cluster healthy",
        itsmTicket: `INC-${Date.now().toString().slice(-5)} closed — KRaft controller failover self-healed`,
      });
    }},
    { ms: 14000, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: { ...mockBroker(0), controllerEpoch: 15 }, pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
    }},
  ]);
}

function runShareGroupRebalance(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];

  // Steps 1-2: monitor detects rebalance and reasons
  allTimers.push(setTimeout(() => {
    agents = patch(agents, "intake", { status: "acting" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "🔄 Share-group rebalance initiated", "info");
    dispatch({ type: "audit", record: auditRec("intake", "Published share-group-rebalance event to ops.requests.v1", "publish", "ops.requests.v1") });
  }, 0));

  allTimers.push(setTimeout(() => {
    agents = patch(agents, "intake",  { status: "online" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(600), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "KIP-932 share-group rebalance detected: payments-share-group · 600 msgs behind", "reasoning") });
  }, 2500));

  allTimers.push(setTimeout(() => {
    dispatch({ type: "audit", record: auditRec("monitor", "Confidence 91% · checkpointing offsets prevents duplicate delivery during rebalance", "reasoning") });
  }, 4500));

  // t≈6s — pause for human approval before checkpoint
  allTimers.push(setTimeout(() => {
    const toolCall: MCPToolCall = {
      jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.checkpointShareGroup", arguments: { shareGroupId: "payments-share-group", reason: "KIP-932 rebalance: 600 msgs behind" } },
    };
    const approval: ApprovalRequest = {
      id: uid(), ts: Date.now(), createdAt: Date.now(),
      agent: "monitor", proposedBy: "monitor-agent",
      toolCall, scenarioId: "share-group-rebalance",
      reason: "Checkpoint payments-share-group offsets to prevent duplicate delivery during KIP-932 rebalance",
      status: "pending",
    };
    agents = patch(agents, "monitor", {
      status: "awaiting-approval", mralPhase: "awaiting",
      lastReasoning: {
        rootCause: "KIP-932 share group rebalance on payments-share-group: 600 messages behind",
        confidence: 0.91,
        kafkaFeatureCited: "KIP-932 Share Groups",
        rebalanceState: "Rebalancing",
        controllerEpoch: 14,
        crossCorrelation: { brokers: "healthy", jvmHeap: "62%", networkInRate: "↑ 1.4×", rebalanceInProgress: true },
        recommendedAction: "kafka.checkpointShareGroup(shareGroupId=payments-share-group)",
        requiresApproval: true,
        rationale: "Share group rebalance triggered by consumer join. Checkpointing offsets prevents duplicate delivery during transition.",
        lessonsCited: [],
      },
    });
    console.log("[client-sim] dispatching share-group approval gate", approval);
    dispatch({ type: "state", payload: { agents, mralPhase: "awaiting", broker: mockBroker(600), pendingApprovals: [approval], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Awaiting approval: kafka.checkpointShareGroup (policy-gated, human-in-the-loop)", "approval") });
    toast(dispatch, "⏳ Approval required: kafka.checkpointShareGroup — check the approval panel", "warning");

    _pendingApprovalCallback = (approved: boolean) => {
      if (approved) {
        // ── APPROVED PATH ──────────────────────────────────────────────────
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(600), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Approved by operator · executing kafka.checkpointShareGroup(shareGroupId=payments-share-group)", "approval") });
          particle(dispatch, "e-inc", "monitor", "writer");
        }, 300));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online" });
          agents = patch(agents, "writer",  { status: "acting" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("writer", "Consuming incident · drafting share-group postmortem · publishing to ops.actions.audit.v1", "consume") });
          particle(dispatch, "e-aud", "writer", "notification");
        }, 3000));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "writer",       { status: "online" });
          agents = patch(agents, "notification", { status: "acting" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Share-group rebalance", message: "✅ payments-share-group rebalance complete — offsets checkpointed", scenarioId: "share-group-rebalance" } });
          dispatch({ type: "audit", record: auditRec("notification", "Slack #sre-alerts posted · ITSM ticket opened · email summary dispatched", "notification") });
          sendEmail(dispatch, "share-group", "Share Group Rebalance", 600, 0, "kafka.checkpointShareGroup", {
            approvedBy: "operator",
            reasoning: {
              rootCause: "KIP-932 share group rebalance detected on payments-share-group (600 msgs behind)",
              confidence: 0.91, kafkaFeature: "KIP-932 Share Groups",
              rationale: "Share group rebalance triggered by consumer join. Checkpointing offsets prevents duplicate delivery.",
            },
            lesson: { notes: "Share group checkpoint prevented duplicate message delivery during rebalance." },
            slackMessage: "✅ Share group rebalance resolved · offsets checkpointed · lag 600→0",
            itsmTicket: `INC-${Date.now().toString().slice(-5)} closed — kafka.checkpointShareGroup resolved share group lag`,
          });
        }, 6000));

        allTimers.push(setTimeout(() => {
          agents = patch(agents, "notification", { status: "online" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          toast(dispatch, "✅ Share-group rebalance approved and handled", "success");
        }, 9000));

      } else {
        // ── REJECTED PATH ──────────────────────────────────────────────────
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          dispatch({ type: "audit", record: auditRec("monitor", "Action REJECTED by operator — kafka.checkpointShareGroup not executed, scenario aborted", "approval") });
          toast(dispatch, "🚫 Rejected — kafka.checkpointShareGroup not executed", "error");
        }, 300));
      }
    };
  }, 6000));

  return () => { allTimers.forEach(clearTimeout); };
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
      dispatch({ type: "audit", record: auditRec("monitor", "Partition imbalance: broker-3 holds 62% of leaders (threshold: 40%)", "reasoning") });
    }},
    { ms: 2500, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Triggering preferred-leader election to rebalance partition leadership", "tool-call") });
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
      dispatch({ type: "audit", record: auditRec("notification", "Slack #sre-alerts posted · ITSM ticket opened · email summary dispatched", "notification") });
      sendEmail(dispatch, "partition-imbalance", "Partition Imbalance", 0, 0, "kafka.rebalancePartitions", {
        approvedBy: "system",
        reasoning: {
          rootCause: "Partition imbalance detected: broker-0 carrying 60% of partition load",
          confidence: 0.88, kafkaFeature: "KIP-848 Cooperative Rebalancing",
          rationale: "Load imbalance detected during cooperative rebalance. Alert suppressed — imbalance self-correcting.",
        },
        lesson: { notes: "Partition rebalance was self-correcting. No manual intervention needed." },
        slackMessage: "ℹ️ Partition imbalance rebalance completed — load redistributed across brokers",
        itsmTicket: `INC-${Date.now().toString().slice(-5)} closed — kafka.rebalancePartitions resolved load imbalance`,
      });
    }},
    { ms: 9000, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
      toast(dispatch, "✅ Partition imbalance resolved", "success");
    }},
  ]);
}

// ── Benign rebalance (false-positive suppression) ────────────────────────────

function runBenignRebalance(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  return schedule([
    { ms: 0, fn: () => {
      agents = patch(agents, "intake", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      particle(dispatch, "e-req", "intake", "monitor");
      toast(dispatch, "🔍 Rebalance event detected — analysing…", "info");
      dispatch({ type: "audit", record: auditRec("intake", "Published rebalance-detected event to ops.requests.v1", "publish", "ops.requests.v1") });
    }},
    { ms: 2000, fn: () => {
      agents = patch(agents, "intake",  { status: "online" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
      dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(120), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "Rebalance detected on payments-consumer — cross-correlating broker + JVM metrics", "reasoning") });
      particle(dispatch, "e-metrics", "monitor", "monitor");
    }},
    { ms: 4500, fn: () => {
      dispatch({ type: "audit", record: auditRec("monitor", "Lag 120 msgs within SLO · JVM heap 41% normal · rolling restart pattern confirmed — false positive", "reasoning") });
    }},
    { ms: 6000, fn: () => {
      agents = patch(agents, "monitor", { status: "acting", mralPhase: "act",
        lastReasoning: {
          rootCause: "Routine rolling-restart rebalance — not a lag anomaly",
          confidence: 0.97,
          kafkaFeatureCited: "KIP-848 Share Groups",
          rebalanceState: "Rebalancing",
          controllerEpoch: 14,
          crossCorrelation: { brokers: "healthy", jvmHeap: "41%", networkInRate: "normal", rebalanceInProgress: true },
          recommendedAction: "suppress — no action needed",
          requiresApproval: false,
          rationale: "Lag 120 msgs/s is within SLO. Rebalance is routine rolling restart, not a spike. Suppressing alert to prevent alert fatigue.",
          lessonsCited: ["lesson-007"],
        }
      });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", "False-positive suppressed — routine rebalance, no action required", "tool-call") });
      toast(dispatch, "🛡️ False positive suppressed — no action needed", "success");
      particle(dispatch, "e-inc", "monitor", "writer");
    }},
    { ms: 9000, fn: () => {
      agents = patch(agents, "monitor", { status: "online" });
      agents = patch(agents, "writer",  { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("writer", "Consuming incident · drafting false-positive suppression record", "consume") });
      particle(dispatch, "e-aud", "writer", "notification");
    }},
    { ms: 12000, fn: () => {
      agents = patch(agents, "writer",       { status: "online" });
      agents = patch(agents, "notification", { status: "acting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Alert suppressed", message: "🛡️ Routine rebalance — false positive suppressed, SLO intact, no ticket opened", scenarioId: "benign-rebalance" } });
      dispatch({ type: "audit", record: auditRec("notification", "Slack #sre-alerts posted · no ITSM ticket (false positive) · email summary dispatched", "notification") });
      toast(dispatch, "✅ False-positive suppression complete — email summary sent", "success");
      sendEmail(dispatch, "benign-rebalance", "False-Positive Suppression", 120, 0, "kafka.suppressRebalancePage", {
        approvedBy: "system",
        reasoning: {
          rootCause: "Rebalance alert fired during expected KIP-848 cooperative rebalance (120 msgs lag)",
          confidence: 0.97, kafkaFeature: "KIP-848 Cooperative Rebalancing",
          rationale: "Lag within cooperative rebalance tolerance (< 300 msgs). Alert is a false positive — suppressing page.",
        },
        lesson: { notes: "False-positive suppression worked correctly. Rebalance lag < 300 msgs is within tolerance." },
        slackMessage: "🔇 Alert suppressed — benign cooperative rebalance in progress (lag 120, within tolerance)",
        itsmTicket: `INC-${Date.now().toString().slice(-5)} closed — kafka.suppressRebalancePage — false positive suppressed`,
      });
    }},
    { ms: 15000, fn: () => {
      agents = patch(agents, "notification", { status: "online" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
    }},
  ]);
}

// ── Extra failure scenarios ───────────────────────────────────────────────────

// 1. Schema Registry Mismatch — Avro producer v2 vs consumer v1
function runSchemaMismatch(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  const LAG = 8400;

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 2, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Avro deserialization failure alert on payments.transactions.v1 — schema id=15 received, id=12 expected", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "⚠️ Schema mismatch — payments.transactions.v1", "warning");
  });

  t(2000, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "Schema Registry version conflict — Avro v2 vs v1", kafkaFeature: "Schema Registry", confidence: 0.93,
        rationale: "Producers upgraded to Avro schema v2; consumers still registered for v1. BACKWARD compatibility not enforced in schema registry." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 2, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Deserialization exception rate: 340/min on payment-processor group. Schema id=12 expected, id=15 received. BACKWARD compatibility broken.", "reasoning") });
  });

  t(4000, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "8,400 msgs queued · 340 DLQ writes/min · all partitions affected. Resolution: re-register schema v2 in BACKWARD_TRANSITIVE mode + hot-redeploy consumers.", "reasoning") });
  });

  t(6000, () => {
    const toolCall: MCPToolCall = {
      jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.updateSchemaCompatibility", arguments: { subject: "payments.transactions-value", fromVersion: 12, toVersion: 15, mode: "BACKWARD_TRANSITIVE" } },
    };
    const approval: ApprovalRequest = {
      id: uid(), ts: Date.now(), createdAt: Date.now(), agent: "monitor", proposedBy: "monitor-agent",
      toolCall, scenarioId: "schema-mismatch",
      reason: "Schema mutation requires operator sign-off — affects all active consumers of payments.transactions.v1",
      status: "pending",
    };
    agents = patch(agents, "monitor", { status: "awaiting-approval", mralPhase: "awaiting" });
    dispatch({ type: "state", payload: { agents, mralPhase: "awaiting", broker: mockBroker(LAG), pendingApprovals: [approval], incidentQueueDepth: 2, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Awaiting approval: kafka.updateSchemaCompatibility (schema mutation is policy-gated)", "approval") });
    toast(dispatch, "⏳ Schema update approval required", "warning");

    _pendingApprovalCallback = (approved) => {
      if (approved) {
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 2, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Approved — executing kafka.updateSchemaCompatibility(BACKWARD_TRANSITIVE) + consumer hot-redeploy", "tool-call") });
          particle(dispatch, "e-inc", "monitor", "writer");
        }, 0));
        allTimers.push(setTimeout(() => {
          dispatch({ type: "audit", record: auditRec("monitor", "Schema compatibility updated to BACKWARD_TRANSITIVE. Consumers redeploying with dual-read (v1+v2) support. Lag draining at 720 msg/s.", "tool-call") });
        }, 2200));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(300), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("writer", "Drafting schema compatibility incident postmortem — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
          particle(dispatch, "e-aud", "writer", "notification");
        }, 4000));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("notification", "Slack #schema-alerts + ITSM ticket opened for schema compatibility incident", "notification") });
          dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Schema Mismatch Resolved", message: "✅ Schema v2 BACKWARD_TRANSITIVE applied — consumers healthy, lag 8400→0", scenarioId: "schema-mismatch" } });
          sendEmail(dispatch, "schema-mismatch", "Schema Registry Mismatch", LAG, 0, "kafka.updateSchemaCompatibility(BACKWARD_TRANSITIVE)", {
            approved: true, approvedBy: "operator",
            reasoning: { rootCause: "Avro schema v2 deployed without BACKWARD_TRANSITIVE mode — broke consumers on payments.transactions.v1", confidence: 0.93, kafkaFeature: "Schema Registry", rationale: "Re-registered v2 schema in BACKWARD_TRANSITIVE mode; consumers hot-redeployed with dual-read support." },
            lesson: { notes: "Always register new Avro schemas in BACKWARD_TRANSITIVE mode before deploying producers. Gate producer releases on schema compatibility check." },
            slackMessage: "✅ Schema mismatch resolved on payments.transactions.v1 — lag 8400→0",
            itsmTicket: `INC-${Date.now().toString().slice(-5)} — Schema Registry Mismatch`,
          });
        }, 6000));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
          dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Lesson persisted: Always use BACKWARD_TRANSITIVE for schema evolution. Gate producer releases on registry validation.", "lesson") });
          dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "schema-mismatch", actionTaken: "kafka.updateSchemaCompatibility", notes: "Use BACKWARD_TRANSITIVE; validate schemas pre-deploy." } });
        }, 8500));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
        }, 10500));
      } else {
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          dispatch({ type: "audit", record: auditRec("monitor", "Schema update rejected — manual schema rollback required", "approval") });
          sendEmail(dispatch, "schema-mismatch", "Schema Registry Mismatch", LAG, LAG, "Rejected — no schema change applied", { approved: false, approvedBy: "operator" });
        }, 0));
      }
    };
  });

  return () => allTimers.forEach(clearTimeout);
}

// 2. Disk Saturation — broker log disk >92%, compaction falling behind
function runDiskSaturation(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Broker-2 disk utilisation 92.4% — log compaction falling behind, segment accumulation detected", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "🔴 Disk saturation on broker-2 (92.4%)", "error");
  });

  t(2000, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "Uncompacted segment accumulation on broker-2", kafkaFeature: "Log Compaction", confidence: 0.91,
        rationale: "log.cleaner.threads=1 saturated; dirty ratio 0.78 on invoices.created.v1 (target 0.5). 12GB uncompacted segments in last 6h." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Root cause: log.cleaner.threads=1 saturated. Dirty ratio 0.78 on invoices.created.v1 (threshold 0.5). 12GB uncompacted in 6h.", "reasoning") });
  });

  t(4200, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Action plan: (1) Increase log.cleaner.threads to 4 (2) Force-compact invoices.created.v1 (3) Adjust retention.bytes for safety headroom.", "reasoning") });
  });

  t(6200, () => {
    agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "kafka.updateBrokerConfig(broker=2, log.cleaner.threads=4) · kafka.forceCompact(topic=invoices.created.v1)", "tool-call") });
    particle(dispatch, "e-inc", "monitor", "writer");
  });

  t(8500, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Compaction throughput ↑ 4×. Disk utilisation declining: 92.4%→81.2% over 8 min. Retention.bytes set to 85% of disk capacity.", "tool-call") });
  });

  t(10500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("writer", "Disk saturation postmortem drafted — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
    particle(dispatch, "e-aud", "writer", "notification");
  });

  t(13000, () => {
    agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("notification", "Slack #sre-infra: disk saturation resolved. ITSM INC auto-closed.", "notification") });
    dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Disk Saturation Resolved", message: "✅ Broker-2 disk: 92.4%→81.2% · compaction threads ×4 · headroom restored", scenarioId: "disk-saturation" } });
    sendEmail(dispatch, "disk-saturation", "Broker Disk Saturation", 0, 0, "kafka.updateBrokerConfig + kafka.forceCompact", {
      reasoning: { rootCause: "log.cleaner.threads=1 saturated; dirty ratio 0.78 on invoices.created.v1", confidence: 0.91, kafkaFeature: "Log Compaction", rationale: "Increased cleaner threads to 4, forced compaction. Disk 92.4%→81.2% in 8 min." },
      lesson: { notes: "Set log.retention.bytes with 20% headroom. Alert at dirty-ratio > 0.6. Use 2+ cleaner threads on high-volume topics." },
      slackMessage: "✅ Broker-2 disk saturation resolved — 92.4%→81.2%",
      itsmTicket: `INC-${Date.now().toString().slice(-5)} — Broker Disk Saturation`,
    });
  });

  t(15500, () => {
    agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
    dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Lesson: alert at dirty-ratio > 0.6 and disk > 80%. Run 2+ cleaner threads; set retention.bytes = 0.85 × disk.", "lesson") });
    dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "disk-saturation", actionTaken: "kafka.forceCompact + increaseCleanerThreads", notes: "Alert at dirty-ratio>0.6. 2+ cleaner threads required." } });
  });

  t(17500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
  });

  return () => allTimers.forEach(clearTimeout);
}

// 3. Under-Replication — ISR drops below minISR on settlements topic
function runUnderReplication(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  const LAG = 18900;

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 3, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "CRITICAL: payments.settlements.v1 ISR=1, minISR=2 — broker-3 replica DEAD, producer acks blocked", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "🔴 Under-replicated partitions — settlements topic", "error");
  });

  t(2200, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "Broker-3 OOM crash — ISR shrunk to 1, minISR=2 violated", kafkaFeature: "KRaft ISR Management", confidence: 0.97,
        rationale: "Broker-3 suffered JVM OOM. ISR on payments.settlements.v1 dropped to 1/3. Producers receiving NotEnoughReplicasException. Settlement batches at risk." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 3, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Broker-3 OOM. ISR: 3→1 on payments.settlements.v1 (all 4 partitions). Producers blocked on acks=all. 18,900 msg backlog.", "reasoning") });
  });

  t(4500, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Resolution: reassign under-replicated partitions to broker-1 (headroom 34%). Broker-3 recovery ETA: 8 min after restart.", "reasoning") });
  });

  t(6500, () => {
    const toolCall: MCPToolCall = {
      jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.reassignPartitions", arguments: { topic: "payments.settlements.v1", fromBroker: 3, toBroker: 1, partitions: [0, 1, 2, 3] } },
    };
    const approval: ApprovalRequest = {
      id: uid(), ts: Date.now(), createdAt: Date.now(), agent: "monitor", proposedBy: "monitor-agent",
      toolCall, scenarioId: "under-replication",
      reason: "Partition reassignment is an irreversible cluster mutation — operator confirmation required",
      status: "pending",
    };
    agents = patch(agents, "monitor", { status: "awaiting-approval", mralPhase: "awaiting" });
    dispatch({ type: "state", payload: { agents, mralPhase: "awaiting", broker: mockBroker(LAG), pendingApprovals: [approval], incidentQueueDepth: 3, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Awaiting approval: kafka.reassignPartitions — partition mutation requires operator sign-off", "approval") });
    toast(dispatch, "⏳ Partition reassignment approval required", "warning");

    _pendingApprovalCallback = (approved) => {
      if (approved) {
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 3, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Approved — executing kafka.reassignPartitions(settlements.v1, broker-3→broker-1, partitions=[0,1,2,3])", "tool-call") });
          particle(dispatch, "e-inc", "monitor", "writer");
        }, 0));
        allTimers.push(setTimeout(() => {
          dispatch({ type: "audit", record: auditRec("monitor", "Reassignment complete. ISR restored to 3/3. Lag draining: 18,900→6,200 at 3,100 msg/s. Producer acks unblocked.", "tool-call") });
        }, 2500));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(800), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("writer", "Under-replication RCA drafted — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
          particle(dispatch, "e-aud", "writer", "notification");
        }, 4500));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("notification", "PagerDuty SEV-1 auto-resolved. Slack #sre-critical: ISR restored.", "notification") });
          dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "itsm", title: "Under-Replication Resolved", message: "✅ payments.settlements.v1 ISR restored 1→3. Lag 18,900→0. SEV-1 resolved.", scenarioId: "under-replication" } });
          sendEmail(dispatch, "under-replication", "Under-Replicated Partitions", LAG, 0, "kafka.reassignPartitions(broker-3→broker-1)", {
            approved: true, approvedBy: "operator",
            reasoning: { rootCause: "Broker-3 JVM OOM — ISR on payments.settlements.v1 shrunk to 1 (minISR=2)", confidence: 0.97, kafkaFeature: "KRaft ISR Management", rationale: "Reassigned 4 partitions to broker-1. ISR restored. Lag fully drained." },
            lesson: { notes: "Set JVM heap to 70% of RAM with -XX:+UseG1GC. Alert at ISR < minISR within 30s. Keep broker-3 headroom >30%." },
            slackMessage: "✅ payments.settlements.v1 ISR restored 1→3. Settlement batches resuming.",
            itsmTicket: `SEV1-${Date.now().toString().slice(-5)} — Under-Replicated Partitions AUTO-RESOLVED`,
          });
        }, 6500));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
          dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", "Lesson: JVM heap G1GC tuning prevents OOM. ISR alert SLO: 30s. Maintain broker headroom >30%.", "lesson") });
          dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "under-replication", actionTaken: "kafka.reassignPartitions", notes: "G1GC + ISR alert within 30s." } });
        }, 9000));
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
        }, 11000));
      } else {
        allTimers.push(setTimeout(() => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(LAG), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          dispatch({ type: "audit", record: auditRec("monitor", "Partition reassignment rejected — under-replication persists, manual intervention needed", "approval") });
          sendEmail(dispatch, "under-replication", "Under-Replicated Partitions", LAG, LAG, "Rejected — no partition reassignment", { approved: false, approvedBy: "operator" });
        }, 0));
      }
    };
  });

  return () => allTimers.forEach(clearTimeout);
}

// 4. Producer Timeout Storm — batch accumulator exhausted, linger.ms backpressure
function runProducerTimeout(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(2100), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Producer timeout storm on payments-gateway: record-queue-time P99 > 4,800ms, batch accumulator full", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "⚠️ Producer timeout storm — payments-gateway", "warning");
  });

  t(2000, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "Producer batch accumulator exhausted — linger.ms/batch.size mismatch under peak load", kafkaFeature: "Producer Config", confidence: 0.89,
        rationale: "batch.size=65536 (64KB) and linger.ms=5 causing 340 batches/s to exhaust accumulator at 1.2× normal throughput. Record-queue-time P99 hit 4.8s." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(2100), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "record-queue-time P99: 4.8s (SLO: 500ms). batch.size 64KB saturated at 340 batches/s peak. buffer.memory 80% consumed.", "reasoning") });
  });

  t(4000, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Fix: increase batch.size to 2MB and linger.ms to 20ms. This reduces batch frequency by 32× while improving throughput and reducing broker I/O pressure.", "reasoning") });
  });

  t(6000, () => {
    agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(2100), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "kafka.tuneProducerConfig(client=payments-gateway, batch.size=2097152, linger.ms=20, buffer.memory=67108864)", "tool-call") });
    particle(dispatch, "e-inc", "monitor", "writer");
  });

  t(8000, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Config applied. record-queue-time P99 dropping: 4.8s→620ms→180ms. Batch efficiency ↑ 28×. Lag clearing.", "tool-call") });
  });

  t(10200, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("writer", "Producer tuning incident report drafted — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
    particle(dispatch, "e-aud", "writer", "notification");
  });

  t(12500, () => {
    agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("notification", "Slack #producer-ops: timeout storm resolved. Config change documented in runbook.", "notification") });
    dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Producer Timeout Resolved", message: "✅ payments-gateway P99 queue-time 4.8s→180ms · batch.size 64KB→2MB", scenarioId: "producer-timeout" } });
    sendEmail(dispatch, "producer-timeout", "Producer Timeout Storm", 2100, 0, "kafka.tuneProducerConfig(batch.size=2MB, linger.ms=20)", {
      reasoning: { rootCause: "batch.size=64KB exhausted under 1.2× peak throughput on payments-gateway", confidence: 0.89, kafkaFeature: "Producer Config", rationale: "Increased batch.size to 2MB and linger.ms to 20ms — 28× batch efficiency gain, P99 4.8s→180ms." },
      lesson: { notes: "Set batch.size=2MB and linger.ms=15-20ms for high-throughput producers. Alert on record-queue-time P99 > 500ms." },
      slackMessage: "✅ Producer timeout storm resolved — P99 queue-time 4.8s→180ms",
      itsmTicket: `INC-${Date.now().toString().slice(-5)} — Producer Timeout Storm`,
    });
  });

  t(15000, () => {
    agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
    dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Lesson: batch.size=2MB + linger.ms=20 for high-throughput producers. Alert on record-queue-time P99>500ms.", "lesson") });
    dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "producer-timeout", actionTaken: "kafka.tuneProducerConfig", notes: "batch.size 2MB + linger.ms 20 for peak traffic." } });
  });

  t(17000, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
  });

  return () => allTimers.forEach(clearTimeout);
}

// 5. Consumer Group Session Timeout — GC pause causes heartbeat miss
function runConsumerSessionTimeout(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(3400), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Group coordinator heartbeat miss on payment-processor (3 members) — session.timeout.ms=10000 exceeded by GC pause", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "⚠️ Consumer group session timeout storm", "warning");
  });

  t(2200, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "JVM GC pause (P99: 12.4s) exceeds session.timeout.ms=10s on payment-processor", kafkaFeature: "Consumer Group Protocol", confidence: 0.92,
        rationale: "payment-processor instances running CMS GC with 4GB heap. GC pause P99 12.4s > session.timeout 10s. Group coordinator evicting members every ~90s." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(3400), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "GC pause P99=12.4s on payment-processor instances. session.timeout=10s evicts members. Triggers full rebalance · 3,400 msg lag spike per event.", "reasoning") });
  });

  t(4500, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Fix: increase session.timeout.ms to 45s, heartbeat.interval.ms to 15s. Switch to G1GC. Fixes heartbeat miss without hiding real failures.", "reasoning") });
  });

  t(6500, () => {
    agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(3400), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "kafka.updateConsumerConfig(group=payment-processor, session.timeout.ms=45000, heartbeat.interval.ms=15000)", "tool-call") });
    particle(dispatch, "e-inc", "monitor", "writer");
  });

  t(8500, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Config applied. Heartbeat miss rate: 12/hour→0. Rebalance frequency dropped from 40/hour→2/hour. Lag clearing.", "tool-call") });
  });

  t(10500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("writer", "Consumer session timeout runbook updated — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
    particle(dispatch, "e-aud", "writer", "notification");
  });

  t(13000, () => {
    agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("notification", "Slack #consumer-ops: session timeout resolved. G1GC migration ticket opened.", "notification") });
    dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Session Timeout Resolved", message: "✅ payment-processor rebalance storm resolved · session.timeout 10s→45s · lag clear", scenarioId: "consumer-session-timeout" } });
    sendEmail(dispatch, "consumer-session-timeout", "Consumer Session Timeout Storm", 3400, 0, "kafka.updateConsumerConfig(session.timeout.ms=45000)", {
      reasoning: { rootCause: "GC pause P99=12.4s exceeds session.timeout.ms=10s on payment-processor group", confidence: 0.92, kafkaFeature: "Consumer Group Protocol", rationale: "Increased session.timeout to 45s and heartbeat.interval to 15s. Rebalance storm eliminated." },
      lesson: { notes: "Set session.timeout.ms = 3× GC pause P99. Use G1GC to keep pauses <1s. Alert on rebalance rate >5/hour." },
      slackMessage: "✅ Consumer session timeout resolved — rebalance storm stopped",
      itsmTicket: `INC-${Date.now().toString().slice(-5)} — Consumer Session Timeout Storm`,
    });
  });

  t(15500, () => {
    agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
    dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Lesson: session.timeout = 3× GC P99. G1GC migration scheduled. Alert threshold: rebalance rate >5/hour.", "lesson") });
    dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "consumer-session-timeout", actionTaken: "kafka.updateConsumerConfig", notes: "session.timeout = 3× GC P99; switch to G1GC." } });
  });

  t(17500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
  });

  return () => allTimers.forEach(clearTimeout);
}

// 6. Compaction Lag — __consumer_offsets and invoice topic cleaner threads saturated
function runCompactionLag(dispatch: DispatchFn): () => void {
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", "Log compaction lag alert: invoices.created.v1 uncompacted ratio 0.82 (threshold 0.5) · __consumer_offsets cleaner queue depth: 240", "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, "⚠️ Compaction lag — invoices topic growing unbounded", "warning");
  });

  t(2000, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause: "Cleaner thread backlog — tombstone write rate exceeds compaction throughput", kafkaFeature: "Log Compaction", confidence: 0.88,
        rationale: "invoices.created.v1 tombstone write rate 8,000/min exceeds cleaner throughput of 5,200/min. __consumer_offsets compaction queue depth 240 (healthy: <20)." } });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Tombstone write rate 8,000/min > cleaner capacity 5,200/min. Queue depth 240. Topic size growing 2.1GB/day. Retention at risk.", "reasoning") });
  });

  t(4000, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Fix: increase log.cleaner.threads 2→6, reduce min.compaction.lag.ms 86400000→3600000 (1h), adjust min.cleanable.dirty.ratio to 0.3.", "reasoning") });
  });

  t(6200, () => {
    agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "kafka.updateTopicConfig(topic=invoices.created.v1, min.compaction.lag.ms=3600000, min.cleanable.dirty.ratio=0.3) + log.cleaner.threads=6", "tool-call") });
    particle(dispatch, "e-inc", "monitor", "writer");
  });

  t(8500, () => {
    dispatch({ type: "audit", record: auditRec("monitor", "Cleaner throughput ↑ 3×: 5,200→15,600/min. Queue depth dropping: 240→87→12. Topic growth rate normalising.", "tool-call") });
  });

  t(10500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("writer", "Compaction lag postmortem drafted — publishing to ops.actions.audit.v1", "consume", "ops.incidents.v1") });
    particle(dispatch, "e-aud", "writer", "notification");
  });

  t(13000, () => {
    agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("notification", "Slack #kafka-ops: compaction lag resolved. Runbook updated with cleaner tuning guide.", "notification") });
    dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Compaction Lag Resolved", message: "✅ invoices.created.v1 cleaner queue 240→12 · topic growth normalised", scenarioId: "compaction-lag" } });
    sendEmail(dispatch, "compaction-lag", "Log Compaction Lag", 0, 0, "kafka.updateTopicConfig(min.compaction.lag.ms=1h, cleanerThreads=6)", {
      reasoning: { rootCause: "Tombstone write rate 8,000/min exceeded cleaner throughput 5,200/min on invoices.created.v1", confidence: 0.88, kafkaFeature: "Log Compaction", rationale: "Increased cleaner threads 2→6. Queue depth 240→12. Topic growth rate normalised." },
      lesson: { notes: "Monitor log.cleaner.backoff.ms and queue depth. Alert when uncompacted ratio >0.5. Run 4+ cleaner threads on compacted topics." },
      slackMessage: "✅ Compaction lag resolved — invoices.created.v1 cleaner queue 240→12",
      itsmTicket: `INC-${Date.now().toString().slice(-5)} — Log Compaction Lag`,
    });
  });

  t(15500, () => {
    agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
    dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", "Lesson: 4+ cleaner threads for compacted topics. Alert: uncompacted ratio >0.5. min.compaction.lag.ms = 1h for invoicing topics.", "lesson") });
    dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "compaction-lag", actionTaken: "kafka.updateTopicConfig + increaseCleanerThreads", notes: "4+ cleaner threads, alert at dirty-ratio>0.5." } });
  });

  t(17500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
  });

  return () => allTimers.forEach(clearTimeout);
}

// ── Topic management scenario ─────────────────────────────────────────────────
// Triggered when user edits, deletes, or creates a Kafka topic via the
// Topics panel. Runs a full MRAL cycle: Monitor detects the change, reasons
// about consumer/partition impact, acts via tool call, Writer drafts the
// change record, Notification dispatches Slack + ITSM.

export interface TopicChangePayload {
  operation: "edit" | "delete" | "create";
  topic: { name: string; partitions: number; replicationFactor: number; retentionHours: number };
  prevTopic?: { name: string; partitions: number };
}

export function runTopicManagement(payload: TopicChangePayload, dispatch: DispatchFn): () => void {
  const { operation, topic, prevTopic } = payload;
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  const opLabel = operation === "edit" ? "Topic Config Update"
    : operation === "delete" ? "Topic Deletion" : "New Topic Created";
  const partitionDelta = (prevTopic && operation === "edit") ? topic.partitions - prevTopic.partitions : 0;

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", `Published ${opLabel} event for ${topic.name} to ops.requests.v1`, "publish", "ops.requests.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, `📋 ${opLabel}: ${topic.name}`, "info");
  });

  t(2000, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "reason" });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    if (operation === "delete") {
      dispatch({ type: "audit", record: auditRec("monitor", `Topic deletion requested: ${topic.name} — checking active consumer groups and pending offsets`, "reasoning") });
    } else if (operation === "edit" && partitionDelta > 0) {
      dispatch({ type: "audit", record: auditRec("monitor", `Partition increase detected: ${topic.name} ${prevTopic?.partitions}→${topic.partitions} — assessing cooperative rebalance impact`, "reasoning") });
    } else if (operation === "edit" && partitionDelta < 0) {
      dispatch({ type: "audit", record: auditRec("monitor", `⚠️ Partition decrease ${topic.name} ${prevTopic?.partitions}→${topic.partitions} — WARNING: partition reduction causes data redistribution`, "reasoning") });
    } else if (operation === "create") {
      dispatch({ type: "audit", record: auditRec("monitor", `New topic ${topic.name}: ${topic.partitions}p × RF${topic.replicationFactor} · retention ${topic.retentionHours}h — validating broker capacity`, "reasoning") });
    } else {
      dispatch({ type: "audit", record: auditRec("monitor", `Config update for ${topic.name}: retention=${topic.retentionHours}h RF=${topic.replicationFactor} — assessing impact`, "reasoning") });
    }
  });

  t(4500, () => {
    if (operation === "delete") {
      dispatch({ type: "audit", record: auditRec("monitor", `No active lag on ${topic.name} · consumer groups will auto-unsubscribe · safe to proceed`, "reasoning") });
    } else if (partitionDelta > 0) {
      dispatch({ type: "audit", record: auditRec("monitor", `Partition increase triggers KIP-848 cooperative rebalance · consumers will redistribute · ~10s rebalance window expected`, "reasoning") });
    } else if (operation === "create") {
      dispatch({ type: "audit", record: auditRec("monitor", `Broker capacity sufficient · partition assignment plan: ${topic.partitions} leaders spread across 3 brokers · RF=${topic.replicationFactor} satisfies durability`, "reasoning") });
    }
  });

  t(6500, () => {
    agents = patch(agents, "monitor", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    if (operation === "delete") {
      dispatch({ type: "audit", record: auditRec("monitor", `kafka.deleteTopic(topic=${topic.name}) — partitions removed, offsets cleared, consumer groups unsubscribed`, "tool-call") });
    } else if (operation === "edit") {
      dispatch({ type: "audit", record: auditRec("monitor", `kafka.alterTopicConfig(topic=${topic.name}, partitions=${topic.partitions}, retentionMs=${topic.retentionHours * 3600000}) — applied`, "tool-call") });
    } else {
      dispatch({ type: "audit", record: auditRec("monitor", `kafka.createTopic(name=${topic.name}, partitions=${topic.partitions}, replicationFactor=${topic.replicationFactor}) — topic online`, "tool-call") });
    }
    particle(dispatch, "e-inc", "monitor", "writer");
  });

  t(9500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("writer", `Drafting ${opLabel} change record · publishing to ops.actions.audit.v1`, "consume") });
    particle(dispatch, "e-aud", "writer", "notification");
  });

  t(12500, () => {
    agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("notification", `Slack #kafka-ops posted · ITSM change record INC-${Date.now().toString().slice(-5)} opened`, "notification") });
    dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: opLabel, message: `✅ ${opLabel}: ${topic.name} (${topic.partitions}p RF${topic.replicationFactor})`, scenarioId: "topic-management" } });
    toast(dispatch, `✅ ${opLabel} complete — Slack + ITSM notified`, "success");
  });

  t(15000, () => {
    agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
    dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", `Lesson recorded: ${opLabel} on ${topic.name} completed — change log persisted to ops.lessons.v1`, "lesson") });
    particle(dispatch, "e-learn", "monitor", "monitor");
  });

  t(17500, () => {
    agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
    dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(0), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
    // Fire email + populate emailSummary so notification strip click re-opens the summary
    const actionVerb = operation === "delete" ? "Deleted" : operation === "edit" ? "Reconfigured" : "Created";
    sendEmail(dispatch, "topic-management", opLabel, 0, 0,
      `${actionVerb} topic: ${topic.name}`,
      {
        approved: true,
        approvedBy: "operator",
        reasoning: {
          rootCause: operation === "delete"
            ? `Topic ${topic.name} deletion requested`
            : operation === "edit"
            ? `Topic ${topic.name} configuration update`
            : `New topic ${topic.name} provisioned`,
          kafkaFeature: "kafka.admin",
          confidence: 0.97,
          rationale: operation === "delete"
            ? `Consumer groups confirmed idle. All offsets cleared. Topic safely removed.`
            : operation === "edit"
            ? `Config applied — partitions: ${topic.partitions}, retention: ${topic.retentionHours}h, RF: ${topic.replicationFactor}.${partitionDelta > 0 ? ` KIP-848 rebalance triggered.` : ""}`
            : `Topic created: ${topic.partitions}p × RF${topic.replicationFactor} · ${topic.retentionHours}h retention.`,
        },
        lesson: {
          notes: `${opLabel} on ${topic.name} completed. Change record persisted to ops.lessons.v1.`,
          adjustedThreshold: undefined,
        },
        slackMessage: `✅ ${opLabel}: ${topic.name} (${topic.partitions}p RF${topic.replicationFactor})`,
        itsmTicket: `CHG-${Date.now().toString().slice(-5)} — ${opLabel}`,
      }
    );
  });

  return () => allTimers.forEach(clearTimeout);
}

// ── Topic Heal scenario ───────────────────────────────────────────────────────
// Triggered when user clicks "Heal" on a degraded/critical topic.
// Runs a full MRAL cycle: Monitor detects unhealthy state → Reason about root
// cause → (critical: approval gate) Act to restore → Learn.

export interface TopicHealPayload {
  topicName: string;
  currentStatus: "degraded" | "critical";
  lagTotal: number;
  partitions: number;
}

export function runTopicHeal(payload: TopicHealPayload, dispatch: DispatchFn, onComplete?: () => void): () => void {
  const { topicName, currentStatus, lagTotal, partitions } = payload;
  let agents = baseAgents();
  const allTimers: ReturnType<typeof setTimeout>[] = [];
  function t(ms: number, fn: () => void) { allTimers.push(setTimeout(fn, ms)); }

  const isCritical = currentStatus === "critical";
  const lagAfter = Math.round(lagTotal * (isCritical ? 0.04 : 0.09));
  const healAction = isCritical
    ? `kafka.restartConsumerGroup + kafka.scaleReplicas(topic=${topicName}, delta=+2)`
    : `kafka.scaleConsumers(topic=${topicName}, delta=+${Math.max(1, Math.floor(partitions / 4))})`;
  const rootCause = isCritical
    ? `Critical consumer backlog on ${topicName}: ${lagTotal.toLocaleString()} msg lag. Dead consumer replica detected.`
    : `Degraded consumer throughput on ${topicName}: lag ${lagTotal.toLocaleString()} exceeds SLO threshold 2,800.`;
  const rationale = isCritical
    ? `Restarted dead consumer group replica. Added 2 consumer instances. Lag cleared ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()} (${Math.round((1 - lagAfter / lagTotal) * 100)}% reduction).`
    : `Scaled consumer group by +${Math.max(1, Math.floor(partitions / 4))} replicas. Lag drained ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()} (${Math.round((1 - lagAfter / lagTotal) * 100)}% reduction).`;

  t(0, () => {
    agents = patch(agents, "intake", { status: "acting", mralPhase: "act" });
    dispatch({ type: "state", payload: { agents, mralPhase: "monitor", broker: mockBroker(lagTotal), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("intake", `${isCritical ? "🔴 CRITICAL" : "🟡 DEGRADED"} alert: ${topicName} lag=${lagTotal.toLocaleString()} — published to ops.incidents.v1`, "publish", "ops.incidents.v1") });
    particle(dispatch, "e-req", "intake", "monitor");
    toast(dispatch, `${isCritical ? "🔴 Critical" : "⚠️ Degraded"} topic detected — initiating MRAL heal cycle`, isCritical ? "error" : "warning");
  });

  t(1800, () => {
    agents = patch(agents, "intake", { status: "online", mralPhase: "idle" });
    agents = patch(agents, "monitor", {
      status: "reasoning", mralPhase: "reason",
      lastReasoning: { rootCause, kafkaFeature: isCritical ? "KIP-848" : "Consumer Groups", confidence: isCritical ? 0.93 : 0.87, rationale },
    });
    dispatch({ type: "state", payload: { agents, mralPhase: "reason", broker: mockBroker(lagTotal), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
    dispatch({ type: "audit", record: auditRec("monitor", rootCause, "reasoning") });
  });

  t(3500, () => {
    dispatch({ type: "audit", record: auditRec("monitor",
      isCritical
        ? `Consumer group health check: 1 of 3 replicas unresponsive (session.timeout exceeded). Lag growth rate: +${Math.round(lagTotal / 100)}/s. Escalation required.`
        : `Consumer lag trend: +${Math.round(lagTotal / 200)}/s for past 8m. Throughput gap = ${Math.max(1, Math.floor(partitions / 4))} replica-equivalents. Scale action sufficient.`,
      "reasoning"
    ) });
  });

  t(5200, () => {
    dispatch({ type: "audit", record: auditRec("monitor",
      isCritical
        ? `Proposed action: restart dead replica + add 2 consumers. Confidence: 93%. Requires approval — production consumer group mutation.`
        : `Proposed action: scale consumers +${Math.max(1, Math.floor(partitions / 4))} replicas. Confidence: 87%. No approval needed — additive change.`,
      "reasoning"
    ) });
  });

  if (isCritical) {
    // Critical → approval gate before acting
    t(6500, () => {
      const approvalId = uid();
      const toolCall: import("./types").MCPToolCall = {
        id: uid(), ts: Date.now(),
        params: {
          name: "kafka.scaleConsumers",
          arguments: { group: `${topicName}-consumer`, delta: 2, reason: `Critical lag ${lagTotal.toLocaleString()} — dead replica restart required` },
        },
        status: "pending",
      };
      agents = patch(agents, "monitor", { status: "awaiting-approval", mralPhase: "awaiting" });
      dispatch({ type: "state", payload: { agents, mralPhase: "awaiting", broker: mockBroker(lagTotal), pendingApprovals: [{ id: approvalId, toolCall, reason: rootCause, ts: Date.now() }], incidentQueueDepth: 1, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", `Approval gate: restart consumer group + scale replicas on ${topicName}. Awaiting operator sign-off.`, "approval") });
      toast(dispatch, `🔐 Approval required — healing ${topicName}`, "warning");

      _pendingApprovalCallback = (approved) => {
        const actMs = Date.now();
        const delay = (ms: number, fn: () => void) => allTimers.push(setTimeout(fn, ms));

        if (!approved) {
          delay(200, () => {
            agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
            dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(lagTotal), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
            dispatch({ type: "audit", record: auditRec("monitor", `Heal action REJECTED by operator. Topic ${topicName} remains critical — manual intervention required.`, "approval") });
            toast(dispatch, `🚫 Heal rejected — ${topicName} remains critical`, "error");
            sendEmail(dispatch, "topic-heal", `Topic Heal — ${topicName}`, lagTotal, lagTotal, healAction, {
              approved: false, approvedBy: "operator",
              reasoning: { rootCause, kafkaFeature: "KIP-848", confidence: 0.93, rationale },
              lesson: { notes: "Operator rejected heal. Escalate to on-call SRE." },
              slackMessage: `🚫 Heal REJECTED for ${topicName}. Manual review required.`,
              itsmTicket: `INC-${actMs.toString().slice(-5)} — Critical Topic Heal REJECTED`,
            });
          });
          return;
        }

        delay(300, () => {
          agents = patch(agents, "monitor", {
            status: "acting", mralPhase: "act",
            lastAction: { detail: healAction, toolCallId: uid(), outcome: "success" },
          });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagTotal), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", `✅ Approved. Executing: restart dead replica + scale +2 consumers on ${topicName}`, "tool-call") });
          particle(dispatch, "e-inc", "monitor", "writer");
        });

        delay(2800, () => {
          dispatch({ type: "audit", record: auditRec("monitor", `Consumer group restarted. Lag draining: ${lagTotal.toLocaleString()} → ~${lagAfter.toLocaleString()} — recovery confirmed`, "tool-call") });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(Math.round(lagTotal * 0.4)), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          toast(dispatch, `✅ ${topicName} recovering — lag draining`, "success");
          if (onComplete) onComplete();
        });

        delay(5000, () => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("writer", `Postmortem drafted: Critical recovery on ${topicName}. Lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}. RCA documented.`, "consume") });
          particle(dispatch, "e-aud", "writer", "notification");
        });

        delay(7500, () => {
          agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
          dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("notification", `Slack #sre-alerts: ${topicName} recovered. ITSM INC auto-closed. Stakeholders notified.`, "notification") });
          dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Topic Healed", message: `✅ ${topicName} recovered — lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}`, scenarioId: "topic-heal" } });
          toast(dispatch, `✅ ${topicName} fully healed — lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}`, "success");
        });

        delay(9500, () => {
          agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
          agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
          dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
          dispatch({ type: "audit", record: auditRec("monitor", `Lesson: Critical consumer failure on ${topicName}. Add liveness probe on consumer replicas. Alert at lag>10k before reaching critical.`, "lesson") });
          dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "topic-heal", actionTaken: `Consumer restart + scale +2 on ${topicName}`, notes: `Add consumer liveness probes. Alert threshold lag>10,000 for ${topicName}.` } });
        });

        delay(11500, () => {
          agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
          dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
          sendEmail(dispatch, "topic-heal", `Topic Heal — ${topicName}`, lagTotal, lagAfter, healAction, {
            approved: true, approvedBy: "operator",
            reasoning: { rootCause, kafkaFeature: "KIP-848", confidence: 0.93, rationale },
            lesson: { notes: `Critical lag on ${topicName} resolved. Add consumer liveness probes. Alert at lag>10k.`, adjustedThreshold: 10000 },
            slackMessage: `✅ ${topicName} recovered — lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}`,
            itsmTicket: `INC-${actMs.toString().slice(-5)} — Critical Topic Heal`,
          });
        });
      };
    });

  } else {
    // Degraded → no approval needed, act directly
    t(6800, () => {
      agents = patch(agents, "monitor", {
        status: "acting", mralPhase: "act",
        lastAction: { detail: healAction, toolCallId: uid(), outcome: "success" },
      });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagTotal), pendingApprovals: [], incidentQueueDepth: 1, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", `Scaling consumer group: +${Math.max(1, Math.floor(partitions / 4))} replicas on ${topicName} — cooperative rebalance initiated`, "tool-call") });
      particle(dispatch, "e-inc", "monitor", "writer");
      if (onComplete) onComplete();
    });

    t(9000, () => {
      dispatch({ type: "audit", record: auditRec("monitor", `Lag draining: ${lagTotal.toLocaleString()} → ~${lagAfter.toLocaleString()} — consumer throughput increased by ${Math.round((1 - lagAfter / lagTotal) * 100)}%`, "tool-call") });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(Math.round(lagTotal * 0.35)), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      toast(dispatch, `✅ ${topicName} recovering — lag draining`, "success");
    });

    t(11500, () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "writer", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("writer", `Change record drafted: degraded→healthy recovery on ${topicName}. Lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}.`, "consume") });
      particle(dispatch, "e-aud", "writer", "notification");
    });

    t(13500, () => {
      agents = patch(agents, "writer", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "notification", { status: "acting", mralPhase: "act" });
      dispatch({ type: "state", payload: { agents, mralPhase: "act", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("notification", `Slack #kafka-ops: ${topicName} healed. Consumer scale-out successful. ITSM ticket auto-closed.`, "notification") });
      dispatch({ type: "notification", record: { id: uid(), ts: Date.now(), channel: "slack", title: "Topic Healed", message: `✅ ${topicName} healthy — lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}`, scenarioId: "topic-heal" } });
      toast(dispatch, `✅ ${topicName} healed — lag ${lagAfter.toLocaleString()}`, "success");
    });

    t(15500, () => {
      agents = patch(agents, "notification", { status: "online", mralPhase: "idle" });
      agents = patch(agents, "monitor", { status: "reasoning", mralPhase: "learn" });
      dispatch({ type: "state", payload: { agents, mralPhase: "learn", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: true } });
      dispatch({ type: "audit", record: auditRec("monitor", `Lesson: Degraded ${topicName} recovered via consumer scale-out. Adjust auto-scale policy: lag>2,800 triggers +${Math.max(1, Math.floor(partitions / 4))} consumer.`, "lesson") });
      dispatch({ type: "lesson", record: { id: uid(), ts: Date.now(), scenarioId: "topic-heal", actionTaken: `Consumer scale +${Math.max(1, Math.floor(partitions / 4))} on ${topicName}`, notes: `Auto-scale policy: lag>2,800 → +1 consumer replica on ${topicName}.` } });
    });

    t(17500, () => {
      agents = patch(agents, "monitor", { status: "online", mralPhase: "idle" });
      dispatch({ type: "state", payload: { agents, mralPhase: "idle", broker: mockBroker(lagAfter), pendingApprovals: [], incidentQueueDepth: 0, scenarioRunning: false } });
      sendEmail(dispatch, "topic-heal", `Topic Heal — ${topicName}`, lagTotal, lagAfter, healAction, {
        approved: true, approvedBy: "auto",
        reasoning: { rootCause, kafkaFeature: "Consumer Groups", confidence: 0.87, rationale },
        lesson: { notes: `Degraded ${topicName} healed via scale-out. Set auto-scale: lag>2,800 → +1 replica.`, adjustedThreshold: 2800 },
        slackMessage: `✅ ${topicName} healthy — lag ${lagTotal.toLocaleString()} → ${lagAfter.toLocaleString()}`,
        itsmTicket: `INC-${Date.now().toString().slice(-5)} — Degraded Topic Heal`,
      });
    });
  }

  return () => allTimers.forEach(clearTimeout);
}

// ── Public API ────────────────────────────────────────────────────────────────

// Keys must match the `id` values in the SCENARIOS array in Dashboard.tsx
export type ScenarioKey = string;

export function runClientScenario(key: ScenarioKey, dispatch: DispatchFn): () => void {
  switch (key) {
    case "lag-spike":                  return runLagSpike(dispatch);
    case "controller-failover":        return runControllerFailover(dispatch);
    case "share-group":
    case "share-group-rebalance":      return runShareGroupRebalance(dispatch);
    case "benign-rebalance":
    case "partition-imbalance":        return runBenignRebalance(dispatch);
    // Extra scenarios
    case "schema-mismatch":            return runSchemaMismatch(dispatch);
    case "disk-saturation":            return runDiskSaturation(dispatch);
    case "under-replication":          return runUnderReplication(dispatch);
    case "producer-timeout":           return runProducerTimeout(dispatch);
    case "consumer-session-timeout":   return runConsumerSessionTimeout(dispatch);
    case "compaction-lag":             return runCompactionLag(dispatch);
    default:
      console.warn("[client-sim] unknown scenario key:", key);
      return () => {};
  }
}
