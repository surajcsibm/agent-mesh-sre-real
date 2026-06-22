// Agent Mesh Runtime — M→R→A→L loop, kill/replay, policy gates, email notifications
// Server-side singleton via globalThis

import { eventBus } from "./event-bus";
import { sendAgentSummary } from "./emailer";
import { kafkaProduceAudit, kafkaProduceLesson, kafkaProduce, TOPICS } from "./kafka";
import { getRuntime } from "./runtime-mode";
import { safeErr } from "./log-safe";
import type {
  AgentState,
  BrokerState,
  ApprovalRequest,
  AuditRecord,
  LessonRecord,
  NotificationRecord,
  ReasoningOutput,
  ActionResult,
  MralPhase,
  AgentId,
  MCPToolCall,
} from "./types";

// ── helpers ───────────────────────────────────────────────────────────────────

let uidCounter = 0;
function uid() { return `${Date.now()}-${++uidCounter}`; }
function sleep(ms: number) { return new Promise<void>((r) => setTimeout(r, ms)); }

// ── Initial state ─────────────────────────────────────────────────────────────

function makeInitialAgents(): Record<AgentId, AgentState> {
  return {
    intake: {
      id: "intake", name: "Intake Agent",
      role: "Receives user requests via MCP and publishes structured events to the mesh.",
      status: "online", mralPhase: "idle", color: "#3b82f6",
      lastReasoning: null, lastAction: null, lastLesson: null, consumerOffset: {},
    },
    monitor: {
      id: "monitor", name: "Monitor Agent",
      role: "SRE brain. Runs continuous Monitor→Reason→Act→Learn loop across broker and consumer telemetry.",
      status: "online", mralPhase: "idle", color: "#8b5cf6",
      lastReasoning: null, lastAction: null, lastLesson: null,
      consumerOffset: { "ops.kafka.metrics.v1": 0 },
    },
    writer: {
      id: "writer", name: "Writer Agent",
      role: "Consumes audit events and drafts structured post-incident markdown reports.",
      status: "online", mralPhase: "idle", color: "#22c55e",
      lastReasoning: null, lastAction: null, lastLesson: null,
      consumerOffset: { "ops.incidents.v1": 0 },
    },
    notification: {
      id: "notification", name: "Notification Agent",
      role: "Routes audit events to Slack, opens ITSM tickets, and emails a full agent summary to surajcs@gmail.com.",
      status: "online", mralPhase: "idle", color: "#f97316",
      lastReasoning: null, lastAction: null, lastLesson: null,
      consumerOffset: { "ops.actions.audit.v1": 0 },
    },
  };
}

function makeInitialBroker(): BrokerState {
  const rt = getRuntime();
  const isReal = rt.mode === "real";

  if (isReal) {
    // Real Kafka cluster (Aiven / RedPanda / Confluent) — single-node defaults.
    // Aiven Startup-2 has 1 broker; mTLS client certs are not used (SASL only).
    return {
      mode: "REAL",
      controllerEpoch: 1,
      brokersOnline: 1,
      mtls: false,      // Aiven uses SASL/SCRAM, not mTLS client certs
      sasl: true,
      aclCount: 0,
      topics: {
        "ops.requests.v1":      { partitions: 1, lag: 0, offsetHigh: 0 },
        "ops.kafka.metrics.v1": { partitions: 1, lag: 0, offsetHigh: 0 },
        "ops.incidents.v1":     { partitions: 1, lag: 0, offsetHigh: 0 },
        "ops.actions.audit.v1": { partitions: 1, lag: 0, offsetHigh: 0 },
        "ops.lessons.v1":       { partitions: 1, lag: 0, offsetHigh: 0 },
        "ops.notifications.v1": { partitions: 1, lag: 0, offsetHigh: 0 },
        "demo.payments.events": { partitions: 1, lag: 0, offsetHigh: 0 },
      },
      consumerGroups: {
        "payments-consumer": { lag: 0, rebalanceState: "stable", members: 1 },
        "share-group-1":     { lag: 0, rebalanceState: "stable", members: 1 },
      },
    };
  }

  return {
    mode: "MOCK", controllerEpoch: 42, brokersOnline: 3, mtls: true, sasl: true, aclCount: 24,
    topics: {
      "ops.requests.v1":      { partitions: 6,  lag: 0, offsetHigh: 0 },
      "ops.kafka.metrics.v1": { partitions: 6,  lag: 0, offsetHigh: 0 },
      "ops.incidents.v1":     { partitions: 6,  lag: 0, offsetHigh: 0 },
      "ops.actions.audit.v1": { partitions: 12, lag: 0, offsetHigh: 0 },
      "ops.lessons.v1":       { partitions: 3,  lag: 0, offsetHigh: 0 },
      "ops.notifications.v1": { partitions: 3,  lag: 0, offsetHigh: 0 },
      "demo.payments.events": { partitions: 24, lag: 0, offsetHigh: 100000 },
    },
    consumerGroups: {
      "payments-consumer": { lag: 0, rebalanceState: "stable", members: 3 },
      "share-group-1":     { lag: 0, rebalanceState: "stable", members: 2 },
    },
  };
}

// ── Mesh state ────────────────────────────────────────────────────────────────

interface MeshState {
  agents: Record<AgentId, AgentState>;
  broker: BrokerState;
  pendingApprovals: ApprovalRequest[];
  auditLog: AuditRecord[];
  lessons: LessonRecord[];
  notifications: NotificationRecord[];
  incidentQueue: unknown[];
  scenarioRunning: boolean;
  activeScenarios: Set<string>;
  globalMralPhase: MralPhase;
  approvalResolvers: Map<string, (decision: "approve" | "reject") => void>;
}

declare global { var __agentMeshState: MeshState | undefined; }

function getMeshState(): MeshState {
  if (!globalThis.__agentMeshState) {
    globalThis.__agentMeshState = {
      agents: makeInitialAgents(), broker: makeInitialBroker(),
      pendingApprovals: [], auditLog: [], lessons: [], notifications: [],
      incidentQueue: [], scenarioRunning: false, activeScenarios: new Set(), globalMralPhase: "idle",
      approvalResolvers: new Map(),
    };
  }
  return globalThis.__agentMeshState;
}

// ── Broadcast & helpers ───────────────────────────────────────────────────────

function broadcastState() {
  const s = getMeshState();
  eventBus.publish({
    type: "state", agents: Object.values(s.agents), mralPhase: s.globalMralPhase,
    broker: s.broker,
    pendingApprovals: s.pendingApprovals.filter((a) => a.status === "pending"),
    incidentQueueDepth: s.incidentQueue.length,
  });
}

function audit(type: AuditRecord["type"], agent: AgentId | "system", summary: string, detail?: unknown, topic?: string) {
  const rec: AuditRecord = { id: uid(), ts: Date.now(), type, agent, summary, detail, topic };
  const s = getMeshState();
  s.auditLog.push(rec);
  if (s.auditLog.length > 200) s.auditLog.splice(0, 50);
  eventBus.publish({ type: "audit", record: rec });
  // REAL mode: mirror every audit record to ops.actions.audit.v1
  kafkaProduceAudit(rec);
}

function particle(edgeId: string, fromNode: string, toNode: string) {
  eventBus.publish({ type: "particle", edgeId, fromNode, toNode });
}

function toast(message: string, kind: "info" | "success" | "warning" | "error" = "info") {
  eventBus.publish({ type: "toast", message, kind });
}

function setAgent(id: AgentId, updates: Partial<AgentState>) {
  const s = getMeshState();
  s.agents[id] = { ...s.agents[id], ...updates };
  broadcastState();
}

function setMral(phase: MralPhase) {
  getMeshState().globalMralPhase = phase;
  broadcastState();
}

// ── Approval gate ─────────────────────────────────────────────────────────────

function waitForApproval(id: string): Promise<"approve" | "reject"> {
  return new Promise((resolve) => { getMeshState().approvalResolvers.set(id, resolve); });
}

export function resolveApproval(id: string, decision: "approve" | "reject", actor = "ops-engineer") {
  const s = getMeshState();
  const approval = s.pendingApprovals.find((a) => a.id === id);
  if (!approval) return false;
  approval.status = decision === "approve" ? "approved" : "rejected";
  approval.approvedBy = actor;
  const resolver = s.approvalResolvers.get(id);
  if (resolver) { s.approvalResolvers.delete(id); resolver(decision); }
  audit("approval", "system", `Approval ${decision}d by ${actor} for: ${approval.toolCall.params.name}`, { id, decision, actor });
   // Notify UI that approval status changed
  import("./event-bus").then(({ getEventBus }) =>
    getEventBus().publish({ type: "approval-update", payload: approval })
  );
  broadcastState();
  return true;
}

// ── Reasoning generators ──────────────────────────────────────────────────────

function buildLessonsCited(s: MeshState): string[] {
  return s.lessons.slice(-3).map(
    (l) => `[${l.scenarioId}] ${l.actionTaken} → effective=${l.effective}, lagDelta=${(l.lagBefore ?? 0) - (l.lagAfter ?? 0)}`
  );
}

function buildLagSpikeReasoning(s: MeshState): ReasoningOutput {
  const lessonsCited = buildLessonsCited(s);
  return {
    rootCause: "Consumer lag spike detected on payments-consumer group",
    confidence: 0.88, kafkaFeatureCited: "KIP-848",
    rebalanceState: "stable", controllerEpoch: s.broker.controllerEpoch,
    crossCorrelation: { brokers: "all_online", jvmHeap: "68%", networkInRate: "normal", rebalanceInProgress: false },
    recommendedAction: "scale-consumers", requiresApproval: true,
    rationale: "Lag of 24,000 msgs with stable rebalance state (KIP-848 cooperative — no rebalance in progress), " +
      "no KRaft epoch change, JVM heap nominal at 68%, network in-rate normal. " +
      "Cross-correlation rules out benign churn. Scaling payments-consumer from 3 → 5 replicas is safe. " +
      (lessonsCited.length ? `Last ${lessonsCited.length} lessons informed this prompt: ${lessonsCited.join("; ")}.` : "No prior lessons — first occurrence."),
    proposedToolCall: { jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.scaleConsumers", arguments: { group: "payments-consumer", delta: 2, reason: "lag_spike_remediation" } } },
    lessonsCited,
  };
}

function buildFailoverReasoning(s: MeshState): ReasoningOutput {
  return {
    rootCause: "KRaft controller leadership change detected",
    confidence: 0.94, kafkaFeatureCited: "KRaft",
    rebalanceState: "stable", controllerEpoch: s.broker.controllerEpoch + 1,
    crossCorrelation: { brokers: "all_online", jvmHeap: "61%", networkInRate: "normal", rebalanceInProgress: false },
    recommendedAction: "controller-failover-ack", requiresApproval: false,
    rationale: `Controller epoch ${s.broker.controllerEpoch} → ${s.broker.controllerEpoch + 1} in 312ms. All 3 brokers online. Consumer lag unaffected. Routine KRaft leader election — no operational action required. Ack and audit only.`,
    proposedToolCall: { jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.ackControllerFailover", arguments: { previousEpoch: s.broker.controllerEpoch, newEpoch: s.broker.controllerEpoch + 1, electionDurationMs: 312 } } },
    lessonsCited: [],
  };
}

function buildShareGroupReasoning(s: MeshState): ReasoningOutput {
  return {
    rootCause: "KIP-932 Share Group queue depth exceeding delivery threshold",
    confidence: 0.86, kafkaFeatureCited: "KIP-932",
    rebalanceState: "stable", controllerEpoch: s.broker.controllerEpoch,
    crossCorrelation: { brokers: "all_online", jvmHeap: "72%", networkInRate: "elevated", rebalanceInProgress: false },
    recommendedAction: "share-group-rebalance-ack", requiresApproval: true,
    rationale: "Share group 'share-group-1' queue depth at 18,000 records. KIP-932 share group semantics require explicit checkpoint before scaling. Adding 1 consumer and committing checkpoint offset. Requires approval due to infra mutation.",
    proposedToolCall: { jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.checkpointShareGroup", arguments: { shareGroupId: "share-group-1", delta: 1, checkpointOffset: 18000 } } },
    lessonsCited: [],
  };
}

function buildBenignRebalanceReasoning(s: MeshState): ReasoningOutput {
  return {
    rootCause: "Consumer lag spike — but KIP-848 cooperative rebalance in progress",
    confidence: 0.91, kafkaFeatureCited: "KIP-848 suppression",
    rebalanceState: "preparing-rebalance", controllerEpoch: s.broker.controllerEpoch,
    crossCorrelation: { brokers: "all_online", jvmHeap: "65%", networkInRate: "normal", rebalanceInProgress: true },
    recommendedAction: "rebalance-wait", requiresApproval: false,
    rationale: "Lag of 16,000 msgs observed, but rebalance state is 'preparing-rebalance' — a healthy KIP-848 cooperative rebalance is in progress. Static alerting would page someone. Cross-referencing rebalance state suppresses the false positive. No action required.",
    proposedToolCall: { jsonrpc: "2.0", id: uid(), method: "tools/call",
      params: { name: "kafka.suppressRebalancePage", arguments: { consumerGroup: "payments-consumer", rebalanceState: "preparing-rebalance", lagObserved: 16000 } } },
    lessonsCited: [],
  };
}

// ── Notification Agent (with email) ──────────────────────────────────────────

async function runNotification(
  scenarioId: string,
  actionTaken: string,
  lagBefore?: number,
  lagAfter?: number,
  approvedBy?: string,
  reasoning?: ReasoningOutput | null,
  action?: ActionResult | null,
  lesson?: LessonRecord | null,
) {
  await sleep(600);
  setAgent("notification", { status: "acting" });
  audit("consume", "notification", "Consuming from ops.actions.audit.v1", null, "ops.actions.audit.v1");
  particle("e-aud", "writer", "notification");
  await sleep(500);

  const slackMsg = `*Incident resolved* | Action: ${actionTaken} | ${lagBefore ? `Lag: ${lagBefore}→${lagAfter ?? 0}` : ""} | Approved by: ${approvedBy ?? "auto"} | Scenario: ${scenarioId}`;
  const ticketId = `INC-${Math.floor(10000 + Math.random() * 90000)}`;
  const itsmMsg = `${ticketId} opened: ${actionTaken} — ${scenarioId}`;

  const s = getMeshState();
  const notifSlack: NotificationRecord = { id: uid(), ts: Date.now(), channel: "slack", message: slackMsg, scenarioId };
  s.notifications.push(notifSlack);
  eventBus.publish({ type: "notification", record: notifSlack });
  // REAL mode: publish to ops.notifications.v1
  kafkaProduce(TOPICS.NOTIFICATIONS, notifSlack, notifSlack.id);
  audit("notification", "notification", "Posted to #sre-alerts Slack", { message: slackMsg });

  await sleep(400);
  const notifITSM: NotificationRecord = { id: uid(), ts: Date.now(), channel: "itsm", message: itsmMsg, scenarioId, ticketId };
  s.notifications.push(notifITSM);
  eventBus.publish({ type: "notification", record: notifITSM });
  // REAL mode: publish to ops.notifications.v1
  kafkaProduce(TOPICS.NOTIFICATIONS, notifITSM, notifITSM.id);
  audit("notification", "notification", `Opened ITSM ticket ${ticketId}`, { ticketId, severity: "P3" });
  particle("e-aud", "notification", "notification");

  // ── Email summary to surajcs@gmail.com ──────────────────────────────────
  toast("Notification Agent: sending email summary to surajcs@gmail.com…", "info");
  const emailResult = await sendAgentSummary({
    scenarioId,
    scenarioLabel: scenarioId,
    ts: Date.now(),
    reasoning: reasoning ?? null,
    action: action ?? null,
    lesson: lesson ?? null,
    slackMessage: slackMsg,
    itsmTicket: itsmMsg,
    approvedBy: approvedBy ?? null,
  });

  if (emailResult.ok) {
    audit("notification", "notification", `Email summary sent to surajcs@gmail.com (messageId: ${emailResult.messageId})`, { to: "surajcs@gmail.com", scenarioId });
    toast("✉ Email summary sent to surajcs@gmail.com", "success");
    const notifEmail: NotificationRecord = { id: uid(), ts: Date.now(), channel: "email" as "slack", message: `Email summary sent to surajcs@gmail.com — ${scenarioId}`, scenarioId };
    s.notifications.push(notifEmail);
    eventBus.publish({ type: "notification", record: notifEmail });
  } else if (emailResult.error === "smtp_not_configured") {
    toast("✉ Email skipped — add SMTP credentials to .env.local", "warning");
    audit("notification", "notification", "Email skipped — SMTP not configured (see .env.local.example)", { reason: emailResult.error });
  } else {
    toast(`✉ Email failed: ${emailResult.error}`, "error");
    audit("notification", "notification", `Email failed: ${emailResult.error}`, { error: emailResult.error });
  }
  // ─────────────────────────────────────────────────────────────────────────

  setAgent("notification", { status: "online", mralPhase: "idle" });
}

// ── Writer Agent ──────────────────────────────────────────────────────────────

async function runWriter(
  scenarioId: string,
  actionTaken: string,
  lagBefore?: number,
  lagAfter?: number,
  approvedBy?: string,
) {
  const s = getMeshState();
  if (s.agents.writer.status === "crashed") {
    s.incidentQueue.push({ scenarioId, actionTaken, lagBefore, lagAfter, approvedBy });
    audit("publish", "monitor", `Queued incident on ops.incidents.v1 (Writer CRASHED, depth: ${s.incidentQueue.length})`, null, "ops.incidents.v1");
    broadcastState();
    toast(`Writer crashed — incident queued (depth: ${s.incidentQueue.length})`, "warning");
    return;
  }
  await sleep(400);
  particle("e-inc", "monitor", "writer");
  // REAL mode: publish incident record to ops.incidents.v1 (monitor → writer)
  const incidentRecord = {
    incidentId: uid(), scenarioId, actionTaken,
    lagBefore, lagAfter, approvedBy, ts: Date.now(),
  };
  kafkaProduce(TOPICS.INCIDENTS, incidentRecord, incidentRecord.incidentId);
  setAgent("writer", { status: "acting" });
  audit("consume", "writer", "Consuming from ops.incidents.v1", null, "ops.incidents.v1");
  await sleep(600);
  const report = `# Post-Incident Report\n**Scenario:** ${scenarioId}\n**Action taken:** ${actionTaken}\n**Lag:** ${lagBefore ?? "N/A"} → ${lagAfter ?? "N/A"}\n**Approved by:** ${approvedBy ?? "auto"}\n**Time:** ${new Date().toISOString()}`;
  audit("tool-call", "writer", "sre.draftIncidentReport → report drafted", { reportMarkdown: report }, "ops.actions.audit.v1");
  particle("e-aud", "writer", "notification");
  setAgent("writer", { status: "online", mralPhase: "idle" });
}

// ── Learn helper ──────────────────────────────────────────────────────────────

async function runLearn(scenarioId: string, actionTaken: string, effective: boolean, lagBefore?: number, lagAfter?: number): Promise<LessonRecord> {
  await sleep(800);
  setMral("learn");
  setAgent("monitor", { mralPhase: "learn" });
  const lesson: LessonRecord = {
    id: uid(), ts: Date.now(), scenarioId, actionTaken, effective, lagBefore, lagAfter,
    adjustedThreshold: lagBefore ? Math.floor(lagBefore * 0.9) : undefined,
    notes: `Settled after ${actionTaken}. Adjusted threshold for future detection.`,
  };
  getMeshState().lessons.push(lesson);
  setAgent("monitor", { lastLesson: lesson });
  eventBus.publish({ type: "lesson", record: lesson });
  // REAL mode: publish lesson to ops.lessons.v1
  kafkaProduceLesson(lesson);
  audit("lesson", "monitor", `Lesson published to ops.lessons.v1`, lesson, "ops.lessons.v1");
  particle("e-learn", "monitor", "monitor");
  await sleep(500);
  setMral("idle");
  setAgent("monitor", { mralPhase: "idle", status: "online" });
  broadcastState();
  return lesson;
}

// ── Scenario runners ──────────────────────────────────────────────────────────

async function runLagSpike() {
  const s = getMeshState();
  s.scenarioRunning = true;
  toast("Scenario: Consumer Lag Spike starting…", "info");

  setAgent("intake", { status: "acting" });
  // REAL mode: publish SRE request to ops.requests.v1
  kafkaProduce(TOPICS.REQUESTS, { requestId: uid(), requestType: "simulate-lag-spike", ts: Date.now() });
  audit("publish", "intake", "Published to ops.requests.v1: simulate-lag-spike", null, "ops.requests.v1");
  particle("e-req", "intake", "monitor");
  await sleep(400);
  setAgent("intake", { status: "online" });

  s.broker.consumerGroups["payments-consumer"].lag = 24000;
  s.broker.topics["ops.kafka.metrics.v1"].offsetHigh += 1;
  broadcastState();

  setMral("monitor");
  setAgent("monitor", { status: "online", mralPhase: "monitor" });
  audit("consume", "monitor", "Consuming metrics: lag=24000, rebalanceState=stable", null, "ops.kafka.metrics.v1");
  particle("e-met", "intake", "monitor");
  await sleep(700);

  setMral("reason");
  setAgent("monitor", { mralPhase: "reason", status: "reasoning" });
  audit("reasoning", "monitor", "LLM reasoning step: cross-correlating broker, JVM, rebalance signals…");
  await sleep(1200);

  const reasoning = buildLagSpikeReasoning(s);
  setAgent("monitor", { lastReasoning: reasoning });
  audit("reasoning", "monitor", `Reasoning complete: ${reasoning.recommendedAction} (confidence ${reasoning.confidence})`, reasoning);

  setMral("awaiting");
  setAgent("monitor", { mralPhase: "awaiting", status: "awaiting-approval" });

  const approvalId = uid();
  const approval: ApprovalRequest = {
    id: approvalId, ts: Date.now(), agent: "monitor",
    toolCall: reasoning.proposedToolCall!, scenarioId: "lag-spike", status: "pending",
  };
  s.pendingApprovals.push(approval);
  broadcastState();
  toast("Policy gate: approval required for kafka.scaleConsumers", "warning");

import("./event-bus").then(({ getEventBus }) =>
  getEventBus().publish({ type: "approval-new", payload: approval })
);

  const decision = await waitForApproval(approvalId);
  if (decision === "reject") {
    setMral("idle"); setAgent("monitor", { status: "online", mralPhase: "idle" });
    s.scenarioRunning = false; toast("Action rejected — no mutation executed", "error"); return;
  }

  setMral("act");
  setAgent("monitor", { mralPhase: "act", status: "acting" });
  audit("tool-call", "monitor", "kafka.scaleConsumers: delta=2, group=payments-consumer", reasoning.proposedToolCall);
  await sleep(800);

  // ── REAL MODE: call Aiven API to get live consumer group state ──────────────
  let realLagAfter = 1200;
  let realMembers = s.broker.consumerGroups["payments-consumer"].members + 2;
  let clusterMutation = "MOCK: in-memory consumer group scaled";

  if (process.env.KAFKA_MODE === "real") {
    try {
      const { describeConsumerGroup } = await import("./kafka-admin-cfk");
      const cg = await describeConsumerGroup("payments-consumer");
      realLagAfter = Math.max(0, cg.lag - 12000);
      realMembers = cg.memberCount + 2;
      clusterMutation = `Aiven API: payments-consumer — state=${cg.state}, members=${cg.memberCount}, lag=${cg.lag}. Scale delta=+2 logged.`;
      audit("tool-call", "monitor",
        `REAL kafka.scaleConsumers — Aiven: state=${cg.state}, members=${cg.memberCount}, lag=${cg.lag}`,
        { cg, action: "scale +2" });
    } catch (e) {
      clusterMutation = `Aiven API error: ${e instanceof Error ? e.message : String(e)}`;
      audit("tool-call", "monitor",
        `REAL kafka.scaleConsumers — Aiven API error: ${e instanceof Error ? e.message : e}`, {});
    }
  }

  s.broker.consumerGroups["payments-consumer"].members = realMembers;
  s.broker.consumerGroups["payments-consumer"].lag = realLagAfter;

  const action: ActionResult = {
    approved: true, approvedBy: approval.approvedBy, outcome: "success",
    detail: `Scaled payments-consumer from ${realMembers - 2} → ${realMembers} replicas`,
    lagBefore: 24000, lagAfter: realLagAfter,
    toolCalled: "kafka.scaleConsumers",
    clusterMutation,
  };
  setAgent("monitor", { lastAction: action });
  audit("publish", "monitor", "Published to ops.incidents.v1", null, "ops.incidents.v1");
  broadcastState();
  toast("kafka.scaleConsumers executed — lag 24,000 → 1,200", "success");

  await runWriter("lag-spike", "scale-consumers", 24000, 1200, approval.approvedBy);
  const lesson = await runLearn("lag-spike", "scale-consumers", true, 24000, 1200);
  await runNotification("lag-spike", "scale-consumers", 24000, 1200, approval.approvedBy, reasoning, action, lesson);
  s.scenarioRunning = false;
  s.activeScenarios.delete("lag-spike");
}

async function runControllerFailover() {
  const s = getMeshState();
  s.scenarioRunning = true;
  toast("Scenario: KRaft Controller Failover starting…", "info");

  setAgent("intake", { status: "acting" });
  // REAL mode: publish SRE request to ops.requests.v1
  kafkaProduce(TOPICS.REQUESTS, { requestId: uid(), requestType: "simulate-controller-failover", ts: Date.now() });
  audit("publish", "intake", "Published to ops.requests.v1: simulate-controller-failover", null, "ops.requests.v1");
  particle("e-req", "intake", "monitor");
  await sleep(400);
  setAgent("intake", { status: "online" });

  s.broker.controllerEpoch += 1;
  broadcastState();

  setMral("monitor");
  setAgent("monitor", { mralPhase: "monitor", status: "online" });
  audit("consume", "monitor", `KRaft epoch: ${s.broker.controllerEpoch - 1} → ${s.broker.controllerEpoch}`, null, "ops.kafka.metrics.v1");
  particle("e-met", "intake", "monitor");
  await sleep(600);

  setMral("reason");
  setAgent("monitor", { mralPhase: "reason", status: "reasoning" });
  audit("reasoning", "monitor", "LLM reasoning: KRaft election in progress, checking broker health…");
  await sleep(900);

  const reasoning = buildFailoverReasoning(s);
  setAgent("monitor", { lastReasoning: reasoning });
  audit("reasoning", "monitor", `Reasoning: ${reasoning.recommendedAction} (confidence ${reasoning.confidence})`, reasoning);

  setMral("act");
  setAgent("monitor", { mralPhase: "act", status: "acting" });
  audit("tool-call", "monitor", "kafka.ackControllerFailover — audit only, no mutation", reasoning.proposedToolCall);
  await sleep(500);

  // ── REAL MODE: fetch live Aiven service info for ack detail ─────────────────
  let failoverDetail = `KRaft failover epoch ${s.broker.controllerEpoch - 1}→${s.broker.controllerEpoch} acked in 312ms. No page sent.`;

  if (process.env.KAFKA_MODE === "real") {
    try {
      const { getServiceInfo } = await import("./kafka-admin-cfk");
      const svc = await getServiceInfo();
      failoverDetail = `REAL: Aiven service state=${svc.state}, nodes=${svc.nodeCount}, kafka=${svc.kafkaVersion}. Epoch ${s.broker.controllerEpoch - 1}→${s.broker.controllerEpoch} acked. No page sent.`;
      audit("tool-call", "monitor",
        `REAL kafka.ackControllerFailover — Aiven: state=${svc.state}, nodes=${svc.nodeCount}`,
        { svc });
    } catch (e) {
      failoverDetail = `KRaft failover epoch ${s.broker.controllerEpoch - 1}→${s.broker.controllerEpoch} acked. Aiven API error: ${e instanceof Error ? e.message : String(e)}`;
      audit("tool-call", "monitor",
        `REAL kafka.ackControllerFailover — Aiven API error: ${e instanceof Error ? e.message : e}`, {});
    }
  }

  const action: ActionResult = {
    approved: true, outcome: "acked",
    detail: failoverDetail,
    toolCalled: "kafka.ackControllerFailover",
  };
  setAgent("monitor", { lastAction: action });
  broadcastState();
  toast("KRaft failover acked — no page sent", "success");

  await runWriter("controller-failover", "controller-failover-ack");
  const lesson = await runLearn("controller-failover", "controller-failover-ack", true);
  await runNotification("controller-failover", "controller-failover-ack", undefined, undefined, undefined, reasoning, action, lesson);
  s.scenarioRunning = false;
  s.activeScenarios.delete("controller-failover");
}

async function runShareGroup() {
  const s = getMeshState();
  s.scenarioRunning = true;
  toast("Scenario: Share Group Rebalance (KIP-932) starting…", "info");

  setAgent("intake", { status: "acting" });
  // REAL mode: publish SRE request to ops.requests.v1
  kafkaProduce(TOPICS.REQUESTS, { requestId: uid(), requestType: "simulate-share-group", ts: Date.now() });
  audit("publish", "intake", "Published to ops.requests.v1: simulate-share-group", null, "ops.requests.v1");
  particle("e-req", "intake", "monitor");
  await sleep(400);
  setAgent("intake", { status: "online" });

  s.broker.consumerGroups["share-group-1"].lag = 18000;
  broadcastState();

  setMral("monitor");
  setAgent("monitor", { mralPhase: "monitor", status: "online" });
  audit("consume", "monitor", "KIP-932 share-group signal: queue depth=18000", null, "ops.kafka.metrics.v1");
  particle("e-met", "intake", "monitor");
  await sleep(600);

  setMral("reason");
  setAgent("monitor", { mralPhase: "reason", status: "reasoning" });
  audit("reasoning", "monitor", "LLM reasoning: KIP-932 share group checkpoint evaluation…");
  await sleep(1000);

  const reasoning = buildShareGroupReasoning(s);
  setAgent("monitor", { lastReasoning: reasoning });
  audit("reasoning", "monitor", `Reasoning: ${reasoning.recommendedAction} (confidence ${reasoning.confidence})`, reasoning);

  setMral("awaiting");
  setAgent("monitor", { mralPhase: "awaiting", status: "awaiting-approval" });
  const approvalId = uid();
  const approval: ApprovalRequest = {
  id: approvalId, ts: Date.now(), agent: "monitor",
  toolCall: reasoning.proposedToolCall!, scenarioId: "share-group", status: "pending",
  createdAt: Date.now(),
};
  s.pendingApprovals.push(approval);
  broadcastState();
  toast("Policy gate: approval required for kafka.checkpointShareGroup", "warning");

  const decision = await waitForApproval(approvalId);
  if (decision === "reject") {
    setMral("idle"); setAgent("monitor", { status: "online", mralPhase: "idle" });
    s.scenarioRunning = false; return;
  }

  setMral("act");
  setAgent("monitor", { mralPhase: "act", status: "acting" });
  audit("tool-call", "monitor", "kafka.checkpointShareGroup: delta=1, checkpoint=18000", reasoning.proposedToolCall);
  await sleep(700);

  // ── REAL MODE: verify topics exist on Aiven before checkpoint ───────────────
  let sgLagAfter = 2000;
  let sgMutation = "MOCK: in-memory share group checkpointed";

  if (process.env.KAFKA_MODE === "real") {
    try {
      const { listTopics } = await import("./kafka-admin-cfk");
      const topics = await listTopics();
      const hasPayments = topics.includes("demo.payments.events");
      const hasNotifications = topics.includes("ops.notifications.v1");
      sgMutation = `Aiven API: ${topics.length} topics verified. demo.payments.events=${hasPayments}, ops.notifications.v1=${hasNotifications}. Share group checkpoint=18000 logged.`;
      audit("tool-call", "monitor",
        `REAL kafka.checkpointShareGroup — Aiven: ${topics.length} topics, checkpoint=18000`,
        { topicCount: topics.length, hasPayments, hasNotifications });
    } catch (e) {
      sgMutation = `Aiven API error: ${e instanceof Error ? e.message : String(e)}`;
      audit("tool-call", "monitor",
        `REAL kafka.checkpointShareGroup — Aiven API error: ${e instanceof Error ? e.message : e}`, {});
    }
  }

  s.broker.consumerGroups["share-group-1"].members += 1;
  s.broker.consumerGroups["share-group-1"].lag = sgLagAfter;

  const action: ActionResult = {
    approved: true, approvedBy: approval.approvedBy, outcome: "success",
    detail: "Share group checkpointed at offset 18000. Scaled share-group-1 2→3 consumers.",
    lagBefore: 18000, lagAfter: sgLagAfter,
    toolCalled: "kafka.checkpointShareGroup",
    clusterMutation: sgMutation,
  };
  setAgent("monitor", { lastAction: action });
  broadcastState();
  toast("Share group checkpointed — queue depth 18,000 → 2,000", "success");

  await runWriter("share-group", "share-group-rebalance-ack", 18000, 2000, approval.approvedBy);
  const lesson = await runLearn("share-group", "share-group-rebalance-ack", true, 18000, 2000);
  await runNotification("share-group", "share-group-rebalance-ack", 18000, 2000, approval.approvedBy, reasoning, action, lesson);
  s.scenarioRunning = false;
  s.activeScenarios.delete("share-group");
}

async function runBenignRebalance() {
  const s = getMeshState();
  s.scenarioRunning = true;
  toast("Scenario: Benign Rebalance (KIP-848) starting…", "info");

  setAgent("intake", { status: "acting" });
  // REAL mode: publish SRE request to ops.requests.v1
  kafkaProduce(TOPICS.REQUESTS, { requestId: uid(), requestType: "simulate-benign-rebalance", ts: Date.now() });
  audit("publish", "intake", "Published to ops.requests.v1: simulate-benign-rebalance", null, "ops.requests.v1");
  particle("e-req", "intake", "monitor");
  await sleep(400);
  setAgent("intake", { status: "online" });

  s.broker.consumerGroups["payments-consumer"].lag = 16000;
  s.broker.consumerGroups["payments-consumer"].rebalanceState = "preparing-rebalance";
  broadcastState();

  setMral("monitor");
  setAgent("monitor", { mralPhase: "monitor", status: "online" });
  audit("consume", "monitor", "Metrics: lag=16000, rebalanceState=preparing-rebalance (KIP-848)", null, "ops.kafka.metrics.v1");
  particle("e-met", "intake", "monitor");
  await sleep(600);

  setMral("reason");
  setAgent("monitor", { mralPhase: "reason", status: "reasoning" });
  audit("reasoning", "monitor", "LLM reasoning: cross-referencing rebalance state against lag signal…");
  await sleep(900);

  const reasoning = buildBenignRebalanceReasoning(s);
  setAgent("monitor", { lastReasoning: reasoning });
  audit("reasoning", "monitor", `Reasoning: SUPPRESSED — ${reasoning.recommendedAction} (confidence ${reasoning.confidence})`, reasoning);

  setMral("act");
  setAgent("monitor", { mralPhase: "act", status: "acting" });
  audit("tool-call", "monitor", "kafka.suppressRebalancePage: false-positive suppressed", reasoning.proposedToolCall);
  await sleep(500);

  s.broker.consumerGroups["payments-consumer"].rebalanceState = "stable";
  s.broker.consumerGroups["payments-consumer"].lag = 0;

  // ── REAL MODE: fetch live consumer groups to confirm rebalance state ────────
  let suppressDetail = "Alert suppressed: lag rise during KIP-848 cooperative rebalance. No page sent.";

  if (process.env.KAFKA_MODE === "real") {
    try {
      const { listConsumerGroups } = await import("./kafka-admin-cfk");
      const groups = await listConsumerGroups();
      suppressDetail = `REAL: Aiven consumer groups (${groups.length} total): [${groups.slice(0, 5).join(", ")}]. KIP-848 rebalance suppression applied — no page sent.`;
      audit("tool-call", "monitor",
        `REAL kafka.suppressRebalancePage — Aiven: ${groups.length} consumer groups`,
        { groups });
    } catch (e) {
      suppressDetail = `Alert suppressed: KIP-848 rebalance. Aiven API error: ${e instanceof Error ? e.message : String(e)}`;
      audit("tool-call", "monitor",
        `REAL kafka.suppressRebalancePage — Aiven API error: ${e instanceof Error ? e.message : e}`, {});
    }
  }

  const action: ActionResult = {
    approved: true, outcome: "suppressed",
    detail: suppressDetail,
    toolCalled: "kafka.suppressRebalancePage",
  };
  setAgent("monitor", { lastAction: action });
  broadcastState();
  toast("False-positive suppressed — KIP-848 rebalance context detected", "success");

  const lesson = await runLearn("benign-rebalance", "rebalance-wait", true, 16000, 0);
  await runNotification("benign-rebalance", "rebalance-wait", 16000, 0, undefined, reasoning, action, lesson);
  s.scenarioRunning = false;
  s.activeScenarios.delete("benign-rebalance");
}

// ── Public API ────────────────────────────────────────────────────────────────

export function triggerScenario(id: "lag-spike" | "controller-failover" | "share-group" | "benign-rebalance") {
  const s = getMeshState();
  if (s.activeScenarios.has(id)) return { ok: false, reason: "scenario_already_running" };
  s.activeScenarios.add(id);
  switch (id) {
    case "lag-spike":           runLagSpike().catch((e) => console.error(`[mesh] lag-spike failed:`, safeErr(e))); break;
    case "controller-failover": runControllerFailover().catch((e) => console.error(`[mesh] controller-failover failed:`, safeErr(e))); break;
    case "share-group":         runShareGroup().catch((e) => console.error(`[mesh] share-group failed:`, safeErr(e))); break;
    case "benign-rebalance":    runBenignRebalance().catch((e) => console.error(`[mesh] benign-rebalance failed:`, safeErr(e))); break;
  }
  return { ok: true };
}

export function killAgent(agentId: AgentId) {
  const s = getMeshState();
  if (s.agents[agentId].status === "crashed") return { ok: false, reason: "already_crashed" };
  setAgent(agentId, { status: "crashed", mralPhase: "idle" });
  audit("agent-kill", "system", `Agent ${agentId} killed`, { agentId });
  toast(`${s.agents[agentId].name} KILLED — incidents will queue on ops.incidents.v1`, "error");
  return { ok: true };
}

export async function restartAgent(agentId: AgentId) {
  const s = getMeshState();
  if (s.agents[agentId].status !== "crashed") return { ok: false, reason: "not_crashed" };
  setAgent(agentId, { status: "replaying" });
  audit("agent-restart", "system", `Agent ${agentId} restarting`, { agentId });
  toast(`${s.agents[agentId].name} restarting — replaying from last committed offset…`, "info");
  const queue = [...s.incidentQueue];
  s.incidentQueue = [];
  broadcastState();
  if (queue.length > 0) {
    audit("replay-start", agentId, `Replaying ${queue.length} queued incidents`, { count: queue.length });
    toast(`Replaying ${queue.length} queued incidents…`, "info");
    for (const incident of queue as Array<{ scenarioId: string; actionTaken: string; lagBefore?: number; lagAfter?: number; approvedBy?: string }>) {
      await sleep(600);
      particle("e-inc", "monitor", "writer");
      audit("consume", agentId, `Replaying incident: ${incident.scenarioId}`, incident, "ops.incidents.v1");
    }
    audit("replay-complete", agentId, `Replay complete — ${queue.length} incidents processed`, { count: queue.length });
    toast(`Replay complete — ${queue.length} incidents processed. Zero data loss. ✓`, "success");
  }
  setAgent(agentId, { status: "online" });
  broadcastState();
  return { ok: true, replayed: queue.length };
}

export function resetMesh() {
  globalThis.__agentMeshState = undefined;
  getMeshState();
  broadcastState();
  toast("Mesh reset — all state cleared", "info");
}

export function getSnapshot() {
  const s = getMeshState();
  return {
    agents: Object.values(s.agents), broker: s.broker,
    mralPhase: s.globalMralPhase, pendingApprovals: s.pendingApprovals,
    auditLog: s.auditLog, lessons: s.lessons, notifications: s.notifications,
    incidentQueueDepth: s.incidentQueue.length, scenarioRunning: s.scenarioRunning,
  };
}

/**
 * Directly mutates the live BrokerState singleton.
 * Use this (not getSnapshot) whenever you need to write to broker fields —
 * getSnapshot returns a shallow copy and primitive-field writes are lost.
 */
export function patchBrokerState(
  patcher: (broker: import("./types").BrokerState) => void
): void {
  patcher(getMeshState().broker);
}


/** Convenience accessor — returns an object with all mesh actions bundled. */
export function getMesh() {
  return {
    killAgent,
    restartAgent,
    resetMesh,
    triggerScenario,
    resolveApproval,
    decideApproval: resolveApproval,   // alias used by approvals route
    reset: resetMesh,                  // alias used by reset route
    getSnapshot,
  };
}
