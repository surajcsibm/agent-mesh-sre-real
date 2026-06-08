// ── Core domain types for the Agent Mesh SRE runtime ─────────────────────────

/** Static definition of an agent (used by canvas/AgentNode.tsx and Canvas.tsx). */
export interface AgentDefinition {
  id: string;
  name: string;
  role: string;
  subtitle: string;
  description?: string;
  accent: "cyan" | "violet" | "emerald" | "amber" | "rose";
  tools: string[];
  consumes: string[];
  produces: string[];
  position: { x: number; y: number };
}

/** Runtime state of an agent (used by canvas/AgentNode.tsx). */
export interface AgentRuntimeState {
  status:
    | "online" | "starting" | "reasoning" | "acting" | "learning"
    | "awaiting-approval" | "crashed" | "replaying" | "offline";
  consumerLag: Record<string, number>;
  processed: number;
  inflight?: number;
  lastReasoning?: ReasoningOutput | null;
  lastAction?: ActionResult | null;
}

// Includes both short IDs used by the server-side mesh runtime and
// long IDs used by the client-side canvas/agents-config layer.
export type AgentId =
  | "intake" | "monitor" | "writer" | "notification"
  | "intake-agent" | "monitor-agent" | "writer-agent" | "notification-agent";

export type TopicName =
  | "ops.requests.v1"
  | "ops.kafka.metrics.v1"
  | "ops.incidents.v1"
  | "ops.actions.audit.v1"
  | "ops.lessons.v1"
  | "ops.notifications.v1"
  | "demo.payments.events"
  | string; // allow dynamic topic names

export type MralPhase = "idle" | "monitor" | "reason" | "awaiting" | "act" | "learn" | "replaying";

export type AgentStatus =
  | "online"
  | "reasoning"
  | "acting"
  | "awaiting-approval"
  | "learning"
  | "crashed"
  | "replaying";

// ── MCP tool call (JSON-RPC 2.0) ──────────────────────────────────────────────

export interface MCPToolCall {
  jsonrpc: "2.0";
  id: string;
  method: "tools/call";
  params: {
    name: string;
    arguments: Record<string, unknown>;
  };
}

// ── Reasoning output from LLM step ───────────────────────────────────────────

export interface ReasoningOutput {
  rootCause: string;
  confidence: number;
  kafkaFeatureCited: string;
  rebalanceState: string;
  controllerEpoch: number;
  crossCorrelation: {
    brokers: string;
    jvmHeap: string;
    networkInRate: string;
    rebalanceInProgress: boolean;
  };
  recommendedAction: string;
  requiresApproval: boolean;
  rationale: string;
  proposedToolCall?: MCPToolCall;
  lessonsCited: string[];
}

// ── Result of an executed action ──────────────────────────────────────────────

export interface ActionResult {
  approved: boolean;
  approvedBy?: string;
  outcome: "success" | "acked" | "suppressed" | "rejected";
  detail: string;
  lagBefore?: number;
  lagAfter?: number;
  toolCalled: string;
  clusterMutation?: string;
}

// ── Agent state ───────────────────────────────────────────────────────────────

export interface AgentState {
  id: AgentId;
  name: string;
  role: string;
  status: AgentStatus;
  mralPhase: MralPhase;
  color: string;
  lastReasoning: ReasoningOutput | null;
  lastAction: ActionResult | null;
  lastLesson: LessonRecord | null;
  consumerOffset: Record<string, number>;
}

// ── Kafka broker / topic telemetry ────────────────────────────────────────────

export interface TopicState {
  partitions: number;
  lag: number;
  offsetHigh: number;
}

export interface ConsumerGroupState {
  lag: number;
  rebalanceState: string;
  members: number;
}

export interface BrokerState {
  mode: "MOCK" | "REAL";
  controllerEpoch: number;
  brokersOnline: number;
  mtls: boolean;
  sasl: boolean;
  aclCount: number;
  topics: Record<string, TopicState>;
  consumerGroups: Record<string, ConsumerGroupState>;
}

// ── Human-in-the-loop approval gate ──────────────────────────────────────────

export interface ApprovalRequest {
  id: string;
  ts: number;
  createdAt: number;
  agent: AgentId;
  proposedBy?: string;
  toolCall: MCPToolCall;
  scenarioId: string;
  reason?: string;
  status: "pending" | "approved" | "rejected";
  approvedBy?: string;
  decidedAt?: number;
  decidedBy?: string;
}

// ── Audit log record ──────────────────────────────────────────────────────────

export type AuditRecordType =
  | "publish"
  | "consume"
  | "reasoning"
  | "tool-call"
  | "approval"
  | "lesson"
  | "notification"
  | "agent-kill"
  | "agent-restart"
  | "replay-start"
  | "replay-complete";

export interface AuditRecord {
  id: string;
  ts: number;
  type: AuditRecordType;
  kind?: string;
  agent: AgentId | "system";
  summary: string;
  detail?: unknown;
  topic?: string;
}

// ── Lesson record (Learn phase) ───────────────────────────────────────────────

export interface LessonRecord {
  id: string;
  ts: number;
  scenarioId: string;
  actionTaken: string;
  effective: boolean;
  lagBefore?: number;
  lagAfter?: number;
  adjustedThreshold?: number;
  notes: string;
}

// ── Notification record ───────────────────────────────────────────────────────

export interface NotificationRecord {
  id: string;
  ts: number;
  channel: "slack" | "itsm" | "email";
  title?: string;
  body?: string;
  message: string;
  scenarioId: string;
  ticketId?: string;
}

// ── SSE event bus message shapes (type-discriminated, server→internal) ───────

export type BusEvent =
  | { type: "state"; agents: AgentState[]; mralPhase: MralPhase; broker: BrokerState; pendingApprovals: ApprovalRequest[]; incidentQueueDepth: number }
  | { type: "audit"; record: AuditRecord }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string }
  | { type: "toast"; message: string; kind: "info" | "success" | "warning" | "error" }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord }
  | { type: "approval-new"; payload: ApprovalRequest }
  | { type: "approval-update"; payload: ApprovalRequest }
| { type: "auto-trigger-scenario"; scenarioId: string }
| { type: "auto-topic-heal"; topicName: string; currentStatus: "degraded" | "critical"; lagTotal: number; partitions: number };

// ── Wire-protocol types (kind-discriminated, used by store + sse-client) ──────

/** Cluster status as exposed in snapshot payloads and broker.ts. */
export interface ClusterStatus {
  mode: "MOCK" | "REAL" | "KRaft";
  controllerEpoch: number;
  // Short-form fields (SnapshotPayload / TopBar simple view)
  brokersOnline: number;
  mtls: boolean;
  sasl: boolean;
  aclCount: number;
  // Rich fields from broker.ts / TopBar detailed view
  controllerId?: number;
  brokers?: Array<{ id: number; rack: string; status: string }>;
  schemaRegistry?: { connected: boolean; specs: number };
  security?: { mTLS: boolean; saslScram: boolean; aclsActive: number };
}

/** A single Kafka record as tracked in the canvas topic view. Generic for broker.ts. */
export interface KafkaRecord<T = unknown> {
  topic: TopicName;
  partition: number;
  offset: number;
  ts: number;
  timestamp?: number;
  key?: string;
  value: T;
  headers?: Record<string, string>;
}

/** Per-topic snapshot including recent records. */
export interface TopicSnapshot {
  partitions: number;
  logEndOffset: number;
  recentRecords: KafkaRecord[];
}

/** Full state snapshot sent on initial SSE connection. */
export interface SnapshotPayload {
  agents: Record<string, AgentRuntimeState>;
  cluster: ClusterStatus;
  approvals: ApprovalRequest[];
  incidents: IncidentEvent[];
  audit: AuditEvent[];
  notifications: NotificationEvent[];
  lessons: LessonRecord[];
  topics: Record<TopicName, TopicSnapshot>;
}

/** An SRE incident event (raised by Monitor Agent). */
export interface IncidentEvent {
  id: string;
  ts: number;
  scenarioId: string;
  severity: "low" | "medium" | "high" | "critical";
  title: string;
  description: string;
}

/** An audit event entry (aligns with AuditRecord for the wire layer). */
export type AuditEvent = AuditRecord;

/** A notification event entry (aligns with NotificationRecord for the wire layer). */
export type NotificationEvent = NotificationRecord;

/** A particle animation hint for the canvas edge animations. */
export interface ParticleHint {
  id: string;
  edgeId: string;
  fromNode: string;
  toNode: string;
  source: string;
  target: string;
  topic?: TopicName;
  color: string;
  durationMs: number;
}

/**
 * Wire-protocol event (kind-discriminated union).
 * Used by sse-client.ts, store.ts, and kafka/tap.ts.
 * The server-side event bus uses BusEvent (type-discriminated).
 */
export type WireEvent =
  | { kind: "snapshot"; payload: SnapshotPayload }
  | { kind: "agent-state"; payload: { agent: AgentId; state: AgentRuntimeState } }
  | { kind: "approval-new"; payload: ApprovalRequest }
  | { kind: "approval-update"; payload: ApprovalRequest }
  | { kind: "incident"; payload: IncidentEvent }
  | { kind: "audit"; payload: AuditEvent }
  | { kind: "notification"; payload: NotificationEvent }
  | { kind: "lesson"; payload: LessonRecord }
  | { kind: "topic-record"; payload: KafkaRecord }
  | { kind: "particle"; payload: ParticleHint }
  | { kind: "log"; payload: { ts: number; agent: AgentId | "system"; level: "info" | "warn" | "error"; message: string } };

// ── Email summary payload ─────────────────────────────────────────────────────

export interface AgentSummaryPayload {
  scenarioId: string;
  scenarioLabel: string;
  ts: number;
  reasoning: ReasoningOutput | null;
  action: ActionResult | null;
  lesson: LessonRecord | null;
  slackMessage: string;
  itsmTicket: string;
  approvedBy: string | null;
  liveEvents?: Array<{ type: string; agent: string; summary: string; ts: number }>;
}

export type EmailResult =
  | { ok: true; messageId: string }
  | { ok: false; error: string };
