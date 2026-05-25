// ── Core domain types for the Agent Mesh SRE runtime ─────────────────────────

/** Static definition of an agent (used by canvas/AgentNode.tsx). */
export interface AgentDefinition {
  id: string;
  name: string;
  role: string;
  subtitle: string;
  accent: "cyan" | "violet" | "emerald" | "amber" | "rose";
  tools: string[];
  consumes: string[];
  produces: string[];
}

/** Runtime state of an agent (used by canvas/AgentNode.tsx). */
export interface AgentRuntimeState {
  status:
    | "online" | "starting" | "reasoning" | "acting" | "learning"
    | "awaiting-approval" | "crashed" | "replaying" | "offline";
  consumerLag: Record<string, number>;
  processed: number;
}

export type AgentId = "intake" | "monitor" | "writer" | "notification";

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
  agent: AgentId;
  toolCall: MCPToolCall;
  scenarioId: string;
  status: "pending" | "approved" | "rejected";
  approvedBy?: string;
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
  message: string;
  scenarioId: string;
  ticketId?: string;
}

// ── SSE event bus message shapes ─────────────────────────────────────────────

export type BusEvent =
  | { type: "state"; agents: AgentState[]; mralPhase: MralPhase; broker: BrokerState; pendingApprovals: ApprovalRequest[]; incidentQueueDepth: number }
  | { type: "audit"; record: AuditRecord }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string }
  | { type: "toast"; message: string; kind: "info" | "success" | "warning" | "error" }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord };

/** Alias kept for backward compatibility with files that import WireEvent. */
export type WireEvent = BusEvent;

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
}

export type EmailResult =
  | { ok: true; messageId: string }
  | { ok: false; error: string };
