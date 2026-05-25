// MCP Tool Registry — typed tool catalogue with JSON Schema contracts and policy tags.
// mesh.ts calls tools by name; this file is the authoritative source of truth for
// what each tool accepts, returns, and whether it requires human approval.

import type { AgentId } from "./types";

export interface MCPTool {
  name:             string;
  description:      string;
  owner?:           string;
  policyTags:       string[];
  requiresApproval: boolean;
  inputSchema:      object;
  outputSchema:     object;
}

export const MCP_TOOLS: MCPTool[] = [
  {
    name: "kafka.scaleConsumers",
    description: "Scale a consumer group by adding replica instances to drain a lag backlog.",
    policyTags: ["infra-mutation", "consumer-scaling", "human-gated"],
    requiresApproval: true,
    inputSchema: {
      type: "object",
      required: ["group", "delta"],
      properties: {
        group:  { type: "string" },
        delta:  { type: "integer", minimum: 1, maximum: 5 },
        reason: { type: "string" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        previousReplicas: { type: "integer" },
        newReplicas:      { type: "integer" },
        lagBefore:        { type: "integer" },
        lagAfter:         { type: "integer" },
        clusterMutation:  { type: "string" },
      },
    },
  },
  {
    name: "kafka.ackControllerFailover",
    description: "Acknowledge a KRaft controller leadership change — audit only, no infra mutation.",
    policyTags: ["read-only", "audit"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["previousEpoch", "newEpoch"],
      properties: {
        previousEpoch:      { type: "integer" },
        newEpoch:           { type: "integer" },
        electionDurationMs: { type: "integer" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        acked:   { type: "boolean" },
        auditId: { type: "string" },
      },
    },
  },
  {
    name: "kafka.checkpointShareGroup",
    description: "Checkpoint a KIP-932 share group and optionally scale consumers.",
    policyTags: ["infra-mutation", "share-group", "human-gated"],
    requiresApproval: true,
    inputSchema: {
      type: "object",
      required: ["shareGroupId"],
      properties: {
        shareGroupId:      { type: "string" },
        delta:             { type: "integer" },
        checkpointOffset:  { type: "integer" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        checkpointed:    { type: "boolean" },
        scaledBy:        { type: "integer" },
        offsetCommitted: { type: "integer" },
      },
    },
  },
  {
    name: "kafka.suppressRebalancePage",
    description: "Suppress an alert page when a cooperative KIP-848 rebalance is in progress.",
    policyTags: ["read-only", "suppression", "audit"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["consumerGroup", "rebalanceState"],
      properties: {
        consumerGroup:  { type: "string" },
        rebalanceState: { type: "string" },
        lagObserved:    { type: "integer" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        suppressed: { type: "boolean" },
        rationale:  { type: "string" },
      },
    },
  },
  {
    name: "sre.draftIncidentReport",
    description: "Draft a structured post-incident markdown report from an audit event.",
    policyTags: ["write", "incident-management"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["incidentId", "actionTaken"],
      properties: {
        incidentId:  { type: "string" },
        actionTaken: { type: "string" },
        lagBefore:   { type: "integer" },
        lagAfter:    { type: "integer" },
        approvedBy:  { type: "string" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        reportMarkdown: { type: "string" },
        wordCount:      { type: "integer" },
      },
    },
  },
  {
    name: "sre.notifySlack",
    description: "Post a structured notification to the #sre-alerts Slack channel.",
    policyTags: ["outbound", "notification"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["channel", "message"],
      properties: {
        channel:  { type: "string" },
        message:  { type: "string" },
        severity: { type: "string" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        messageTs: { type: "string" },
        channelId: { type: "string" },
      },
    },
  },
  {
    name: "sre.openITSMTicket",
    description: "Open a ServiceNow / ITSM ticket for a resolved incident.",
    policyTags: ["outbound", "itsm", "compliance"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["title", "description", "severity"],
      properties: {
        title:           { type: "string" },
        description:     { type: "string" },
        severity:        { type: "string" },
        assignmentGroup: { type: "string" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        ticketId: { type: "string" },
        url:      { type: "string" },
      },
    },
  },
  {
    name: "sre.sendEmailSummary",
    description: "Send a full MRAL trace summary email after each scenario run.",
    policyTags: ["outbound", "email", "notification"],
    requiresApproval: false,
    inputSchema: {
      type: "object",
      required: ["scenarioId", "recipient"],
      properties: {
        scenarioId: { type: "string" },
        recipient:  { type: "string", format: "email" },
        reasoning:  { type: "object" },
        action:     { type: "object" },
        lesson:     { type: "object" },
      },
    },
    outputSchema: {
      type: "object",
      properties: {
        ok:        { type: "boolean" },
        messageId: { type: "string" },
        error:     { type: "string" },
      },
    },
  },
];

// Per-agent tool binding — enforces least-privilege access.
// An agent can only call the tools listed here, regardless of what mesh.ts requests.
export const AGENT_TOOLS: Record<AgentId, string[]> = {
  intake:       ["kafka.scaleConsumers", "kafka.checkpointShareGroup"],
  monitor:      ["kafka.scaleConsumers", "kafka.ackControllerFailover",
                 "kafka.checkpointShareGroup", "kafka.suppressRebalancePage",
                 "sre.draftIncidentReport"],
  writer:       ["sre.draftIncidentReport", "sre.sendEmailSummary"],
  notification: ["sre.notifySlack", "sre.openITSMTicket", "sre.sendEmailSummary"],
};

// Convenience lookup by tool name.
export const MCP_TOOL_MAP = new Map(MCP_TOOLS.map((t) => [t.name, t]));

/** Return true if the given agent is allowed to call the named tool. */
export function agentCanCall(agentId: AgentId, toolName: string): boolean {
  return (AGENT_TOOLS[agentId] ?? []).includes(toolName);
}
