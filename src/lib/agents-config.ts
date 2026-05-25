/**
 * Static definitions for the four agents in the mesh.
 * Positions are tuned for the React Flow canvas.
 */
import type { AgentDefinition } from "./types";

export const AGENTS: Record<string, AgentDefinition> = {
  "intake-agent": {
    id: "intake-agent",
    name: "Intake Agent",
    role: "MCP Gateway",
    subtitle: "Receives operator requests via MCP tool call",
    description:
      "Exposes intake.submitOpsRequest tool. Validates inputs, signs the request, publishes to ops.requests.v1 with an idempotency key.",
    consumes: [],
    produces: ["ops.requests.v1"],
    tools: ["intake.submitOpsRequest"],
    position: { x: 60, y: 240 },
    accent: "cyan",
  },
  "monitor-agent": {
    id: "monitor-agent",
    name: "Monitor Agent",
    role: "SRE Brain",
    subtitle: "Monitor → Reason → Act → Learn",
    description:
      "Consumes ops.kafka.metrics.v1 + ops.requests.v1. Cross-references rebalance state, KRaft epoch, share-group offsets. Reasoning step emits structured JSON. Actions are policy-gated.",
    consumes: ["ops.requests.v1", "ops.kafka.metrics.v1", "ops.lessons.v1"],
    produces: ["ops.incidents.v1", "ops.actions.audit.v1", "ops.lessons.v1"],
    tools: [
      "kafka.scaleConsumers",
      "kafka.acknowledgeFailover",
      "kafka.shareGroupCheckpoint",
    ],
    position: { x: 460, y: 240 },
    accent: "violet",
  },
  "writer-agent": {
    id: "writer-agent",
    name: "Writer Agent",
    role: "Postmortem Author",
    subtitle: "Drafts structured incident reports",
    description:
      "Consumes ops.incidents.v1. Uses an LLM to draft post-incident markdown. Publishes the report to ops.actions.audit.v1.",
    consumes: ["ops.incidents.v1"],
    produces: ["ops.actions.audit.v1"],
    tools: ["writer.draftIncidentReport"],
    position: { x: 860, y: 240 },
    accent: "emerald",
  },
  "notification-agent": {
    id: "notification-agent",
    name: "Notification Agent",
    role: "Outbound Routing",
    subtitle: "Slack + ITSM closing the loop",
    description:
      "Consumes ops.actions.audit.v1 (writer-published reports). Posts to Slack, opens ITSM tickets.",
    consumes: ["ops.actions.audit.v1"],
    produces: ["ops.notifications.v1"],
    tools: ["notify.slack", "itsm.openTicket"],
    position: { x: 1260, y: 240 },
    accent: "amber",
  },
};

export const AGENT_ORDER: string[] = [
  "intake-agent",
  "monitor-agent",
  "writer-agent",
  "notification-agent",
];
