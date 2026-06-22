"use client";

import { useMemo } from "react";
import {
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
  ReactFlow,
  ReactFlowProvider,
  type Edge,
  type Node,
} from "@xyflow/react";
import { AGENTS, AGENT_ORDER } from "@/lib/agents-config";
import { useMesh } from "@/lib/store";
import { AgentNode } from "./AgentNode";
import { TopicEdge } from "./TopicEdge";
import { ParticleLayer } from "./ParticleLayer";

const nodeTypes = { agent: AgentNode };
const edgeTypes = { topic: TopicEdge };

const EDGE_STROKE = "rgba(180, 200, 240, 0.55)";

export function Canvas() {
  const agentsState = useMesh((s) => s.agents);

  const nodes: Node[] = useMemo(
    () =>
      AGENT_ORDER.map((id) => ({
        id,
        type: "agent",
        position: AGENTS[id].position,
        data: { def: AGENTS[id], state: (agentsState as Record<string, unknown> | undefined)?.[id] },
        draggable: true,
      })),
    [agentsState]
  );

  const edges: Edge[] = useMemo(
    () => [
      {
        id: "intake-monitor",
        source: "intake-agent",
        target: "monitor-agent",
        type: "topic",
        data: { topic: "ops.requests.v1", partitions: 3, feature: "classic" },
        markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_STROKE },
      },
      {
        id: "monitor-writer",
        source: "monitor-agent",
        target: "writer-agent",
        type: "topic",
        data: { topic: "ops.incidents.v1", partitions: 3, feature: "classic" },
        markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_STROKE },
      },
      {
        id: "writer-notification",
        source: "writer-agent",
        target: "notification-agent",
        type: "topic",
        data: { topic: "ops.actions.audit.v1", partitions: 3, audit: true },
        markerEnd: { type: MarkerType.ArrowClosed, color: "#fbbf24" },
      },
      {
        id: "monitor-self",
        source: "monitor-agent",
        target: "monitor-agent",
        sourceHandle: "self-out",
        targetHandle: "self-in",
        type: "topic",
        data: { topic: "ops.lessons.v1", partitions: 1, feature: "classic" },
        markerEnd: { type: MarkerType.ArrowClosed, color: EDGE_STROKE },
      },
    ],
    []
  );

  return (
    <ReactFlowProvider>
      <div className="w-full h-full relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          fitView
          fitViewOptions={{ padding: 0.2, minZoom: 0.6, maxZoom: 1.2 }}
          minZoom={0.4}
          maxZoom={1.6}
          proOptions={{ hideAttribution: true }}
          defaultEdgeOptions={{ type: "topic" }}
          panOnScroll
          panOnDrag={[1, 2]}
          selectionOnDrag
        >
          <Background variant={BackgroundVariant.Dots} gap={28} size={1.2} color="rgba(255,255,255,0.06)" />
          <Controls position="bottom-right" showInteractive={false} />
        </ReactFlow>
        <ParticleLayer />
      </div>
    </ReactFlowProvider>
  );
}
