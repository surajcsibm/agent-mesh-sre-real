"use client";

import { useMemo, useCallback } from "react";
import {
  ReactFlow,
  Background, Controls,
  Handle, Position,
} from "@xyflow/react";
import type { Node, Edge, NodeProps } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { AgentState, MralPhase } from "@/lib/types";
import clsx from "clsx";

// ── Status indicator colours (dot only) ──────────────────────────────────────

const STATUS_COLOR: Record<string, string> = {
  online:              "#16a34a",
  reasoning:           "#7c3aed",
  acting:              "#ea580c",
  "awaiting-approval": "#d97706",
  crashed:             "#dc2626",
  replaying:           "#0891b2",
  idle:                "#94a3b8",
};

const MRAL_COLOR: Record<MralPhase, string> = {
  idle:      "#94a3b8",
  monitor:   "#2563eb",
  reason:    "#7c3aed",
  awaiting:  "#d97706",
  act:       "#ea580c",
  learn:     "#16a34a",
  replaying: "#0891b2",
};

// ── Agent node — uniform light blue background ────────────────────────────────

interface AgentNodeData extends Record<string, unknown> {
  agent: AgentState;
  onKill: (id: string) => void;
  onRestart: (id: string) => void;
}

function AgentNode({ data }: NodeProps<Node<AgentNodeData>>) {
  const { agent, onKill, onRestart } = data as AgentNodeData;
  const statusColor = STATUS_COLOR[agent.status] ?? "#94a3b8";
  const mralColor   = MRAL_COLOR[agent.mralPhase] ?? "#94a3b8";
  const crashed     = agent.status === "crashed";
  const isActive    = ["reasoning", "acting", "learning"].includes(agent.status);
  const isAwaiting  = agent.status === "awaiting-approval";

  return (
    <div
      className={clsx(
        "relative rounded-xl border-2 p-3 w-44 shadow-md transition-all duration-300",
        crashed   ? "bg-red-50 border-red-300" :
        isAwaiting? "bg-amber-50 border-amber-400" :
        isActive  ? "bg-white border-blue-400" :
                    "bg-blue-50 border-blue-200"
      )}
      style={{
        transform:  isActive || isAwaiting ? "scale(1.07)" : "scale(1)",
        boxShadow:  isAwaiting
          ? `0 0 0 3px #fcd34d55, 0 8px 24px rgba(217, 119, 6, 0.25)`
          : isActive
          ? `0 0 0 2px ${statusColor}44, 0 8px 24px ${statusColor}30`
          : undefined,
        zIndex: isActive || isAwaiting ? 10 : 0,
      }}
    >
      <Handle type="target" position={Position.Left}
        style={{ background: "#93c5fd", border: "2px solid #bfdbfe" }} />
      <Handle type="source" position={Position.Right}
        style={{ background: "#93c5fd", border: "2px solid #bfdbfe" }} />

      {/* Header */}
      <div className="flex items-center gap-2 mb-2">
        <div className="w-2.5 h-2.5 rounded-full shrink-0 animate-pulse"
          style={{ background: statusColor }} />
        <div className={clsx("text-xs font-bold truncate leading-tight",
          crashed ? "text-red-800" : "text-blue-900")}>
          {agent.name}
        </div>
      </div>

      {/* MRAL phase badge */}
      <div className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 mb-2"
        style={{ background: mralColor + "18", border: `1px solid ${mralColor}40` }}>
        <span className="text-[10px] font-bold uppercase tracking-wider"
          style={{ color: mralColor }}>
          {agent.mralPhase}
        </span>
      </div>

      {/* Status text */}
      <div className={clsx(
        "text-[10px] truncate mb-1.5 font-semibold uppercase tracking-wide",
        crashed    ? "text-red-600" :
        isAwaiting ? "text-amber-600" :
        isActive   ? "text-blue-600" :
                     "text-slate-400"
      )}>
        {agent.status.replace(/-/g, " ")}
      </div>

      {/* Last action snippet */}
      {agent.lastAction && (
        <div className="text-[9px] text-slate-500 truncate bg-blue-100/60 rounded px-1.5 py-0.5 mb-2 border border-blue-100">
          {agent.lastAction.detail.slice(0, 40)}…
        </div>
      )}

      {/* Kill / Restart */}
      <div className="flex gap-1 mt-1">
        {!crashed ? (
          <button onClick={() => onKill(agent.id)}
            className="flex-1 text-[9px] font-semibold py-1 rounded-lg
                       bg-red-50 text-red-600 hover:bg-red-100
                       transition-colors border border-red-200">
            Kill
          </button>
        ) : (
          <button onClick={() => onRestart(agent.id)}
            className="flex-1 text-[9px] font-semibold py-1 rounded-lg
                       bg-cyan-50 text-cyan-700 hover:bg-cyan-100
                       transition-colors border border-cyan-200">
            Restart
          </button>
        )}
      </div>
    </div>
  );
}

// ── Broker node — light blue, slightly deeper ─────────────────────────────────

interface BrokerNodeData extends Record<string, unknown> {
  mode: string;
  brokersOnline: number;
  controllerEpoch: number;
  topicCount: number;
}

function BrokerNode({ data: rawData }: NodeProps<Node<BrokerNodeData>>) {
  const data = rawData as BrokerNodeData;
  return (
    <div className="rounded-xl border-2 border-blue-300 bg-blue-100 p-3 w-44 shadow-md">
      <Handle type="source" position={Position.Right}
        style={{ background: "#60a5fa", border: "2px solid #93c5fd" }} />
      <Handle type="target" position={Position.Left}
        style={{ background: "#60a5fa", border: "2px solid #93c5fd" }} />
      <div className="text-xs font-bold text-blue-800 mb-2">Kafka Broker</div>
      <div className="space-y-0.5">
        {[
          ["Mode",   data.mode],
          ["Online", `${data.brokersOnline}/3`],
          ["Epoch",  String(data.controllerEpoch)],
          ["Topics", String(data.topicCount)],
        ].map(([k, v]) => (
          <div key={k} className="flex justify-between text-[10px]">
            <span className="text-blue-500">{k}</span>
            <span className="text-blue-900 font-semibold">{v}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

const nodeTypes = { agent: AgentNode, broker: BrokerNode };

// ── Layout positions ──────────────────────────────────────────────────────────

const NODE_POS: Record<string, { x: number; y: number }> = {
  broker:       { x: 60,  y: 200 },
  intake:       { x: 280, y: 200 },
  monitor:      { x: 500, y: 100 },
  writer:       { x: 720, y: 100 },
  notification: { x: 720, y: 300 },
};

const EDGE_DEFS = [
  { id: "e-req",   source: "broker",       target: "intake",       label: "ops.requests.v1" },
  { id: "e-met",   source: "intake",       target: "monitor",      label: "ops.kafka.metrics.v1" },
  { id: "e-inc",   source: "monitor",      target: "writer",       label: "ops.incidents.v1" },
  { id: "e-aud",   source: "writer",       target: "notification", label: "ops.actions.audit.v1" },
  { id: "e-learn", source: "monitor",      target: "monitor",      label: "ops.lessons.v1" },
  { id: "e-notif", source: "notification", target: "broker",       label: "ops.notifications.v1" },
];

// ── AgentCanvas ───────────────────────────────────────────────────────────────

interface Props {
  agents: AgentState[];
  broker: { mode: string; brokersOnline: number; controllerEpoch: number; topics: Record<string, unknown> } | null;
  activeParticles: { edgeId: string }[];
  onKill: (id: string) => void;
  onRestart: (id: string) => void;
}

export default function AgentCanvas({ agents, broker, activeParticles, onKill, onRestart }: Props) {
  const agentMap      = useMemo(() => Object.fromEntries(agents.map((a) => [a.id, a])), [agents]);
  const activeEdgeIds = useMemo(() => new Set(activeParticles.map((p) => p.edgeId)), [activeParticles]);

  const nodes: Node[] = useMemo(() => {
    const list: Node[] = [];

    if (broker) {
      list.push({
        id: "broker", type: "broker",
        position: NODE_POS.broker,
        data: {
          mode: broker.mode,
          brokersOnline: broker.brokersOnline,
          controllerEpoch: broker.controllerEpoch,
          topicCount: Object.keys(broker.topics).length,
        },
      });
    }

    for (const [id, pos] of Object.entries(NODE_POS)) {
      if (id === "broker") continue;
      const agent = agentMap[id];
      if (!agent) continue;
      list.push({
        id, type: "agent",
        position: pos,
        data: { agent, onKill, onRestart },
      });
    }

    return list;
  }, [agentMap, broker, onKill, onRestart]);

  const edges: Edge[] = useMemo(() =>
    EDGE_DEFS.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label,
      animated: activeEdgeIds.has(e.id),
      style: {
        stroke: activeEdgeIds.has(e.id) ? "#2563eb" : "#bfdbfe",
        strokeWidth: activeEdgeIds.has(e.id) ? 2.5 : 1.5,
      },
      labelStyle:   { fontSize: 9, fill: "#64748b" },
      labelBgStyle: { fill: "#eff6ff", fillOpacity: 0.9 },
    })),
  [activeEdgeIds]);

  const onNodeClick = useCallback(() => {}, []);

  return (
    <div className="w-full h-full rounded-xl overflow-hidden border border-blue-100 shadow-sm">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodeClick={onNodeClick}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#bfdbfe" gap={24} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
