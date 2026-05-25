"use client";

import { useMemo, useState } from "react";
import { ReactFlow, Handle, Position } from "@xyflow/react";
import type { Node, Edge, NodeProps } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { AgentState, MralPhase } from "@/lib/types";

// ── Agent accent colours (per wireframe) ─────────────────────────────────────
const ACCENT: Record<string, string> = {
  intake:       "#1D9E75",
  monitor:      "#7c3aed",
  writer:       "#1D9E75",
  notification: "#f97316",
};

// Light background tints per agent (very soft, stays legible)
const BG_LIGHT: Record<string, string> = {
  intake:       "#e6f5f0",   // soft emerald
  monitor:      "#ede9fe",   // soft violet
  writer:       "#ecfdf5",   // soft mint
  notification: "#fff7ed",   // soft orange
};

const DISPLAY_NAME: Record<string, string> = {
  intake:       "INTAKE",
  monitor:      "MONITOR",
  writer:       "WRITER",
  notification: "NOTIFY",
};

const SUBTITLE: Record<string, string> = {
  intake:       "MCP Gateway",
  monitor:      "SRE Brain",
  writer:       "Postmortem Author",
  notification: "Outbound Routing",
};

const STATUS_DOT: Record<string, string> = {
  online:              "#1D9E75",
  reasoning:           "#7c3aed",
  acting:              "#ea580c",
  "awaiting-approval": "#d97706",
  crashed:             "#dc2626",
  replaying:           "#0891b2",
  idle:                "#94a3b8",
};

// ── Invisible handle style ────────────────────────────────────────────────────
const H: React.CSSProperties = { opacity: 0, width: 6, height: 6, minWidth: 6, minHeight: 6 };

// ── AgentNode ─────────────────────────────────────────────────────────────────

interface AgentNodeData extends Record<string, unknown> {
  agent: AgentState;
  onKill:    (id: string) => void;
  onRestart: (id: string) => void;
}

const DIAMETER = 172;

function AgentNode({ data: rawData }: NodeProps<Node<AgentNodeData>>) {
  const { agent, onKill, onRestart } = rawData as AgentNodeData;
  const [confirmKill, setConfirmKill] = useState(false);

  const accent     = ACCENT[agent.id]        ?? "#1D9E75";
  const bgLight    = BG_LIGHT[agent.id]      ?? "#f0faf6";
  const dotColor   = STATUS_DOT[agent.status] ?? "#94a3b8";
  const crashed    = agent.status === "crashed";
  const isActive   = ["reasoning", "acting"].includes(agent.status);
  const isAwaiting = agent.status === "awaiting-approval";
  const glow       = isAwaiting ? "#d97706" : accent;

  return (
    <div style={{
      width:        DIAMETER,
      height:       DIAMETER,
      borderRadius: "50%",
      background:   crashed ? "#fef2f2" : bgLight,
      border:       `3px solid ${crashed ? "#fca5a5" : accent}`,
      display:      "flex",
      flexDirection:"column",
      alignItems:   "center",
      justifyContent:"center",
      position:     "relative",
      boxShadow: (isActive || isAwaiting)
        ? `0 0 0 7px ${glow}20, 0 8px 30px ${glow}22`
        : "0 3px 14px rgba(30,58,95,0.10)",
      transform:  (isActive || isAwaiting) ? "scale(1.09)" : "scale(1)",
      transition: "all 0.3s ease",
    }}>
      {/* ReactFlow handles — invisible, on circle perimeter */}
      <Handle type="source" position={Position.Top}    id="st" style={H} />
      <Handle type="target" position={Position.Top}    id="tt" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Right}  id="sr" style={H} />
      <Handle type="target" position={Position.Right}  id="tr" style={{ ...H, top: "62%" }} />
      <Handle type="source" position={Position.Bottom} id="sb" style={H} />
      <Handle type="target" position={Position.Bottom} id="tb" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Left}   id="sl" style={H} />
      <Handle type="target" position={Position.Left}   id="tl" style={{ ...H, top: "62%" }} />

      {/* Agent name */}
      <div style={{
        fontSize: 15, fontWeight: 800, letterSpacing: "0.5px",
        color: accent, textTransform: "uppercase", lineHeight: 1.2,
        textAlign: "center",
      }}>
        {DISPLAY_NAME[agent.id] ?? agent.id.toUpperCase()}
      </div>

      {/* Subtitle */}
      <div style={{
        fontSize: 10, color: "#64748b", marginTop: 3, marginBottom: 8,
        textAlign: "center", lineHeight: 1.3, padding: "0 14px",
      }}>
        {SUBTITLE[agent.id] ?? agent.role}
      </div>

      {/* Status badge */}
      <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
        <span style={{
          width: 10, height: 10, borderRadius: "50%", background: dotColor,
          display: "inline-block", boxShadow: `0 0 0 4px ${dotColor}22`,
          animation: "pulse 2s infinite", flexShrink: 0,
        }} />
        <span style={{ fontSize: 10, fontWeight: 700, color: dotColor }}>
          {agent.status.replace(/-/g, " ")}
        </span>
      </div>

      {/* Kill / Restart — small button at bottom of circle */}
      <div style={{ position: "absolute", bottom: 18 }}>
        {!crashed ? (
          <button
            onClick={() => setConfirmKill(true)}
            style={{
              fontSize: 9, fontWeight: 700, padding: "2px 10px",
              background: "rgba(220,38,38,0.07)", color: "#dc2626",
              border: "1px solid rgba(220,38,38,0.25)", borderRadius: 20,
              cursor: "pointer", letterSpacing: "0.3px",
            }}
          >
            Kill
          </button>
        ) : (
          <button
            onClick={() => onRestart(agent.id)}
            style={{
              fontSize: 9, fontWeight: 700, padding: "2px 10px",
              background: "rgba(29,158,117,0.10)", color: "#1D9E75",
              border: "1px solid rgba(29,158,117,0.30)", borderRadius: 20,
              cursor: "pointer",
            }}
          >
            Restart
          </button>
        )}
      </div>

      {/* Kill confirmation popup — floats just below the circle */}
      {confirmKill && (
        <div style={{
          position: "absolute",
          top: "calc(100% + 10px)",
          left: "50%",
          transform: "translateX(-50%)",
          zIndex: 999,
          background: "#fff",
          border: "2px solid #ef4444",
          borderRadius: 14,
          padding: "12px 16px",
          boxShadow: "0 8px 28px rgba(220,38,38,0.20)",
          minWidth: 160,
          textAlign: "center",
          animation: "slideDown 0.15s ease-out",
        }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: "#1e293b", marginBottom: 6 }}>
            Kill {DISPLAY_NAME[agent.id] ?? agent.id}?
          </div>
          <div style={{ fontSize: 10, color: "#64748b", marginBottom: 10 }}>
            Agent will stop processing immediately.
          </div>
          <div style={{ display: "flex", gap: 6, justifyContent: "center" }}>
            <button
              onClick={() => { onKill(agent.id); setConfirmKill(false); }}
              style={{
                fontSize: 11, fontWeight: 700, padding: "4px 14px",
                background: "#dc2626", color: "#fff",
                border: "none", borderRadius: 8, cursor: "pointer",
              }}
            >
              Yes, Kill
            </button>
            <button
              onClick={() => setConfirmKill(false)}
              style={{
                fontSize: 11, fontWeight: 600, padding: "4px 12px",
                background: "#f1f5f9", color: "#64748b",
                border: "1px solid #dce5ef", borderRadius: 8, cursor: "pointer",
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ── BrokerNode ────────────────────────────────────────────────────────────────

interface BrokerNodeData extends Record<string, unknown> {
  mode: string;
  brokersOnline: number;
  controllerEpoch: number;
  topicCount: number;
  mtls?: boolean;
  sasl?: boolean;
}

function BrokerNode({ data: rawData }: NodeProps<Node<BrokerNodeData>>) {
  const d = rawData as BrokerNodeData;
  const secure = d.mtls !== false && d.sasl !== false;

  return (
    <div style={{
      width: 186,
      background: "#ffffff",
      border: "2px solid #3b82f6",
      borderRadius: 14,
      padding: "14px 14px 14px",
      boxShadow: "0 4px 18px rgba(59,130,246,0.16)",
    }}>
      <Handle type="source" position={Position.Top}    id="st" style={H} />
      <Handle type="target" position={Position.Top}    id="tt" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Right}  id="sr" style={H} />
      <Handle type="target" position={Position.Right}  id="tr" style={{ ...H, top: "62%" }} />
      <Handle type="source" position={Position.Bottom} id="sb" style={H} />
      <Handle type="target" position={Position.Bottom} id="tb" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Left}   id="sl" style={H} />
      <Handle type="target" position={Position.Left}   id="tl" style={{ ...H, top: "62%" }} />

      <div style={{
        fontSize: 14, fontWeight: 800, letterSpacing: "0.7px",
        color: "#3b82f6", textTransform: "uppercase", marginBottom: 6,
      }}>
        BROKER
      </div>
      <div style={{ fontSize: 12, color: "#64748b", marginBottom: 5 }}>
        {d.mode} · {d.brokersOnline} online
      </div>
      <div style={{ fontSize: 11, color: secure ? "#1D9E75" : "#f97316", fontWeight: 700 }}>
        mTLS + SASL {secure ? "✓" : "✗"}
      </div>
      <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 5 }}>
        {d.topicCount} topics · epoch {d.controllerEpoch}
      </div>
    </div>
  );
}

const nodeTypes = { agent: AgentNode, broker: BrokerNode };

// ── Node positions — 2×2 agents + Broker in centre (per wireframe) ────────────
//
//   [INTAKE]              [WRITER]
//            [BROKER]
//   [MONITOR]             [NOTIFY]
//
const NODE_POS: Record<string, { x: number; y: number }> = {
  intake:       { x: 10,  y: 10  },
  writer:       { x: 570, y: 10  },
  broker:       { x: 278, y: 200 },
  monitor:      { x: 10,  y: 410 },
  notification: { x: 570, y: 410 },
};

// ── Edge colours (inactive state, per wireframe accent colours) ───────────────
const EDGE_COLOR: Record<string, string> = {
  "e-req":   "#1D9E75",   // broker → intake  (emerald)
  "e-met":   "#7c3aed",   // intake → monitor (violet)
  "e-inc":   "#1D9E75",   // monitor → writer (emerald)
  "e-aud":   "#f97316",   // writer → notify  (orange)
  "e-notif": "#3b82f6",   // notify → broker  (blue)
  "e-learn": "#7c3aed",   // monitor → broker (violet)
};

// Edge definitions with specific source/target handles for clean 2×2 routing
const EDGE_DEFS = [
  { id: "e-req",   source: "broker",       target: "intake",       label: "ops.requests",  sh: "sl", th: "sr"  },
  { id: "e-met",   source: "intake",       target: "monitor",      label: "ops.metrics",   sh: "sb", th: "tt"  },
  { id: "e-inc",   source: "monitor",      target: "writer",       label: "ops.incidents", sh: "sr", th: "sl"  },
  { id: "e-aud",   source: "writer",       target: "notification", label: "ops.audit",     sh: "sb", th: "tt"  },
  { id: "e-notif", source: "notification", target: "broker",       label: "ops.notify",    sh: "sl", th: "tr"  },
  { id: "e-learn", source: "monitor",      target: "broker",       label: "ops.lessons",   sh: "sr", th: "tl"  },
];

// ── AgentCanvas ───────────────────────────────────────────────────────────────

interface Props {
  agents: AgentState[];
  broker: {
    mode: string;
    brokersOnline: number;
    controllerEpoch: number;
    topics: Record<string, unknown>;
    mtls?: boolean;
    sasl?: boolean;
  } | null;
  activeParticles: { edgeId: string }[];
  onKill:    (id: string) => void;
  onRestart: (id: string) => void;
}

export default function AgentCanvas({ agents, broker, activeParticles, onKill, onRestart }: Props) {
  const agentMap      = useMemo(() => Object.fromEntries(agents.map(a => [a.id, a])), [agents]);
  const activeEdgeIds = useMemo(() => new Set(activeParticles.map(p => p.edgeId)), [activeParticles]);

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
          mtls: broker.mtls,
          sasl: broker.sasl,
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
    EDGE_DEFS.map(e => {
      const active    = activeEdgeIds.has(e.id);
      const baseColor = EDGE_COLOR[e.id] ?? "#94a3b8";
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        type: "bezier",
        sourceHandle: e.sh,
        targetHandle: e.th,
        label: e.label,
        animated: active,
        style: {
          stroke:           active ? "#16a34a" : baseColor,
          strokeWidth:      active ? 7 : 4,
          strokeDasharray:  active ? undefined : "10 6",
          opacity:          active ? 1 : 0.65,
        },
        labelStyle:         { fontSize: 10, fill: "#64748b", fontWeight: 600 },
        labelBgStyle:       { fill: "rgba(255,255,255,0.85)", fillOpacity: 0.85 },
        labelBgPadding:     [4, 2] as [number, number],
        labelBgBorderRadius: 3,
      };
    }),
  [activeEdgeIds]);

  return (
    <div style={{ width: "100%", height: "100%" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.12, maxZoom: 1.0 }}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnDrag={false}
        zoomOnScroll={false}
        zoomOnPinch={false}
        style={{ background: "transparent" }}
      />
    </div>
  );
}
