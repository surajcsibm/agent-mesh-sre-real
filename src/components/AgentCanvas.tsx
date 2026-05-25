"use client";

import { useMemo, useState } from "react";
import { ReactFlow, Handle, Position } from "@xyflow/react";
import type { Node, Edge, NodeProps } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { AgentState, MralPhase } from "@/lib/types";

// ── Agent accent colours (per wireframe) ─────────────────────────────────────
const ACCENT: Record<string, string> = {
  intake:       "#1D9E75",   // emerald green
  monitor:      "#7c3aed",   // violet
  writer:       "#0891b2",   // cyan-600 — distinct from intake
  notification: "#f97316",   // orange
};

// Light background tints per agent (very soft, stays legible)
const BG_LIGHT: Record<string, string> = {
  intake:       "#e6f5f0",   // soft emerald
  monitor:      "#ede9fe",   // soft violet
  writer:       "#ecfeff",   // soft cyan
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
            onClick={(e) => { e.stopPropagation(); setConfirmKill(true); }}
            style={{
              fontSize: 9, fontWeight: 700, padding: "2px 10px",
              background: "rgba(220,38,38,0.07)", color: "#dc2626",
              border: "1px solid rgba(220,38,38,0.25)", borderRadius: 20,
              cursor: "pointer", letterSpacing: "0.3px",
              pointerEvents: "all",
            }}
          >
            Kill
          </button>
        ) : (
          <button
            onClick={(e) => { e.stopPropagation(); onRestart(agent.id); }}
            style={{
              fontSize: 9, fontWeight: 700, padding: "2px 10px",
              background: "rgba(29,158,117,0.10)", color: "#1D9E75",
              border: "1px solid rgba(29,158,117,0.30)", borderRadius: 20,
              cursor: "pointer",
              pointerEvents: "all",
            }}
          >
            Restart
          </button>
        )}
      </div>

      {/* Kill confirmation popup — floats just below the circle */}
      {confirmKill && (
        <div
          onClick={(e) => e.stopPropagation()}
          style={{
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
            pointerEvents: "all",
          }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: "#1e293b", marginBottom: 6 }}>
            Kill {DISPLAY_NAME[agent.id] ?? agent.id}?
          </div>
          <div style={{ fontSize: 10, color: "#64748b", marginBottom: 10 }}>
            Agent will stop processing immediately.
          </div>
          <div style={{ display: "flex", gap: 6, justifyContent: "center" }}>
            <button
              onClick={(e) => { e.stopPropagation(); onKill(agent.id); setConfirmKill(false); }}
              style={{
                fontSize: 11, fontWeight: 700, padding: "4px 14px",
                background: "#dc2626", color: "#fff",
                border: "none", borderRadius: 8, cursor: "pointer",
                pointerEvents: "all",
              }}
            >
              Yes, Kill
            </button>
            <button
              onClick={(e) => { e.stopPropagation(); setConfirmKill(false); }}
              style={{
                fontSize: 11, fontWeight: 600, padding: "4px 12px",
                background: "#f1f5f9", color: "#64748b",
                border: "1px solid #dce5ef", borderRadius: 8, cursor: "pointer",
                pointerEvents: "all",
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

const BROKER_DIAMETER = 172;

function BrokerNode({ data: rawData }: NodeProps<Node<BrokerNodeData>>) {
  const d = rawData as BrokerNodeData;
  const secure = d.mtls !== false && d.sasl !== false;

  return (
    <div style={{
      width:         BROKER_DIAMETER,
      height:        BROKER_DIAMETER,
      borderRadius:  "50%",
      background:    "#eff6ff",          // soft blue tint
      border:        "3px solid #3b82f6",
      boxShadow:     "0 4px 20px rgba(59,130,246,0.18)",
      display:       "flex",
      flexDirection: "column",
      alignItems:    "center",
      justifyContent:"center",
      gap:           3,
    }}>
      <Handle type="source" position={Position.Top}    id="st" style={H} />
      <Handle type="target" position={Position.Top}    id="tt" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Right}  id="sr" style={H} />
      <Handle type="target" position={Position.Right}  id="tr" style={{ ...H, top: "62%" }} />
      <Handle type="source" position={Position.Bottom} id="sb" style={H} />
      <Handle type="target" position={Position.Bottom} id="tb" style={{ ...H, left: "60%" }} />
      <Handle type="source" position={Position.Left}   id="sl" style={H} />
      <Handle type="target" position={Position.Left}   id="tl" style={{ ...H, top: "62%" }} />

      {/* Name */}
      <div style={{
        fontSize: 14, fontWeight: 800, letterSpacing: "0.7px",
        color: "#3b82f6", textTransform: "uppercase",
      }}>
        BROKER
      </div>

      {/* Mode + online count */}
      <div style={{ fontSize: 10, color: "#64748b", textAlign: "center" }}>
        {d.mode} · {d.brokersOnline} online
      </div>

      {/* Security status */}
      <div style={{
        fontSize: 10, fontWeight: 700,
        color: secure ? "#1D9E75" : "#f97316",
        textAlign: "center",
      }}>
        mTLS+SASL {secure ? "✓" : "✗"}
      </div>

      {/* Topics + epoch */}
      <div style={{ fontSize: 10, color: "#94a3b8", textAlign: "center" }}>
        {d.topicCount} topics
      </div>
    </div>
  );
}

// ── SubAgentNode — ephemeral small circles spawned by Monitor ────────────────
// Appears below Monitor during Reason / Act / Learn phases.

const SUB_DIAMETER = 90;

interface SubAgentNodeData extends Record<string, unknown> {
  label:    string;   // "REASON" | "ACT" | "LEARN"
  accent:   string;
  bgColor:  string;
  sublabel: string;   // short description line
}

function SubAgentNode({ data: rawData }: NodeProps<Node<SubAgentNodeData>>) {
  const d = rawData as SubAgentNodeData;
  return (
    <div style={{
      width:         SUB_DIAMETER,
      height:        SUB_DIAMETER,
      borderRadius:  "50%",
      background:    d.bgColor,
      border:        `2px dashed ${d.accent}`,
      display:       "flex",
      flexDirection: "column",
      alignItems:    "center",
      justifyContent:"center",
      position:      "relative",
      boxShadow:     `0 0 0 5px ${d.accent}18, 0 4px 16px ${d.accent}28`,
      animation:     "pulse 1.6s infinite",
      transition:    "all 0.3s ease",
    }}>
      <Handle type="target" position={Position.Top}   id="tt" style={H} />
      <Handle type="target" position={Position.Left}  id="tl" style={H} />
      <Handle type="target" position={Position.Right} id="tr" style={H} />

      {/* Phase label */}
      <div style={{
        fontSize: 11, fontWeight: 900, color: d.accent,
        textTransform: "uppercase", letterSpacing: "0.5px",
        lineHeight: 1.2,
      }}>
        {d.label}
      </div>

      {/* Sub-label */}
      <div style={{
        fontSize: 8, color: "#64748b", marginTop: 4,
        textAlign: "center", lineHeight: 1.3,
        padding: "0 8px",
      }}>
        {d.sublabel}
      </div>
    </div>
  );
}

const nodeTypes = { agent: AgentNode, broker: BrokerNode, subagent: SubAgentNode };

// ── Node positions — 2×2 agents + Broker in centre (per wireframe) ────────────
//
//   [INTAKE]              [WRITER]
//            [BROKER]
//   [MONITOR]             [NOTIFY]
//        [SUB-AGENT]          ← ephemeral, below Monitor
//
const NODE_POS: Record<string, { x: number; y: number }> = {
  intake:       { x: 30,  y: 20  },
  writer:       { x: 390, y: 20  },
  broker:       { x: 206, y: 180 },
  monitor:      { x: 30,  y: 340 },
  notification: { x: 390, y: 340 },
};

// Position of ephemeral sub-agent bubble (below Monitor, centred)
const SUB_AGENT_POS = { x: 41, y: 545 }; // Monitor bottom edge ~512 → 33 px gap

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

    // ── Ephemeral sub-agent bubbles spawned by Monitor ──────────────────────
    // One small circle appears below Monitor during each active MRAL phase.
    const mon = agentMap["monitor"];
    if (mon) {
      const isLearning = mon.status === "reasoning" && mon.mralPhase === "learn";
      const isReasoning = mon.status === "reasoning" && mon.mralPhase !== "learn";
      const isActing    = mon.status === "acting";
      const isAwaiting  = mon.status === "awaiting-approval";

      if (isReasoning || isAwaiting) {
        list.push({
          id: "sub-reason", type: "subagent",
          position: SUB_AGENT_POS,
          data: {
            label:   "REASON",
            accent:  "#7c3aed",
            bgColor: "#f5f3ff",
            sublabel: isAwaiting
              ? "Awaiting approval…"
              : (mon.lastReasoning?.rootCause?.slice(0, 36) ?? "Analyzing metrics…"),
          } as SubAgentNodeData,
        });
      } else if (isActing) {
        list.push({
          id: "sub-act", type: "subagent",
          position: SUB_AGENT_POS,
          data: {
            label:   "ACT",
            accent:  "#ea580c",
            bgColor: "#fff7ed",
            sublabel: mon.lastAction?.detail?.slice(0, 36) ?? "Executing action…",
          } as SubAgentNodeData,
        });
      } else if (isLearning) {
        list.push({
          id: "sub-learn", type: "subagent",
          position: SUB_AGENT_POS,
          data: {
            label:   "LEARN",
            accent:  "#16a34a",
            bgColor: "#f0fdf4",
            sublabel: mon.lastLesson?.notes?.slice(0, 36) ?? "Recording lesson…",
          } as SubAgentNodeData,
        });
      }
    }

    return list;
  }, [agentMap, broker, onKill, onRestart]);

  const edges: Edge[] = useMemo(() => {
    const list: Edge[] = EDGE_DEFS.map(e => {
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
          stroke:      active ? "#16a34a" : baseColor,
          strokeWidth: active ? 7 : 4,
          opacity:     active ? 1 : 0.70,
        },
        labelStyle:         { fontSize: 10, fill: "#64748b", fontWeight: 600 },
        labelBgStyle:       { fill: "rgba(255,255,255,0.85)", fillOpacity: 0.85 },
        labelBgPadding:     [4, 2] as [number, number],
        labelBgBorderRadius: 3,
      };
    });

    // ── Ephemeral edges from Monitor to active sub-agent bubble ────────────
    const mon = agentMap["monitor"];
    if (mon) {
      const isLearning  = mon.status === "reasoning" && mon.mralPhase === "learn";
      const isReasoning = mon.status === "reasoning" && mon.mralPhase !== "learn";
      const isActing    = mon.status === "acting";
      const isAwaiting  = mon.status === "awaiting-approval";

      if (isReasoning || isAwaiting) {
        list.push({
          id: "e-sub-reason",
          source: "monitor", target: "sub-reason",
          sourceHandle: "sb", targetHandle: "tt",
          type: "bezier", animated: true,
          label: "spawn",
          style: { stroke: "#7c3aed", strokeWidth: 2, strokeDasharray: "6 3", opacity: 0.9 },
          labelStyle:         { fontSize: 9, fill: "#7c3aed", fontWeight: 700 },
          labelBgStyle:       { fill: "rgba(255,255,255,0.85)", fillOpacity: 0.85 },
          labelBgPadding:     [3, 1] as [number, number],
          labelBgBorderRadius: 3,
        });
      } else if (isActing) {
        list.push({
          id: "e-sub-act",
          source: "monitor", target: "sub-act",
          sourceHandle: "sb", targetHandle: "tt",
          type: "bezier", animated: true,
          label: "spawn",
          style: { stroke: "#ea580c", strokeWidth: 2, strokeDasharray: "6 3", opacity: 0.9 },
          labelStyle:         { fontSize: 9, fill: "#ea580c", fontWeight: 700 },
          labelBgStyle:       { fill: "rgba(255,255,255,0.85)", fillOpacity: 0.85 },
          labelBgPadding:     [3, 1] as [number, number],
          labelBgBorderRadius: 3,
        });
      } else if (isLearning) {
        list.push({
          id: "e-sub-learn",
          source: "monitor", target: "sub-learn",
          sourceHandle: "sb", targetHandle: "tt",
          type: "bezier", animated: true,
          label: "spawn",
          style: { stroke: "#16a34a", strokeWidth: 2, strokeDasharray: "6 3", opacity: 0.9 },
          labelStyle:         { fontSize: 9, fill: "#16a34a", fontWeight: 700 },
          labelBgStyle:       { fill: "rgba(255,255,255,0.85)", fillOpacity: 0.85 },
          labelBgPadding:     [3, 1] as [number, number],
          labelBgBorderRadius: 3,
        });
      }
    }

    return list;
  }, [activeEdgeIds, agentMap]);

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
