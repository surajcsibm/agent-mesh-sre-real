"use client";
import React from "react";

import dynamic from "next/dynamic";
import { useMeshStream } from "./useMeshStream";
import type { ApprovalRequest, AuditRecord, MCPToolCall, AgentState } from "@/lib/types";
import type { EmailSummaryData, TopicChangePayload, TopicHealPayload } from "./useMeshStream";
import clsx from "clsx";
import { useSession, signOut } from "next-auth/react";
import { useState, useEffect, useRef } from "react";
import { useClusterStore, useClusterPolling, type ModeInfo } from "@/lib/cluster-status";
import type { BrokerState } from "@/lib/types";

const AgentCanvas = dynamic(() => import("./AgentCanvas"), { ssr: false });

// ── Scenario definitions ──────────────────────────────────────────────────────

// Top-4 always pinned; EXTRA_SCENARIOS scrolls below them
const PINNED_SCENARIOS = [
  { id: "lag-spike",           label: "Consumer Lag Spike",         badge: "KIP-848", color: "#2563eb" },
  { id: "controller-failover", label: "KRaft Controller Failover",  badge: "KRaft",   color: "#7c3aed" },
  { id: "share-group",         label: "Share Group Rebalance",      badge: "KIP-932", color: "#ea580c" },
  { id: "benign-rebalance",    label: "False-Positive Suppression", badge: "KIP-848", color: "#16a34a" },
] as const;

const EXTRA_SCENARIOS = [
  { id: "schema-mismatch",          label: "Schema Registry Mismatch",   badge: "Avro",    color: "#7c3aed" },
  { id: "disk-saturation",          label: "Broker Disk Saturation",      badge: "I/O",     color: "#dc2626" },
  { id: "under-replication",        label: "Under-Replicated Partitions", badge: "ISR",     color: "#b91c1c" },
  { id: "producer-timeout",         label: "Producer Timeout Storm",      badge: "Batch",   color: "#d97706" },
  { id: "consumer-session-timeout", label: "Consumer Session Timeout",    badge: "GC",      color: "#4f46e5" },
  { id: "compaction-lag",           label: "Log Compaction Lag",          badge: "Compact", color: "#0891b2" },
] as const;

// Keep backward compat for any code that still iterates SCENARIOS
const SCENARIOS = [...PINNED_SCENARIOS, ...EXTRA_SCENARIOS];

// ── Semantic colour maps ──────────────────────────────────────────────────────


// ── Monitor poll detection hook ───────────────────────────────────────────────
interface MonDetection {
  detected:   Set<string>;
  triggered:  Set<string>;
  suppressed: Set<string>;
  cycleCount: number;
  running:    boolean;
}

function emptyDet(): MonDetection {
  return { detected: new Set(), triggered: new Set(), suppressed: new Set(), cycleCount: 0, running: false };
}

function useMonitorDetection(): MonDetection {
  const [det, setDet] = React.useState<MonDetection>(emptyDet);
  React.useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const r = await fetch("/api/mesh/poll");
        if (!r.ok || !alive) return;
        const { poll } = await r.json();
        const detected = new Set<string>(), triggered = new Set<string>(), suppressed = new Set<string>();
        for (const d of (poll.detectedThisCycle ?? [])) {
          detected.add(d.scenarioId);
          (d.gate === "suppress" ? suppressed : triggered).add(d.scenarioId);
        }
        setDet({ detected, triggered, suppressed, cycleCount: poll.cycleCount ?? 0, running: poll.running ?? false });
      } catch { /* poll not ready */ }
    };
    refresh();
    const iv = setInterval(refresh, 5_000);
    return () => { alive = false; clearInterval(iv); };
  }, []);
  return det;
}

// Which Monitor scenarios signal a problem for a given topic?
function topicRelatedScenarios(t: { lagTotal: number; replicationFactor: number; msgPerSec: number; status: string }, det: MonDetection): string[] {
  const r: string[] = [];
  if (t.status !== "healthy") {
    if (t.lagTotal > 5_000 && det.detected.has("lag-spike"))                r.push("lag-spike");
    if (t.lagTotal > 3_000 && det.detected.has("consumer-session-timeout")) r.push("consumer-session-timeout");
    if (t.msgPerSec  < 20  && det.detected.has("producer-timeout"))         r.push("producer-timeout");
    if (t.replicationFactor < 3 && det.detected.has("under-replication"))   r.push("under-replication");
  }
  if (det.detected.has("disk-saturation"))  r.push("disk-saturation");
  if (det.detected.has("compaction-lag"))   r.push("compaction-lag");
  return r;
}

// Ring style for a scenario button based on detection state
function scenarioRing(id: string, det: MonDetection): React.CSSProperties {
  if (det.suppressed.has(id)) return { boxShadow: "0 0 0 1.5px #a78bfa55" };
  if (det.triggered.has(id))  return { boxShadow: "0 0 0 2px #fbbf2460, 0 0 14px #fbbf2430" };
  if (det.detected.has(id))   return { boxShadow: "0 0 0 1.5px #22d3ee55, 0 0 10px #22d3ee20" };
  return {};
}

// Small inline badge for detected scenarios
function DetBadge({ id, det }: { id: string; det: MonDetection }) {
  if (!det.detected.has(id)) return null;
  const [c, icon, label] = det.suppressed.has(id)
    ? ["#a78bfa", "⊘", "suppressed"]
    : det.triggered.has(id)
    ? ["#fbbf24", "⚡", "auto-fired"]
    : ["#22d3ee", "●", "detected"];
  return (
    <span style={{ background: c + "18", border: `1px solid ${c}40`, color: c }}
      className="text-[9.5px] font-mono px-1.5 py-0.5 rounded-md shrink-0 flex items-center gap-0.5">
      {icon} {label}
    </span>
  );
}

const POLL_COLOR = "#0e7490";
const AUDIT_COLOR: Record<string, string> = {
  publish:          "#2563eb",
  consume:          "#16a34a",
  reasoning:        "#7c3aed",
  "tool-call":      "#ea580c",
  approval:         "#d97706",
  lesson:           "#0891b2",
  notification:     "#db2777",
  "agent-kill":     "#dc2626",
  "agent-restart":  "#16a34a",
  "replay-start":   "#0891b2",
  "replay-complete":"#16a34a",
};

const MRAL_LABELS: Record<string, string> = {
  idle: "IDLE", monitor: "MONITOR", reason: "REASON",
  awaiting: "AWAITING", act: "ACT", learn: "LEARN", replaying: "REPLAYING",
};

const MRAL_BG: Record<string, string> = {
  idle:      "bg-slate-100  text-slate-500  border-slate-300",
  monitor:   "bg-blue-50    text-blue-700   border-blue-200",
  reason:    "bg-violet-50  text-violet-700 border-violet-200",
  awaiting:  "bg-amber-50   text-amber-700  border-amber-200",
  act:       "bg-orange-50  text-orange-700 border-orange-200",
  learn:     "bg-green-50   text-green-700  border-green-200",
  replaying: "bg-cyan-50    text-cyan-700   border-cyan-200",
};

const MRAL_DOT: Record<string, string> = {
  idle: "#94a3b8", monitor: "#2563eb", reason: "#7c3aed",
  awaiting: "#d97706", act: "#ea580c", learn: "#16a34a", replaying: "#0891b2",
};

// ── Arctic Clean theme tokens ─────────────────────────────────────────────────
// Page bg:    #f0f4f8  (light blue-gray)
// Surface:    #ffffff  (white cards)
// Sidebar bg: #f8fafc
// Nav:        #1e3a5f  (deep navy)
// Border:     #dce5ef
// Accent:     #1D9E75  (emerald)
// Text-1:     #1e3a5f  (navy)
// Text-2:     #64748b  (slate-500)

// ── Toast stack ───────────────────────────────────────────────────────────────

function ToastStack({ toasts }: { toasts: { id: number; message: string; kind: string }[] }) {
  const styles: Record<string, { bg: string; border: string; text: string; dot: string }> = {
    info:    { bg: "bg-sky-50",      border: "border-sky-200",    text: "text-sky-800",    dot: "bg-sky-500"    },
    success: { bg: "bg-emerald-50",  border: "border-emerald-200",text: "text-emerald-800",dot: "bg-emerald-500"},
    warning: { bg: "bg-amber-50",    border: "border-amber-200",  text: "text-amber-800",  dot: "bg-amber-500"  },
    error:   { bg: "bg-red-50",      border: "border-red-200",    text: "text-red-700",    dot: "bg-red-500"    },
  };
  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col-reverse gap-2 max-w-sm pointer-events-none">
      {toasts.map((t) => {
        const s = styles[t.kind] ?? styles.info;
        return (
          <div key={t.id}
            className={clsx(
              "flex items-start gap-3 rounded-xl px-4 py-3 shadow-xl border pointer-events-auto",
              "animate-[slideInRight_0.25s_ease-out]",
              s.bg, s.border
            )}>
            <div className={clsx("w-2 h-2 rounded-full mt-1.5 shrink-0 animate-pulse", s.dot)} />
            <span className={clsx("text-sm font-medium leading-snug", s.text)}>{t.message}</span>
          </div>
        );
      })}
    </div>
  );
}

// ── Live activity banner ──────────────────────────────────────────────────────
// Slides in below the nav whenever any agent is actively processing.

const AGENT_COLOR: Record<string, string> = {
  intake: "#0ea5e9", monitor: "#a78bfa", writer: "#34d399", notification: "#fbbf24",
};

const PHASE_DESC: Partial<Record<AgentState["status"], string>> = {
  reasoning:          "is analyzing the incident",
  acting:             "is executing an action",
  "awaiting-approval":"is waiting for your approval",
  learning:           "is recording a lesson",
};

function LiveActivityBanner({ agents }: { agents: AgentState[] }) {
  const active = agents.find((a) =>
    (["reasoning", "acting", "awaiting-approval", "learning"] as AgentState["status"][]).includes(a.status)
  );
  if (!active) return null;

  const color = AGENT_COLOR[active.id] ?? "#60a5fa";
  const desc  = PHASE_DESC[active.status] ?? active.status;

  // Pick the most informative detail line
  let detail: string | null = null;
  if (active.status === "reasoning" && active.lastReasoning) {
    const pct = Math.round(active.lastReasoning.confidence * 100);
    detail = `${active.lastReasoning.rootCause} · ${pct}% confidence`;
  } else if (active.status === "acting" && active.lastAction) {
    detail = active.lastAction.detail;
  } else if (active.status === "learning" && active.lastLesson) {
    detail = active.lastLesson.notes?.slice(0, 90) ?? null;
  }

  return (
    <div className="mx-4 mt-2 mb-1 transition-all animate-[slideDown_0.3s_ease-out]">
      <div
        className="flex items-center gap-3 rounded-xl border px-4 py-2.5 shadow-sm"
        style={{ borderColor: color + "44", background: color + "0d" }}
      >
        <span className="w-2 h-2 rounded-full animate-pulse shrink-0" style={{ background: color }} />
        <span className="text-sm font-bold" style={{ color }}>{active.name}</span>
        <span className="text-sm text-slate-600">{desc}</span>
        {detail && (
          <>
            <span className="text-slate-300 shrink-0">·</span>
            <span className="text-xs text-[#64748b] truncate flex-1">{detail}</span>
          </>
        )}
        {active.status === "awaiting-approval" && (
          <span className="ml-auto shrink-0 text-xs font-bold text-amber-700 bg-amber-50
                           border border-amber-300 rounded-full px-2.5 py-0.5 animate-pulse">
            ↓ Review below
          </span>
        )}
      </div>
    </div>
  );
}

// ── Human-readable tool-call description ─────────────────────────────────────

function describeToolCall(toolCall: MCPToolCall) {
  const name = toolCall.params.name;
  const args = toolCall.params.arguments as Record<string, unknown>;

  switch (name) {
    case "kafka.scaleConsumers":
      return {
        emoji: "⬆️",
        title: "Scale Consumer Group",
        summary: `Add ${args.delta} consumer replica${Number(args.delta) > 1 ? "s" : ""} to "${args.group}"`,
        rows: [
          ["Consumer group",   String(args.group)],
          ["Replicas to add",  `+${args.delta} consumers`],
          ["Reason",           String(args.reason ?? "Lag spike detected")],
        ] as [string, string][],
        impact: "Temporary infra change · Auto-reverts when lag clears",
        impactColor: "amber",
      };
    case "kafka.checkpointShareGroup":
      return {
        emoji: "📌",
        title: "Checkpoint Share Group",
        summary: `Checkpoint KIP-932 share group "${args.shareGroupId}"`,
        rows: [
          ["Share group ID",   String(args.shareGroupId)],
          ...(args.delta ? [["Consumer delta", `+${args.delta}`] as [string, string]] : []),
          ...(args.checkpointOffset ? [["Checkpoint offset", String(args.checkpointOffset)] as [string, string]] : []),
        ] as [string, string][],
        impact: "Offset committed · No partition reassignment needed",
        impactColor: "blue",
      };
    case "kafka.suppressRebalancePage":
      return {
        emoji: "🔇",
        title: "Suppress Rebalance Alert",
        summary: `Suppress page for consumer group "${args.consumerGroup}" during rebalance`,
        rows: [
          ["Consumer group",  String(args.consumerGroup)],
          ["State",           String(args.rebalanceState ?? "Rebalancing")],
          ...(args.lagObserved ? [["Observed lag", String(args.lagObserved)] as [string, string]] : []),
        ] as [string, string][],
        impact: "Read-only · No cluster mutation, audit record only",
        impactColor: "green",
      };
    default:
      return {
        emoji: "⚙️",
        title: name.replace(/\./g, " › "),
        summary: "Execute infrastructure action",
        rows: Object.entries(args).map(([k, v]) => [k, String(v)]) as [string, string][],
        impact: "Infrastructure mutation",
        impactColor: "amber",
      };
  }
}

// ── Approval gate ─────────────────────────────────────────────────────────────

function ApprovalGate({ approvals, onDecide }: {
  approvals: ApprovalRequest[];
  onDecide: (id: string, d: "approve" | "reject") => void;
}) {
  if (!approvals.length) return null;

  return (
    <div className="fixed inset-0 z-40 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4
                    animate-[fadeIn_0.2s_ease-out]">
      {approvals.map((a) => {
        const desc = describeToolCall(a.toolCall);
        return (
          <div key={a.id}
            className="bg-white border border-amber-200 rounded-2xl shadow-2xl max-w-lg w-full overflow-hidden
                       animate-[slideUp_0.25s_ease-out]">

            {/* Header */}
            <div className="bg-amber-50 border-b border-amber-200 px-6 py-4 flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-white border border-amber-200 flex items-center justify-center text-xl shadow-sm">
                🔐
              </div>
              <div>
                <h2 className="text-base font-bold text-slate-800">Policy Gate — Approval Required</h2>
                <p className="text-xs text-amber-700 font-medium mt-0.5">
                  This action mutates your Kafka cluster. Review carefully before approving.
                </p>
              </div>
            </div>

            {/* Body */}
            <div className="px-6 py-5">
              {/* Proposed action */}
              <div className="flex items-center gap-2 mb-4">
                <span className="text-2xl">{desc.emoji}</span>
                <div>
                  <div className="text-sm font-bold text-slate-800">{desc.title}</div>
                  <div className="text-xs text-slate-500 mt-0.5">{desc.summary}</div>
                </div>
              </div>

              {/* Parameters table */}
              <div className="rounded-xl border border-slate-200 overflow-hidden mb-4">
                <table className="w-full text-sm">
                  <tbody>
                    {desc.rows.map(([label, value], i) => (
                      <tr key={i} className={i < desc.rows.length - 1 ? "border-b border-slate-100" : ""}>
                        <td className="px-4 py-2.5 text-xs text-slate-500 font-medium w-36 bg-slate-50">{label}</td>
                        <td className="px-4 py-2.5 text-xs font-semibold text-slate-800 font-mono">{value}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Reason / rationale */}
              {a.reason && (
                <div className="bg-blue-50 border border-blue-200 rounded-xl px-4 py-3 mb-4">
                  <div className="text-[10px] font-bold text-blue-500 uppercase tracking-wider mb-1">Why this action</div>
                  <div className="text-sm text-blue-900 leading-relaxed">{a.reason}</div>
                </div>
              )}

              {/* Impact note */}
              <div className={clsx(
                "rounded-xl px-4 py-2.5 text-xs font-medium mb-5",
                desc.impactColor === "green"
                  ? "bg-green-50 border border-green-200 text-green-700"
                  : desc.impactColor === "blue"
                  ? "bg-blue-50 border border-blue-200 text-blue-700"
                  : "bg-amber-50 border border-amber-200 text-amber-700"
              )}>
                ⚡ {desc.impact}
              </div>

              {/* Buttons */}
              <div className="flex gap-3">
                <button onClick={() => onDecide(a.id, "approve")}
                  className="flex-1 py-3 rounded-xl bg-emerald-500 hover:bg-emerald-600
                             text-white font-bold text-sm transition-colors shadow-sm">
                  ✓ Approve
                </button>
                <button onClick={() => onDecide(a.id, "reject")}
                  className="flex-1 py-3 rounded-xl bg-white hover:bg-red-50
                             text-red-600 font-bold text-sm transition-colors border-2 border-red-200 hover:border-red-400">
                  ✕ Reject — No Action
                </button>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Scenario-end modal — mirrors the email template visually ─────────────────

function DataRow({ label, children, last = false }: { label: string; children: React.ReactNode; last?: boolean }) {
  return (
    <tr className={last ? "" : "border-b border-[#e2e8f0]"}>
      <td style={{ width: 148, padding: "9px 16px", fontSize: 12, color: "#64748b", fontWeight: 600, background: "#f8fafc", verticalAlign: "top" }}>
        {label}
      </td>
      <td style={{ padding: "9px 16px", fontSize: 13, color: "#1e293b", lineHeight: 1.5 }}>
        {children}
      </td>
    </tr>
  );
}

function ScenarioEndModal({ data, onClose }: { data: EmailSummaryData; onClose: () => void }) {
  const isRejected = !data.approved;
  const ts = new Date().toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" });
  const confidence = data.reasoning ? `${Math.round(data.reasoning.confidence * 100)}%` : "—";

  const MRAL_PHASES = [
    { label: "Monitor", color: "#3b82f6" },
    { label: "Reason",  color: "#8b5cf6" },
    { label: "Act",     color: "#f97316" },
    { label: "Learn",   color: "#22c55e" },
  ];

  const emailStatus = data.sent
    ? "✉️ Sent to Admin/Stakeholders"
    : data.emailError === "smtp_not_configured"
    ? "⚠️ Skipped — SMTP not set in Vercel"
    : data.emailError === "network_error"
    ? "⚠️ Failed — network error"
    : `⚠️ Failed — ${data.emailError ?? "unknown"}`;

  return (
    <div className="fixed inset-0 z-50 bg-black/55 backdrop-blur-sm flex items-center justify-center p-3
                    animate-[fadeIn_0.2s_ease-out] overflow-y-auto">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-[600px] overflow-hidden my-4
                      animate-[slideUp_0.25s_ease-out]"
           style={{ fontFamily: "'Segoe UI', Arial, sans-serif" }}>

        {/* ── Header — matches email gradient ── */}
        <div style={{ background: "linear-gradient(135deg,#1e3a8a 0%,#3b82f6 100%)", padding: "24px 28px" }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: "#fff", letterSpacing: -0.3 }}>
            🤖 Agent Mesh SRE — Incident Summary
          </div>
          <div style={{ fontSize: 12, color: "#bfdbfe", marginTop: 5 }}>
            Scenario: <strong style={{ color: "#fff" }}>{data.scenarioLabel}</strong>
            &nbsp;·&nbsp;{ts}
            {isRejected && (
              <span style={{ marginLeft: 8, background: "#fca5a5", color: "#7f1d1d", padding: "2px 8px",
                             borderRadius: 20, fontSize: 11, fontWeight: 700 }}>REJECTED</span>
            )}
          </div>
        </div>

        {/* ── MRAL badges ── */}
        <div style={{ padding: "14px 28px 0", display: "flex", gap: 6, flexWrap: "wrap" }}>
          {MRAL_PHASES.map((p) => (
            <span key={p.label} style={{
              background: p.color + "18", color: p.color,
              border: `1px solid ${p.color}40`, borderRadius: 20,
              padding: "3px 12px", fontSize: 11, fontWeight: 700, letterSpacing: "0.5px",
            }}>{p.label}</span>
          ))}
        </div>

        <div style={{ padding: "0 28px 20px" }}>

          {/* ── Reasoning ── */}
          <div style={{ marginTop: 18 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#475569", textTransform: "uppercase",
                          letterSpacing: "0.8px", marginBottom: 8 }}>🧠 Monitor → Reason</div>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #e2e8f0", borderRadius: 8, overflow: "hidden", fontSize: 13 }}>
              <tbody>
                <DataRow label="Root cause">{data.reasoning?.rootCause ?? "—"}</DataRow>
                <DataRow label="Kafka feature">
                  <span style={{ background: "#dbeafe", color: "#1d4ed8", padding: "1px 8px",
                                 borderRadius: 4, fontSize: 11, fontWeight: 700 }}>
                    {data.reasoning?.kafkaFeature ?? "—"}
                  </span>
                </DataRow>
                <DataRow label="Confidence"><strong>{confidence}</strong></DataRow>
                <DataRow label="Rationale" last>{data.reasoning?.rationale ?? "—"}</DataRow>
              </tbody>
            </table>
          </div>

          {/* ── Act ── */}
          <div style={{ marginTop: 18 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#475569", textTransform: "uppercase",
                          letterSpacing: "0.8px", marginBottom: 8 }}>⚡ Act</div>
            <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #e2e8f0", borderRadius: 8, overflow: "hidden", fontSize: 13 }}>
              <tbody>
                <DataRow label="Action taken">
                  <code style={{ fontSize: 12, color: "#0f172a" }}>{data.action}</code>
                </DataRow>
                <DataRow label="Outcome">
                  {isRejected
                    ? <span style={{ background: "#fef2f2", color: "#dc2626", border: "1px solid #fca5a5",
                                     borderRadius: 20, padding: "2px 10px", fontSize: 12, fontWeight: 700 }}>🚫 REJECTED</span>
                    : <span style={{ background: "#dcfce7", color: "#16a34a", border: "1px solid #86efac",
                                     borderRadius: 20, padding: "2px 10px", fontSize: 12, fontWeight: 700 }}>✅ SUCCESS</span>
                  }
                </DataRow>
                {!isRejected && data.lagBefore > 0 && (
                  <DataRow label="Lag resolved">
                    <strong>{data.lagBefore.toLocaleString()} → {data.lagAfter.toLocaleString()} messages</strong>
                  </DataRow>
                )}
                <DataRow label={isRejected ? "Rejected by" : "Approved by"} last>
                  {data.approvedBy ?? "operator"}
                </DataRow>
              </tbody>
            </table>
          </div>

          {/* ── Learn ── */}
          <div style={{ marginTop: 18 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#475569", textTransform: "uppercase",
                          letterSpacing: "0.8px", marginBottom: 8 }}>📚 Learn</div>
            <div style={{ background: "#f8fafc", border: "1px solid #e2e8f0", borderRadius: 8, padding: "12px 14px" }}>
              <div style={{ fontSize: 13, color: "#334155", lineHeight: 1.6 }}>
                {isRejected
                  ? "No lesson recorded — action was rejected by operator. Cluster was not modified."
                  : (data.lesson?.notes ?? "No lesson recorded.")}
              </div>
              {!isRejected && data.lesson?.adjustedThreshold && (
                <div style={{ fontSize: 12, color: "#3b82f6", marginTop: 5, fontWeight: 600 }}>
                  Adjusted threshold → {data.lesson.adjustedThreshold.toLocaleString()} msgs
                </div>
              )}
            </div>
          </div>

          {/* ── Notifications — removed; review via Scenario History bar ── */}{/* ── Live Events Timeline ── */}
          {data.liveEvents && data.liveEvents.length > 0 && (
            <div style={{ marginTop: 18 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: "#475569", textTransform: "uppercase",
                            letterSpacing: "0.8px", marginBottom: 8 }}>📡 Live Events Timeline</div>
              <table style={{ width: "100%", borderCollapse: "collapse", border: "1px solid #e2e8f0", borderRadius: 8, overflow: "hidden", fontSize: 12 }}>
                <thead>
                  <tr style={{ background: "#f8fafc", borderBottom: "1px solid #e2e8f0" }}>
                    <th style={{ textAlign: "left", padding: "7px 12px", color: "#64748b", fontWeight: 600, width: 80 }}>Event</th>
                    <th style={{ textAlign: "left", padding: "7px 12px", color: "#64748b", fontWeight: 600, width: 90 }}>Agent</th>
                    <th style={{ textAlign: "left", padding: "7px 12px", color: "#64748b", fontWeight: 600 }}>Summary</th>
                  </tr>
                </thead>
                <tbody>
                  {data.liveEvents.map((ev, i) => {
                    const color = AUDIT_COLOR[ev.type] ?? "#94a3b8";
                    return (
                      <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#f8fafc", borderBottom: i < data.liveEvents!.length - 1 ? "1px solid #f1f5f9" : "none" }}>
                        <td style={{ padding: "7px 12px" }}>
                          <span style={{ background: color + "18", color, border: `1px solid ${color}30`, borderRadius: 4, padding: "2px 7px", fontSize: 10, fontWeight: 700 }}>
                            {ev.type}
                          </span>
                        </td>
                        <td style={{ padding: "7px 12px", color: "#64748b", fontSize: 11 }}>[{ev.agent}]</td>
                        <td style={{ padding: "7px 12px", color: "#334155", lineHeight: 1.5 }}>{ev.summary}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {/* ── Footer strip ── */}
          <div style={{ marginTop: 16, paddingTop: 14, borderTop: "1px solid #e2e8f0",
                        fontSize: 11, color: "#94a3b8", lineHeight: 1.6 }}>
            This summary was automatically sent to <strong>Admin / Stakeholders</strong>
            {" "}by the Agent Mesh SRE Notification Agent.
            &nbsp;·&nbsp;
            <span className={data.sent ? "text-emerald-600 font-medium" : "text-amber-600 font-medium"}
                  style={{ fontSize: 11 }}>{emailStatus}</span>
          </div>
          {!data.sent && data.emailError === "smtp_not_configured" && (
            <div className="mt-2 bg-amber-50 border border-amber-200 rounded-xl px-4 py-2.5 text-xs text-amber-700">
              <strong>Enable emails:</strong> add SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
              NOTIFICATION_EMAIL in Vercel → Settings → Environment Variables → Redeploy.
            </div>
          )}

          <button onClick={onClose}
            className="mt-5 w-full py-3 rounded-xl font-bold text-sm transition-colors shadow-sm"
            style={{ background: isRejected ? "#1e293b" : "#2563eb", color: "#fff" }}>
            {isRejected ? "Understood — no action taken" : "Got it"}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── User menu ─────────────────────────────────────────────────────────────────

function UserMenu() {
  const { data: session } = useSession();
  const [open, setOpen] = useState(false);

  if (!session?.user) return null;

  const name     = session.user.name  ?? "User";
  const email    = session.user.email ?? "";
  const image    = session.user.image;
  const initials = name.split(" ").map((n) => n[0]).join("").slice(0, 2).toUpperCase();

  return (
    <div className="relative">
      <button onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-2 rounded-full pl-1.5 pr-3 py-1 transition-colors"
        style={{ border: "1px solid rgba(255,255,255,0.25)", background: "rgba(255,255,255,0.12)" }}
        onMouseEnter={e => (e.currentTarget.style.background = "rgba(255,255,255,0.22)")}
        onMouseLeave={e => (e.currentTarget.style.background = "rgba(255,255,255,0.12)")}>
        {image
          ? <img src={image} alt={name} referrerPolicy="no-referrer" className="w-6 h-6 rounded-full" />
          : <div className="w-6 h-6 rounded-full flex items-center justify-center text-[9px] font-bold text-white"
               style={{ background: "#1D9E75" }}>
              {initials}
            </div>
        }
        <span className="text-xs text-white/90 font-medium max-w-[100px] truncate hidden sm:block">{name}</span>
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1.5 w-56 rounded-xl bg-white shadow-xl z-50"
             style={{ border: "1px solid #dce5ef" }}>
          <div className="px-4 py-3" style={{ borderBottom: "1px solid #f0f4f8" }}>
            <div className="text-xs font-semibold truncate" style={{ color: "#1e3a5f" }}>{name}</div>
            <div className="text-[10px] truncate" style={{ color: "#94a3b8" }}>{email}</div>
          </div>
          <button onClick={() => signOut({ callbackUrl: "/login" })}
            className="w-full text-left px-4 py-2.5 text-xs rounded-b-xl transition-colors"
            style={{ color: "#64748b" }}
            onMouseEnter={e => { (e.currentTarget.style.background = "#f0f4f8"); (e.currentTarget.style.color = "#1e3a5f"); }}
            onMouseLeave={e => { (e.currentTarget.style.background = ""); (e.currentTarget.style.color = "#64748b"); }}>
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}

// ── Audit log panel ───────────────────────────────────────────────────────────

function AuditLogPanel({ log }: { log: AuditRecord[] }) {
  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-1.5 mb-3 px-1">
        <span className="w-2 h-2 rounded-full shrink-0" style={{ background: "#2563eb" }} />
        <span className="text-xs font-bold uppercase tracking-widest" style={{ color: "#1e3a5f", opacity: 0.7 }}>
          Audit Log
        </span>
        <span className="text-xs font-normal ml-1" style={{ color: "#94a3b8" }}>({log.length})</span>
      </div>
      <div className="flex-1 overflow-y-auto space-y-2 pr-0.5">
        {[...log].reverse().map((r) => (
          <div key={r.id}
            className="rounded-xl bg-white border border-[#e8eef4] px-3 py-3"
            style={{ borderLeftWidth: 3, borderLeftColor: r.summary?.startsWith("[POLL]") ? POLL_COLOR : (AUDIT_COLOR[r.type] ?? "#94a3b8") }}>
            <div className="flex items-center gap-1.5 mb-1.5 flex-wrap">
              <span className="text-[10px] font-bold uppercase tracking-wide px-1.5 py-0.5 rounded shrink-0"
                style={{
                  background: (r.summary?.startsWith("[POLL]") ? POLL_COLOR : (AUDIT_COLOR[r.type] ?? "#94a3b8")) + "18",
                  color: r.summary?.startsWith("[POLL]") ? POLL_COLOR : (AUDIT_COLOR[r.type] ?? "#94a3b8"),
                  border: `1px solid ${(r.summary?.startsWith("[POLL]") ? POLL_COLOR : (AUDIT_COLOR[r.type] ?? "#94a3b8"))}30`,
                }}>
                {r.summary?.startsWith("[POLL]") ? "autopoll" : r.type}
              </span>
              <span className="text-[11px] font-semibold text-[#94a3b8] shrink-0">[{r.agent}]</span>
            </div>
            <div className="text-xs text-[#334155] leading-relaxed break-words whitespace-normal font-medium">
              {r.summary}
            </div>
          </div>
        ))}
        {log.length === 0 && (
          <div className="flex flex-col items-center justify-center pt-8 gap-2">
            <span style={{ fontSize: 28, opacity: 0.2 }}>📋</span>
            <p className="text-xs italic" style={{ color: "#94a3b8" }}>No events yet — trigger a scenario.</p>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Agent live feed — vertical scrollable panel below the canvas ──────────────
// Shows all recent audit events as stacked cards (newest first).
// Visible whenever a scenario is running (hidden when summary modal is up).

const KEY_EVENT_TYPES = new Set([
  "reasoning", "tool-call", "approval", "lesson", "notification",
]);

function AgentLiveFeed({ log, agents, running, hidden }: {
  log: AuditRecord[];
  agents: AgentState[];
  running: boolean;
  hidden: boolean;
}) {
  const active = agents.find((a) =>
    (["reasoning", "acting", "awaiting-approval", "learning"] as AgentState["status"][]).includes(a.status)
  );
  const agentColor = active ? (AGENT_COLOR[active.id] ?? "#60a5fa") : "#1D9E75";
  const desc       = active ? (PHASE_DESC[active.status] ?? active.status) : null;

  // Show all audit events, most recent first, up to 40
  const recent = [...log].reverse().slice(0, 40);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center gap-2 px-3 py-2 shrink-0"
           style={{ borderBottom: "1px solid #dce5ef", background: "#f8fafc" }}>
        <span className="w-1.5 h-1.5 rounded-full shrink-0 animate-pulse"
              style={{ background: running && !hidden ? "#1D9E75" : "#94a3b8" }} />
        <span className="text-[10px] font-bold uppercase tracking-widest" style={{ color: "#1e3a5f" }}>
          Live Events Feed
        </span>
        {running && !hidden && active && (
          <>
            <span style={{ color: "#dce5ef", fontSize: 10 }}>·</span>
            <span className="w-1.5 h-1.5 rounded-full shrink-0 animate-pulse"
                  style={{ background: agentColor }} />
            <span className="text-[10px] font-bold shrink-0" style={{ color: agentColor }}>
              {active.name}
            </span>
            {desc && (
              <span className="text-[9px] shrink-0" style={{ color: "#94a3b8" }}>{desc}</span>
            )}
          </>
        )}
        <span className="ml-auto text-[9px]" style={{ color: "#94a3b8" }}>
          {log.length} events
        </span>
      </div>

      {/* Scrollable vertical event list */}
      <div className="flex-1 overflow-y-auto p-2 space-y-1.5">
        {recent.length === 0 ? (
          <p className="text-xs italic text-center pt-3" style={{ color: "#94a3b8" }}>
            Trigger a scenario to see live events…
          </p>
        ) : recent.map((r) => {
          const color = AUDIT_COLOR[r.type] ?? "#94a3b8";
          return (
            <div key={r.id}
              className="flex items-start gap-2 rounded-lg px-2.5 py-2 border"
              style={{ background: color + "06", borderColor: color + "22" }}>
              <span className="shrink-0 text-[9px] font-bold uppercase tracking-wide px-1.5 py-0.5 rounded mt-0.5"
                style={{ background: color + "18", color, border: `1px solid ${color}30` }}>
                {r.type}
              </span>
              <div className="flex-1 min-w-0">
                <span className="text-[9px] font-semibold mr-1" style={{ color: "#94a3b8" }}>
                  [{r.agent}]
                </span>
                <span className="text-[10px] leading-snug break-words" style={{ color: "#475569" }}>
                  {r.summary}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Canvas topics grid — shows top N topics as compact cards ─────────────────

function CanvasTopicsGrid({
  topics, topicsVisible, onShowMore,
}: {
  topics: KafkaTopic[];
  topicsVisible: number;
  onShowMore: () => void;
}) {
  const sorted = [...topics].sort((a, b) => {
    const order = { critical: 0, degraded: 1, healthy: 2 };
    const sd = order[a.status] - order[b.status];
    return sd !== 0 ? sd : b.lagTotal - a.lagTotal;
  });
  const visible = sorted.slice(0, topicsVisible);
  const remaining = sorted.length - topicsVisible;

  return (
    <div className="p-3">
      {/* Section header */}
      <div className="flex items-center justify-between mb-2.5">
        <span className="text-[10px] font-bold uppercase tracking-widest"
              style={{ color: "#1e3a5f", opacity: 0.55 }}>
          Kafka Topics
          <span className="font-normal normal-case ml-1.5" style={{ color: "#94a3b8" }}>
            ({topics.length} total · showing {visible.length})
          </span>
        </span>
        <span className="text-[9px]" style={{ color: "#94a3b8" }}>
          {topics.filter(t => t.status === "critical").length > 0 && (
            <span className="text-red-500 font-bold mr-2">
              {topics.filter(t => t.status === "critical").length} critical
            </span>
          )}
          {topics.filter(t => t.status === "degraded").length > 0 && (
            <span className="text-amber-500 font-bold">
              {topics.filter(t => t.status === "degraded").length} degraded
            </span>
          )}
        </span>
      </div>

      {/* Topic cards grid */}
      <div className="grid gap-2" style={{ gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))" }}>
        {visible.map(t => {
          const st  = TOPIC_STATUS_STYLE[t.status];
          const lag = t.lagTotal > 9999 ? `${(t.lagTotal / 1000).toFixed(0)}k` :
                      t.lagTotal > 999  ? `${(t.lagTotal / 1000).toFixed(1)}k` :
                      String(t.lagTotal);
          return (
            <div key={t.id}
              className="rounded-xl border px-3 py-2.5 transition-all"
              style={{ background: "#fff", borderColor: "#dce5ef" }}>
              {/* Status + name */}
              <div className="flex items-center gap-1.5 mb-1">
                <span className={`w-1.5 h-1.5 rounded-full shrink-0 animate-pulse ${st.dot}`} />
                <span className="text-[10px] font-bold truncate" style={{ color: "#1e3a5f" }}>
                  {t.name.split(".").slice(-2).join(".")}
                </span>
              </div>
              {/* Metrics */}
              <div className="flex items-center gap-2 text-[9px]" style={{ color: "#94a3b8" }}>
                <span>{t.partitions}p</span>
                <span style={{ color: "#e2e8f0" }}>·</span>
                <span className={
                  t.lagTotal > 5000 ? "text-red-500 font-bold" :
                  t.lagTotal > 1000 ? "text-amber-500 font-bold" : ""
                }>{lag}</span>
                <span style={{ color: "#e2e8f0" }}>·</span>
                <span>{t.msgPerSec}/s</span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Show more button */}
      {remaining > 0 && (
        <button
          onClick={onShowMore}
          className="mt-3 w-full py-2 rounded-xl text-[10px] font-bold transition-colors"
          style={{ background: "#f0f4f8", color: "#64748b", border: "1px solid #dce5ef" }}
          onMouseEnter={e => (e.currentTarget.style.background = "#dce5ef")}
          onMouseLeave={e => (e.currentTarget.style.background = "#f0f4f8")}
        >
          Show {Math.min(20, remaining)} more topics ↓
        </button>
      )}
    </div>
  );
}

// ── Kafka Topic types & seed data ─────────────────────────────────────────────

interface KafkaTopic {
  id: string;
  name: string;
  partitions: number;
  replicationFactor: number;
  retentionHours: number;
  lagTotal: number;
  msgPerSec: number;
  status: "healthy" | "degraded" | "critical";
  consumerGroups: string[];
  description: string;
  createdAt: number;  // Unix ms — used for newest-first sort
}

// Seed topics get staggered createdAt so they sort newest-last by default
const _NOW = Date.now();
const INITIAL_TOPICS: KafkaTopic[] = [
  {
    id: "t1", name: "payments.transactions.v1", partitions: 12, replicationFactor: 3,
    retentionHours: 72, lagTotal: 1240, msgPerSec: 820, status: "healthy",
    consumerGroups: ["payment-processor", "fraud-detector", "audit-writer"],
    description: "Core payment transaction events from POS, online, and API channels.",
    createdAt: _NOW - 8 * 60000,
  },
  {
    id: "t2", name: "payments.fraud.alerts.v1", partitions: 6, replicationFactor: 3,
    retentionHours: 168, lagTotal: 0, msgPerSec: 43, status: "healthy",
    consumerGroups: ["fraud-review-svc", "risk-engine"],
    description: "Real-time fraud alert events generated by the ML fraud model.",
    createdAt: _NOW - 7 * 60000,
  },
  {
    id: "t3", name: "invoices.created.v1", partitions: 8, replicationFactor: 3,
    retentionHours: 48, lagTotal: 320, msgPerSec: 210, status: "healthy",
    consumerGroups: ["invoice-renderer", "email-notifier", "erp-sync"],
    description: "Invoice creation events emitted when a new invoice is generated.",
    createdAt: _NOW - 6 * 60000,
  },
  {
    id: "t4", name: "invoices.paid.v1", partitions: 6, replicationFactor: 3,
    retentionHours: 48, lagTotal: 0, msgPerSec: 180, status: "healthy",
    consumerGroups: ["reconciliation-svc", "ledger-updater"],
    description: "Payment-confirmed events for invoices; triggers ledger reconciliation.",
    createdAt: _NOW - 5 * 60000,
  },
  {
    id: "t5", name: "invoices.overdue.v1", partitions: 4, replicationFactor: 2,
    retentionHours: 336, lagTotal: 4500, msgPerSec: 12, status: "degraded",
    consumerGroups: ["collections-agent", "crm-updater"],
    description: "Overdue invoice notifications; high lag indicates collections backlog.",
    createdAt: _NOW - 4 * 60000,
  },
  {
    id: "t6", name: "payments.refunds.v1", partitions: 6, replicationFactor: 3,
    retentionHours: 72, lagTotal: 0, msgPerSec: 55, status: "healthy",
    consumerGroups: ["refund-processor", "customer-notifier"],
    description: "Refund initiation and completion events across all payment methods.",
    createdAt: _NOW - 3 * 60000,
  },
  {
    id: "t7", name: "payments.settlements.v1", partitions: 4, replicationFactor: 3,
    retentionHours: 168, lagTotal: 18900, msgPerSec: 8, status: "critical",
    consumerGroups: ["settlement-engine"],
    description: "End-of-day settlement batches; critical lag may delay bank transfers.",
    createdAt: _NOW - 2 * 60000,
  },
  {
    id: "t8", name: "audit.payment.events.v1", partitions: 16, replicationFactor: 3,
    retentionHours: 720, lagTotal: 210, msgPerSec: 1200, status: "healthy",
    consumerGroups: ["audit-archiver", "compliance-reporter", "siem-forwarder"],
    description: "Immutable audit trail for all payment events; 30-day retention for compliance.",
    createdAt: _NOW - 1 * 60000,
  },
];

const TOPIC_STATUS_STYLE: Record<KafkaTopic["status"], { dot: string; text: string; bg: string; border: string }> = {
  healthy:  { dot: "bg-emerald-400", text: "text-emerald-700", bg: "bg-emerald-50", border: "border-emerald-200" },
  degraded: { dot: "bg-amber-400",   text: "text-amber-700",   bg: "bg-amber-50",   border: "border-amber-200"   },
  critical: { dot: "bg-red-500",     text: "text-red-700",     bg: "bg-red-50",     border: "border-red-200"     },
};

// Auto-create topics when a scenario runs — each scenario seeds its own relevant topics
const SCENARIO_AUTO_TOPICS: Record<string, Omit<KafkaTopic, "id" | "createdAt">[]> = {
  "lag-spike": [
    { name: "ops.kafka.metrics.v1", partitions: 6, replicationFactor: 3, retentionHours: 24,
      lagTotal: 18500, msgPerSec: 420, status: "critical",
      consumerGroups: ["payments-consumer", "sre-monitor"],
      description: "Kafka consumer group metrics — high lag detected on payments-consumer." },
    { name: "ops.requests.v1", partitions: 3, replicationFactor: 3, retentionHours: 24,
      lagTotal: 340, msgPerSec: 85, status: "healthy",
      consumerGroups: ["sre-monitor"],
      description: "Incoming SRE operation requests." },
  ],
  "controller-failover": [
    { name: "ops.incidents.v1", partitions: 3, replicationFactor: 3, retentionHours: 72,
      lagTotal: 0, msgPerSec: 12, status: "healthy",
      consumerGroups: ["monitor-agent", "writer-agent"],
      description: "KRaft controller incident events — tracks failover and re-election." },
    { name: "ops.kafka.metrics.v1", partitions: 6, replicationFactor: 3, retentionHours: 24,
      lagTotal: 5200, msgPerSec: 210, status: "degraded",
      consumerGroups: ["sre-monitor"],
      description: "Kafka broker metrics — elevated lag during controller re-election." },
  ],
  "share-group": [
    { name: "share.group.events.v1", partitions: 12, replicationFactor: 3, retentionHours: 48,
      lagTotal: 24000, msgPerSec: 1800, status: "critical",
      consumerGroups: ["share-group-consumer-a", "share-group-consumer-b"],
      description: "KIP-932 share group partition events — storm detected during rebalance." },
    { name: "ops.kafka.metrics.v1", partitions: 6, replicationFactor: 3, retentionHours: 24,
      lagTotal: 8100, msgPerSec: 630, status: "critical",
      consumerGroups: ["sre-monitor"],
      description: "Kafka broker metrics — high churn from share-group rebalance." },
  ],
  "benign-rebalance": [
    { name: "ops.kafka.metrics.v1", partitions: 6, replicationFactor: 3, retentionHours: 24,
      lagTotal: 1200, msgPerSec: 310, status: "degraded",
      consumerGroups: ["payments-consumer", "sre-monitor"],
      description: "Kafka metrics — transient lag spike flagged as false-positive." },
  ],
  "schema-mismatch": [
    { name: "schema.registry.events.v1", partitions: 4, replicationFactor: 3, retentionHours: 168,
      lagTotal: 3700, msgPerSec: 95, status: "degraded",
      consumerGroups: ["avro-consumer", "schema-validator"],
      description: "Schema Registry events — version mismatch between producer v2 and consumer v1." },
  ],
  "disk-saturation": [
    { name: "ops.broker.disk.v1", partitions: 3, replicationFactor: 3, retentionHours: 24,
      lagTotal: 0, msgPerSec: 28, status: "healthy",
      consumerGroups: ["disk-monitor"],
      description: "Broker disk utilisation metrics — broker-2 approaching 95% capacity." },
    { name: "audit.payment.events.v1", partitions: 16, replicationFactor: 3, retentionHours: 720,
      lagTotal: 41000, msgPerSec: 2100, status: "critical",
      consumerGroups: ["audit-archiver", "compliance-reporter"],
      description: "Audit trail topic — high-volume writes contributing to disk saturation." },
  ],
  "under-replication": [
    { name: "payments.transactions.v1", partitions: 12, replicationFactor: 3, retentionHours: 72,
      lagTotal: 0, msgPerSec: 820, status: "degraded",
      consumerGroups: ["payment-processor", "fraud-detector"],
      description: "Payment transactions — under-replicated: ISR below replication factor." },
    { name: "payments.settlements.v1", partitions: 4, replicationFactor: 3, retentionHours: 168,
      lagTotal: 18900, msgPerSec: 8, status: "critical",
      consumerGroups: ["settlement-engine"],
      description: "Settlements — under-replicated partitions risk data loss." },
  ],
  "producer-timeout": [
    { name: "payments.transactions.v1", partitions: 12, replicationFactor: 3, retentionHours: 72,
      lagTotal: 6200, msgPerSec: 240, status: "critical",
      consumerGroups: ["payment-processor"],
      description: "Payment transactions — producer batch timeouts causing consumer lag spike." },
  ],
  "consumer-session-timeout": [
    { name: "invoices.created.v1", partitions: 8, replicationFactor: 3, retentionHours: 48,
      lagTotal: 9800, msgPerSec: 60, status: "critical",
      consumerGroups: ["invoice-renderer", "email-notifier"],
      description: "Invoice creation events — consumer GC pauses causing session timeouts." },
  ],
  "compaction-lag": [
    { name: "ops.lessons.v1", partitions: 1, replicationFactor: 3, retentionHours: 8760,
      lagTotal: 0, msgPerSec: 3, status: "degraded",
      consumerGroups: ["lesson-reader"],
      description: "Compacted lessons topic — log compaction falling behind on high-frequency updates." },
    { name: "audit.payment.events.v1", partitions: 16, replicationFactor: 3, retentionHours: 720,
      lagTotal: 52000, msgPerSec: 1900, status: "critical",
      consumerGroups: ["audit-archiver"],
      description: "Audit trail — compaction lag growing due to high write throughput." },
  ],
};

// ── Topics panel — in left sidebar ───────────────────────────────────────────

function TopicsPanel({
  topics, prevLagRef, onSelect, onCreateNew, visibleCount, onShowMore, monDet,
}: {
  topics: KafkaTopic[];
  prevLagRef: React.MutableRefObject<Record<string, number>>;
  onSelect: (t: KafkaTopic) => void;
  onCreateNew: () => void;
  visibleCount: number;
  onShowMore: () => void;
  monDet?: MonDetection;
}) {
  const det = monDet ?? emptyDet();
  // Sort: newest first (most recently created/updated topic at top)
  const sorted = [...topics].sort((a, b) => b.createdAt - a.createdAt);
  const visible = sorted.slice(0, visibleCount);
  const remaining = sorted.length - visibleCount;

  return (
    <div className="flex flex-col min-h-0">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div>
          <div className="text-xs font-extrabold uppercase tracking-widest" style={{ color: "#1e3a5f" }}>
            Kafka Topics
          </div>
          <div className="text-[11px] font-semibold mt-0.5" style={{ color: "#475569" }}>
            {topics.length} total &nbsp;·&nbsp; {visible.length} shown
          </div>
        </div>
        <button onClick={onCreateNew}
          className="text-[10px] font-bold rounded-lg px-2 py-1 transition-colors"
          style={{ background: "#e6f5f0", color: "#0F6E56", border: "1px solid #a3d9c8" }}
          onMouseEnter={e => { (e.currentTarget.style.background = "#1D9E75"); (e.currentTarget.style.color = "#fff"); }}
          onMouseLeave={e => { (e.currentTarget.style.background = "#e6f5f0"); (e.currentTarget.style.color = "#0F6E56"); }}>
          + New
        </button>
      </div>

      {/* Scrollable topic list — no height cap so user can scroll naturally */}
      <div className="space-y-2 overflow-y-auto" style={{ maxHeight: "520px" }}>
        {visible.map((t) => {
          const st = TOPIC_STATUS_STYLE[t.status];
          const prevLag = prevLagRef.current[t.id] ?? t.lagTotal;
          const lagTrend = t.lagTotal > prevLag + 50 ? "▲" : t.lagTotal < prevLag - 50 ? "▼" : null;
          const trendColor = lagTrend === "▲" ? "text-red-500" : "text-emerald-500";
          prevLagRef.current[t.id] = t.lagTotal;

          return (
            <button
              key={t.id}
              onClick={() => onSelect(t)}
              className="w-full text-left rounded-xl p-3 border transition-all shadow-sm"
              style={{ background: "#fff", borderColor: "#dce5ef" }}
              onMouseEnter={e => { (e.currentTarget as HTMLElement).style.borderColor = "#1D9E75"; (e.currentTarget as HTMLElement).style.background = "#f0faf6"; }}
              onMouseLeave={e => { (e.currentTarget as HTMLElement).style.borderColor = "#dce5ef"; (e.currentTarget as HTMLElement).style.background = "#fff"; }}
            >
              <div className="flex items-center justify-between gap-1.5 mb-1.5">
                <span className="text-[11px] font-bold leading-tight truncate" style={{ color: "#1e3a5f" }}>
                  {t.name.split(".").slice(-2).join(".")}
                </span>
                <span className={`flex items-center gap-1 text-[9px] font-bold shrink-0 px-1.5 py-0.5 rounded-full border ${st.bg} ${st.border} ${st.text}`}>
                  <span className={`w-1.5 h-1.5 rounded-full animate-pulse ${st.dot}`} />
                  {t.status}
                </span>
              </div>
              <div className="flex items-center gap-2 text-[10px]" style={{ color: "#64748b" }}>
                <span className="font-medium">{t.partitions}p</span>
                <span style={{ color: "#cbd5e1" }}>·</span>
                <span>
                  lag <span className={clsx("font-semibold", t.lagTotal > 5000 ? "text-red-600" : t.lagTotal > 1000 ? "text-amber-600" : "text-[#1e3a5f]")}>
                    {t.lagTotal > 999 ? `${(t.lagTotal / 1000).toFixed(1)}k` : t.lagTotal}
                  </span>
                  {lagTrend && <span className={clsx("ml-0.5 text-[9px] font-bold", trendColor)}>{lagTrend}</span>}
                </span>
                <span style={{ color: "#cbd5e1" }}>·</span>
                <span className="font-medium">{t.msgPerSec}/s</span>
              </div>
              {topicRelatedScenarios(t, det).length > 0 && (
                <div className="mt-1.5 flex items-center gap-1 flex-wrap">
                  {topicRelatedScenarios(t, det).map(sid => (
                    <span key={sid}
                      className="text-[9px] font-mono px-1.5 py-0.5 rounded-md"
                      style={{ background: "#22d3ee12", border: "1px solid #22d3ee35", color: "#0e7490" }}>
                      ⚡ {sid}
                    </span>
                  ))}
                </div>
              )}
            </button>
          );
        })}
      </div>

      {/* "More" button — OUTSIDE the scroll area, always visible at bottom */}
      {remaining > 0 && (
        <button
          onClick={onShowMore}
          className="w-full mt-2 text-[10px] font-semibold rounded-xl py-2 border border-dashed transition-all"
          style={{ color: "#1D9E75", borderColor: "#a3d9c8", background: "#f0faf6" }}
          onMouseEnter={e => { (e.currentTarget.style.background = "#e6f5f0"); (e.currentTarget.style.borderColor = "#1D9E75"); }}
          onMouseLeave={e => { (e.currentTarget.style.background = "#f0faf6"); (e.currentTarget.style.borderColor = "#a3d9c8"); }}
        >
          + {Math.min(20, remaining)} more topic{remaining > 1 ? "s" : ""} ↓
        </button>
      )}
    </div>
  );
}

// ── Create / Copy topic modal ─────────────────────────────────────────────────

function CreateTopicModal({
  existingTopics,
  templateTopic,
  onClose,
  onCreate,
}: {
  existingTopics: KafkaTopic[];
  templateTopic?: KafkaTopic;   // non-null = copy mode
  onClose: () => void;
  onCreate: (t: KafkaTopic) => void;
}) {
  const isCopy = !!templateTopic;
  const [draft, setDraft] = useState<Omit<KafkaTopic, "id" | "status" | "consumerGroups" | "createdAt">>({
    name:              isCopy ? `${templateTopic!.name}.copy` : "",
    partitions:        templateTopic?.partitions        ?? 6,
    replicationFactor: templateTopic?.replicationFactor ?? 3,
    retentionHours:    templateTopic?.retentionHours    ?? 72,
    lagTotal:          0,
    msgPerSec:         templateTopic?.msgPerSec         ?? 50,
    description:       isCopy ? `Copy of ${templateTopic!.name}` : "",
  });
  const [submitted, setSubmitted] = useState(false);

  const isDuplicate = existingTopics.some(t => t.name.trim() === draft.name.trim());
  const nameEmpty   = draft.name.trim() === "";
  const hasError    = submitted && (nameEmpty || isDuplicate);

  const handleSubmit = () => {
    setSubmitted(true);
    if (nameEmpty || isDuplicate) return;
    const newTopic: KafkaTopic = {
      id: `t-${Date.now()}`,
      name: draft.name.trim(),
      partitions: draft.partitions,
      replicationFactor: draft.replicationFactor,
      retentionHours: draft.retentionHours,
      lagTotal: 0,
      msgPerSec: draft.msgPerSec,
      status: "healthy",
      consumerGroups: isCopy ? [...(templateTopic?.consumerGroups ?? [])] : [],
      description: draft.description,
      createdAt: Date.now(),
    };
    onCreate(newTopic);
  };

  return (
    <div className="fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4
                    animate-[fadeIn_0.2s_ease-out]">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden animate-[slideUp_0.25s_ease-out]">

        {/* Header */}
        <div className="px-6 py-4 flex items-start justify-between" style={{ background: "#1e3a5f" }}>
          <div>
            <div className="text-xs font-bold uppercase tracking-widest mb-1" style={{ color: "#93c5fd" }}>
              {isCopy ? "Copy Topic" : "Create New Topic"}
            </div>
            <div className="text-sm font-bold text-white">
              {isCopy ? `Copying from ${templateTopic!.name.split(".").slice(-2).join(".")}` : "New Kafka Topic"}
            </div>
          </div>
          <button onClick={onClose} className="transition-colors text-lg leading-none" style={{ color: "#93c5fd" }}
            onMouseEnter={e => (e.currentTarget.style.color = "#fff")}
            onMouseLeave={e => (e.currentTarget.style.color = "#93c5fd")}>×</button>
        </div>

        {/* Form */}
        <div className="px-6 py-5 space-y-4">
          <div>
            <label className="text-[10px] font-bold uppercase tracking-wider block mb-1" style={{ color: "#64748b" }}>
              Topic Name *
            </label>
            <input
              className="w-full text-xs border rounded-lg px-3 py-2 font-mono focus:outline-none"
              style={{
                borderColor: hasError && (nameEmpty || isDuplicate) ? "#ef4444" : "#dce5ef",
                boxShadow: hasError && (nameEmpty || isDuplicate) ? "0 0 0 2px rgba(239,68,68,0.12)" : "none",
              }}
              placeholder="e.g. payments.events.v2"
              value={draft.name}
              onChange={e => { setSubmitted(false); setDraft({ ...draft, name: e.target.value }); }}
            />
            {submitted && nameEmpty && <p className="text-[10px] text-red-500 mt-1">Topic name is required.</p>}
            {submitted && !nameEmpty && isDuplicate && (
              <p className="text-[10px] text-red-500 mt-1">⚠️ A topic named &quot;{draft.name.trim()}&quot; already exists.</p>
            )}
          </div>

          <div className="grid grid-cols-3 gap-3">
            {[
              { label: "Partitions", key: "partitions" as const, min: 1, max: 64 },
              { label: "Replicas",   key: "replicationFactor" as const, min: 1, max: 5 },
              { label: "Retention (h)", key: "retentionHours" as const, min: 1, max: 8760 },
            ].map(f => (
              <div key={f.key}>
                <label className="text-[10px] font-bold uppercase tracking-wider block mb-1" style={{ color: "#64748b" }}>{f.label}</label>
                <input type="number" min={f.min} max={f.max}
                  className="w-full text-xs border rounded-lg px-3 py-2 focus:outline-none"
                  style={{ borderColor: "#dce5ef" }}
                  value={draft[f.key]}
                  onChange={e => setDraft({ ...draft, [f.key]: parseInt(e.target.value) || f.min })}
                />
              </div>
            ))}
          </div>

          <div>
            <label className="text-[10px] font-bold uppercase tracking-wider block mb-1" style={{ color: "#64748b" }}>Description</label>
            <textarea rows={2}
              className="w-full text-xs border rounded-lg px-3 py-2 focus:outline-none resize-none"
              style={{ borderColor: "#dce5ef" }}
              placeholder="What events does this topic carry?"
              value={draft.description}
              onChange={e => setDraft({ ...draft, description: e.target.value })}
            />
          </div>
        </div>

        {/* Footer */}
        <div className="px-6 pb-5 flex gap-3" style={{ borderTop: "1px solid #dce5ef", paddingTop: "1rem" }}>
          <button onClick={handleSubmit}
            className="flex-1 py-2.5 rounded-xl text-white text-xs font-bold transition-colors"
            style={{ background: "#1D9E75" }}
            onMouseEnter={e => (e.currentTarget.style.background = "#0F6E56")}
            onMouseLeave={e => (e.currentTarget.style.background = "#1D9E75")}>
            {isCopy ? "✓ Create Copy" : "✓ Create Topic"}
          </button>
          <button onClick={onClose}
            className="flex-1 py-2.5 rounded-xl text-xs font-semibold transition-colors"
            style={{ background: "#f0f4f8", color: "#64748b", border: "1px solid #dce5ef" }}
            onMouseEnter={e => (e.currentTarget.style.background = "#dce5ef")}
            onMouseLeave={e => (e.currentTarget.style.background = "#f0f4f8")}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Topic detail / edit modal ─────────────────────────────────────────────────

function TopicModal({
  topic, scenarioRunning, onClose, onSave, onDelete, onHeal, onCopy,
}: {
  topic: KafkaTopic;
  scenarioRunning: boolean;
  onClose: () => void;
  onSave: (updated: KafkaTopic) => void;
  onDelete: (t: KafkaTopic) => void;
  onHeal: (t: KafkaTopic) => void;
  onCopy: (t: KafkaTopic) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<KafkaTopic>({ ...topic });
  const st = TOPIC_STATUS_STYLE[topic.status];

  const hasChanges =
    draft.name !== topic.name ||
    draft.partitions !== topic.partitions ||
    draft.replicationFactor !== topic.replicationFactor ||
    draft.retentionHours !== topic.retentionHours ||
    draft.description !== topic.description;

  return (
    <div className="fixed inset-0 z-50 bg-slate-900/60 backdrop-blur-sm flex items-center justify-center p-4
                    animate-[fadeIn_0.2s_ease-out]">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden animate-[slideUp_0.25s_ease-out]">

        {/* Header */}
        <div className="px-6 py-4 flex items-start justify-between" style={{ background: "#1e3a5f" }}>
          <div>
            <div className="text-xs font-bold uppercase tracking-widest mb-1" style={{ color: "#93c5fd" }}>Kafka Topic</div>
            <div className="text-sm font-bold text-white font-mono leading-tight">{topic.name}</div>
          </div>
          <div className="flex items-center gap-2">
            <span className={`text-[9px] font-bold px-2 py-1 rounded-full ${st.bg} ${st.text} ${st.border} border`}>
              {topic.status}
            </span>
            <button onClick={onClose} className="transition-colors text-lg leading-none" style={{ color: "#93c5fd" }}
              onMouseEnter={e => (e.currentTarget.style.color = "#fff")}
              onMouseLeave={e => (e.currentTarget.style.color = "#93c5fd")}>×</button>
          </div>
        </div>

        {/* Body */}
        <div className="px-6 py-5 space-y-4 max-h-[60vh] overflow-y-auto">

          {/* Description */}
          {!editing && (
            <p className="text-xs text-slate-500 leading-relaxed">{topic.description}</p>
          )}

          {/* Stats row */}
          {!editing && (
            <div className="grid grid-cols-4 gap-2">
              {[
                { label: "Partitions", value: topic.partitions },
                { label: "Replicas",   value: topic.replicationFactor },
                { label: "Retention",  value: `${topic.retentionHours}h` },
                { label: "Lag",        value: topic.lagTotal > 999 ? `${(topic.lagTotal/1000).toFixed(1)}k` : topic.lagTotal },
              ].map((s) => (
                <div key={s.label} className="rounded-xl p-2.5 text-center border" style={{ background: "#f0f4f8", borderColor: "#dce5ef" }}>
                  <div className="text-[10px] mb-0.5" style={{ color: "#94a3b8" }}>{s.label}</div>
                  <div className="text-sm font-bold" style={{ color: "#1e3a5f" }}>{s.value}</div>
                </div>
              ))}
            </div>
          )}

          {/* Consumer groups */}
          {!editing && (
            <div>
              <div className="text-[10px] font-bold uppercase tracking-widest mb-1.5" style={{ color: "#94a3b8" }}>Consumer Groups</div>
              <div className="flex flex-wrap gap-1.5">
                {topic.consumerGroups.map((g) => (
                  <span key={g} className="text-[10px] rounded-full px-2.5 py-0.5 font-medium"
                    style={{ background: "#e6f5f0", border: "1px solid #a3d9c8", color: "#0F6E56" }}>
                    {g}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Edit form */}
          {editing && (
            <div className="space-y-3">
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Topic Name</label>
                <input
                  className="w-full text-xs border border-slate-300 rounded-lg px-3 py-2 font-mono focus:outline-none focus:border-blue-400"
                  value={draft.name}
                  onChange={(e) => setDraft({ ...draft, name: e.target.value })}
                />
              </div>
              <div className="grid grid-cols-3 gap-3">
                <div>
                  <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Partitions</label>
                  <input type="number" min={1} max={64}
                    className="w-full text-xs border border-slate-300 rounded-lg px-3 py-2 focus:outline-none focus:border-blue-400"
                    value={draft.partitions}
                    onChange={(e) => setDraft({ ...draft, partitions: parseInt(e.target.value) || 1 })}
                  />
                </div>
                <div>
                  <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Replicas</label>
                  <input type="number" min={1} max={5}
                    className="w-full text-xs border border-slate-300 rounded-lg px-3 py-2 focus:outline-none focus:border-blue-400"
                    value={draft.replicationFactor}
                    onChange={(e) => setDraft({ ...draft, replicationFactor: parseInt(e.target.value) || 1 })}
                  />
                </div>
                <div>
                  <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Retention (h)</label>
                  <input type="number" min={1} max={8760}
                    className="w-full text-xs border border-slate-300 rounded-lg px-3 py-2 focus:outline-none focus:border-blue-400"
                    value={draft.retentionHours}
                    onChange={(e) => setDraft({ ...draft, retentionHours: parseInt(e.target.value) || 1 })}
                  />
                </div>
              </div>
              <div>
                <label className="text-[10px] font-bold text-slate-500 uppercase tracking-wider block mb-1">Description</label>
                <textarea rows={2}
                  className="w-full text-xs border border-slate-300 rounded-lg px-3 py-2 focus:outline-none focus:border-blue-400 resize-none"
                  value={draft.description}
                  onChange={(e) => setDraft({ ...draft, description: e.target.value })}
                />
              </div>
            </div>
          )}
        </div>

        {/* Footer actions */}
        <div className="px-6 pb-5 flex items-center gap-2 pt-4" style={{ borderTop: "1px solid #dce5ef" }}>
          {!editing ? (
            <>
              {/* Heal button — only shown for degraded/critical topics */}
              {topic.status !== "healthy" && (
                <button
                  disabled={scenarioRunning}
                  onClick={() => onHeal(topic)}
                  className="flex-1 py-2.5 rounded-xl text-white text-xs font-bold transition-colors disabled:opacity-40"
                  style={{ background: topic.status === "critical" ? "#dc2626" : "#d97706" }}
                  onMouseEnter={e => { if (!(e.currentTarget as HTMLButtonElement).disabled) (e.currentTarget.style.background = topic.status === "critical" ? "#b91c1c" : "#b45309"); }}
                  onMouseLeave={e => { (e.currentTarget.style.background = topic.status === "critical" ? "#dc2626" : "#d97706"); }}
                >
                  {scenarioRunning ? "⏳ Running…" : topic.status === "critical" ? "🔴 Heal Now" : "⚠️ Heal Topic"}
                </button>
              )}
              <button
                onClick={() => setEditing(true)}
                className="flex-1 py-2.5 rounded-xl text-white text-xs font-bold transition-colors"
                style={{ background: "#1e3a5f" }}
                onMouseEnter={e => (e.currentTarget.style.background = "#162d4a")}
                onMouseLeave={e => (e.currentTarget.style.background = "#1e3a5f")}
              >
                ✏️ Edit
              </button>
              <button
                onClick={() => onCopy(topic)}
                className="py-2.5 px-3 rounded-xl text-xs font-bold transition-colors"
                style={{ background: "#fff", border: "1px solid #dce5ef", color: "#64748b" }}
                onMouseEnter={e => { (e.currentTarget.style.background = "#f0f4f8"); }}
                onMouseLeave={e => { (e.currentTarget.style.background = "#fff"); }}
                title="Copy topic"
              >
                📋
              </button>
              <button
                onClick={() => onDelete(topic)}
                className="py-2.5 px-3 rounded-xl bg-white border-2 border-red-200 hover:border-red-400 hover:bg-red-50 text-red-600 text-xs font-bold transition-colors"
              >
                🗑
              </button>
              <button onClick={onClose}
                className="px-4 py-2.5 rounded-xl text-xs font-semibold transition-colors"
                style={{ background: "#f0f4f8", color: "#64748b" }}
                onMouseEnter={e => (e.currentTarget.style.background = "#dce5ef")}
                onMouseLeave={e => (e.currentTarget.style.background = "#f0f4f8")}>
                Close
              </button>
            </>
          ) : (
            <>
              <button
                disabled={!hasChanges || scenarioRunning}
                onClick={() => { onSave(draft); setEditing(false); }}
                className="flex-1 py-2.5 rounded-xl disabled:opacity-40 text-white text-xs font-bold transition-colors"
                style={{ background: "#1D9E75" }}
                onMouseEnter={e => !((e.currentTarget as HTMLButtonElement).disabled) && (e.currentTarget.style.background = "#0F6E56")}
                onMouseLeave={e => !((e.currentTarget as HTMLButtonElement).disabled) && (e.currentTarget.style.background = "#1D9E75")}
              >
                {scenarioRunning ? "⏳ Applying…" : "✓ Save & Apply"}
              </button>
              <button onClick={() => { setDraft({ ...topic }); setEditing(false); }}
                className="flex-1 py-2.5 rounded-xl text-xs font-semibold transition-colors"
                style={{ background: "#fff", border: "1px solid #dce5ef", color: "#64748b" }}
                onMouseEnter={e => (e.currentTarget.style.background = "#f0f4f8")}
                onMouseLeave={e => (e.currentTarget.style.background = "#fff")}>
                Cancel
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Delete confirmation modal ─────────────────────────────────────────────────

function DeleteConfirmModal({
  topic, onConfirm, onCancel,
}: {
  topic: KafkaTopic;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="fixed inset-0 z-[60] bg-slate-900/70 backdrop-blur-sm flex items-center justify-center p-4
                    animate-[fadeIn_0.15s_ease-out]">
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm overflow-hidden animate-[slideUp_0.2s_ease-out]">
        <div className="bg-red-50 border-b border-red-200 px-6 py-4 flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-white border border-red-200 flex items-center justify-center text-xl shadow-sm">
            🗑
          </div>
          <div>
            <h2 className="text-sm font-bold text-slate-800">Delete Kafka Topic</h2>
            <p className="text-xs text-red-600 font-medium mt-0.5">This action will trigger the MRAL cycle.</p>
          </div>
        </div>
        <div className="px-6 py-5">
          <p className="text-sm text-slate-600 mb-3">
            Are you sure you want to delete topic
          </p>
          <div className="bg-red-50 border border-red-200 rounded-lg px-3 py-2 font-mono text-sm font-bold text-red-800 mb-5 break-all">
            {topic.name}
          </div>
          <p className="text-xs text-slate-400 mb-5">
            All {topic.consumerGroups.length} consumer group{topic.consumerGroups.length !== 1 ? "s" : ""} will be notified.
            The agent mesh will log this deletion and send an audit notification.
          </p>
          <div className="flex gap-3">
            <button onClick={onConfirm}
              className="flex-1 py-3 rounded-xl bg-red-600 hover:bg-red-700 text-white font-bold text-sm transition-colors">
              Yes, Delete
            </button>
            <button onClick={onCancel}
              className="flex-1 py-3 rounded-xl bg-white border-2 border-slate-200 hover:bg-slate-50 text-slate-600 font-bold text-sm transition-colors">
              Cancel
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Scenario history bar ──────────────────────────────────────────────────────

const SCENARIO_BADGE_COLOR: Record<string, string> = {
  "lag-spike":               "#2563eb",
  "controller-failover":     "#7c3aed",
  "share-group":             "#ea580c",
  "benign-rebalance":        "#16a34a",
  "schema-mismatch":         "#8b5cf6",
  "disk-saturation":         "#dc2626",
  "under-replication":       "#b91c1c",
  "producer-timeout":        "#d97706",
  "consumer-session-timeout":"#4f46e5",
  "compaction-lag":          "#0891b2",
  "topic-heal":              "#1D9E75",
  "topic-change":            "#0891b2",
};

function inferColor(label: string): string {
  const lower = label.toLowerCase();
  if (lower.includes("lag"))        return "#2563eb";
  if (lower.includes("failover"))   return "#7c3aed";
  if (lower.includes("share"))      return "#ea580c";
  if (lower.includes("suppress") || lower.includes("false")) return "#16a34a";
  if (lower.includes("schema"))     return "#8b5cf6";
  if (lower.includes("disk"))       return "#dc2626";
  if (lower.includes("under"))      return "#b91c1c";
  if (lower.includes("producer"))   return "#d97706";
  if (lower.includes("session"))    return "#4f46e5";
  if (lower.includes("compact"))    return "#0891b2";
  if (lower.includes("heal"))       return "#1D9E75";
  return "#64748b";
}

function ScenarioHistoryBar({ history, onView }: {
  history: EmailSummaryData[];
  onView: (d: EmailSummaryData) => void;
}) {
  if (!history.length) return null;

  return (
    <div className="p-3">
      <div className="flex items-center gap-2 mb-3 px-1">
        <span className="w-2.5 h-2.5 rounded-full" style={{ background: "#1D9E75" }} />
        <span className="text-xs font-extrabold uppercase tracking-widest" style={{ color: "#1e3a5f" }}>
          Scenario History
        </span>
        <span className="text-[11px] font-semibold ml-1" style={{ color: "#475569" }}>
          {history.length} runs — click any to view
        </span>
      </div>

      <div className="space-y-1.5">
        {history.map((h, i) => {
          const approved  = h.approved;
          const cardColor = inferColor(h.scenarioLabel);
          const confidence = h.reasoning ? Math.round(h.reasoning.confidence * 100) : null;

          return (
            <button
              key={i}
              onClick={() => onView(h)}
              className="w-full text-left rounded-xl border flex items-center gap-3 px-4 py-3 transition-all"
              style={{
                background: "#fff", borderColor: "#dce5ef",
                borderLeftWidth: 4, borderLeftColor: cardColor,
              }}
              onMouseEnter={e => {
                (e.currentTarget as HTMLElement).style.background = cardColor + "08";
                (e.currentTarget as HTMLElement).style.borderColor = cardColor + "60";
                (e.currentTarget as HTMLElement).style.borderLeftColor = cardColor;
              }}
              onMouseLeave={e => {
                (e.currentTarget as HTMLElement).style.background = "#fff";
                (e.currentTarget as HTMLElement).style.borderColor = "#dce5ef";
                (e.currentTarget as HTMLElement).style.borderLeftColor = cardColor;
              }}
            >
              {/* Status dot */}
              <span style={{
                width: 9, height: 9, borderRadius: "50%", flexShrink: 0, display: "inline-block",
                background: approved ? "#16a34a" : "#dc2626",
              }} />

              {/* Scenario name */}
              <span style={{ fontSize: 14, fontWeight: 700, color: "#1e3a5f", flex: 1, textAlign: "left" }}>
                {h.scenarioLabel}
              </span>

              {h.completedAt && (
                <span style={{ fontSize: 11, color: "#94a3b8", flexShrink: 0, marginLeft: 16, fontWeight: 400, whiteSpace: "nowrap" }}>
                  {new Date(h.completedAt).toLocaleString("en-GB", { dateStyle: "short", timeStyle: "short" })}
                </span>
              )}
              {/* Confidence */}
              {confidence !== null && (
                <span style={{ fontSize: 12, fontWeight: 600, color: "#475569", flexShrink: 0 }}>
                  {confidence}%
                </span>
              )}

              {/* Status badge */}
              <span style={{
                fontSize: 11, fontWeight: 700, padding: "3px 10px", borderRadius: 20, flexShrink: 0,
                background: (h as { status?: string }).status === "awaiting-approval" ? "#fef3c7" : approved ? "#dcfce7" : "#fef2f2",
                color: (h as { status?: string }).status === "awaiting-approval" ? "#92400e" : approved ? "#16a34a" : "#dc2626",
                border: `1px solid ${(h as { status?: string }).status === "awaiting-approval" ? "#fcd34d" : approved ? "#86efac" : "#fca5a5"}`,
              }}>
                {(h as { status?: string }).status === "awaiting-approval"
                  ? "⏳ Awaiting Approval"
                  : approved ? "✓ Resolved" : "✗ Rejected"}
              </span>

              {/* Chevron */}
              <span style={{ fontSize: 16, fontWeight: 700, color: "#64748b", flexShrink: 0 }}>›</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ── Cluster Statistics modal ──────────────────────────────────────────────────
// Opened when the user clicks the "REAL mode" / "MOCK mode" badge in the nav.
// Shows live Aiven connection details (from useClusterStore) + broker topology.

function ClusterStatsModal({
  modeInfo,
  broker,
  onClose,
}: {
  modeInfo: ModeInfo | null;
  broker: BrokerState | null;
  onClose: () => void;
}) {
  const kafka      = modeInfo?.kafka;
  const isReal     = modeInfo?.mode === "real";
  const isAiven    = isReal && !!kafka?.bootstrapInternal && !modeInfo?.kubeAvailable;
  const host       = kafka?.bootstrapInternal?.split(":")[0] ?? "—";
  const port       = kafka?.bootstrapInternal?.split(":")[1] ?? "—";
  const hostShort  = host === "—" ? "—" : host.split(".").slice(0, 2).join(".") + "…";

  const topics = broker ? Object.entries(broker.topics) : [];
  const groups = broker ? Object.entries(broker.consumerGroups) : [];

  return (
    <div
      className="fixed inset-0 z-50 bg-slate-900/55 backdrop-blur-sm flex items-start justify-end pt-14 pr-3
                 animate-[fadeIn_0.15s_ease-out]"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl shadow-2xl w-full max-w-[480px] overflow-hidden animate-[slideDown_0.2s_ease-out]"
        style={{ maxHeight: "calc(100vh - 72px)", overflowY: "auto" }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-5 py-4 flex items-center justify-between"
             style={{ background: "#1e3a5f", borderBottom: "1px solid rgba(255,255,255,0.08)" }}>
          <div>
            <div className="text-[10px] font-bold uppercase tracking-widest mb-0.5" style={{ color: "#93c5fd" }}>
              {isAiven ? "Aiven · Direct Kafka" : isReal ? "Real Cluster" : "Simulator"}
            </div>
            <div className="text-sm font-bold text-white">Cluster Statistics</div>
          </div>
          <button onClick={onClose} className="text-xl leading-none" style={{ color: "#93c5fd" }}
            onMouseEnter={e => (e.currentTarget.style.color = "#fff")}
            onMouseLeave={e => (e.currentTarget.style.color = "#93c5fd")}>×</button>
        </div>

        <div className="p-5 space-y-5">

          {/* ── Connection info (real/Aiven) ── */}
          {isReal && kafka && (
            <section>
              <div className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: "#94a3b8" }}>
                🔗 Connection
              </div>
              <div className="rounded-xl border overflow-hidden" style={{ borderColor: "#dce5ef" }}>
                {[
                  { label: "Bootstrap Host", value: host,                                                mono: true  },
                  { label: "Port",           value: port,                                                mono: true  },
                  { label: "SASL Mechanism", value: "SCRAM-SHA-256",                                    mono: false },
                  { label: "Auth User",      value: kafka.username ?? "avnadmin",                       mono: true  },
                  { label: "CA Certificate", value: kafka.hasCaCert ? "✓ Custom CA present" : "✗ None", mono: false, special: true, ok: kafka.hasCaCert },
                  { label: "Password",       value: kafka.hasPassword ? "✓ Set" : "✗ Not set",          mono: false, special: true, ok: kafka.hasPassword },
                ].map((r, i, arr) => (
                  <div key={r.label}
                    className="flex items-center justify-between px-4 py-2.5"
                    style={{
                      borderBottom: i < arr.length - 1 ? "1px solid #f1f5f9" : "none",
                      background: i % 2 === 0 ? "#fff" : "#f8fafc",
                    }}>
                    <span style={{ fontSize: 11, color: "#64748b" }}>{r.label}</span>
                    <span style={{
                      fontSize: 11, fontWeight: 700,
                      fontFamily: r.mono ? "monospace" : "inherit",
                      color: r.special ? (r.ok ? "#16a34a" : "#f97316") : "#1e3a5f",
                      maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                    }}>
                      {r.label === "Bootstrap Host" ? (
                        <span title={host}>{hostShort}</span>
                      ) : r.value}
                    </span>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* ── Cluster status grid ── */}
          {broker && (
            <section>
              <div className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: "#94a3b8" }}>
                ⬡ Cluster Status
              </div>
              <div className="grid grid-cols-3 gap-2">
                {[
                  { label: "Mode",        value: broker.mode,                      color: broker.mode === "REAL" ? "#16a34a" : "#7c3aed" },
                  { label: "Brokers",     value: `${broker.brokersOnline} online`,  color: "#1e3a5f" },
                  { label: "Topics",      value: `${topics.length}`,               color: "#2563eb" },
                  { label: "Ctrl Epoch",  value: `${broker.controllerEpoch}`,      color: "#1e3a5f" },
                  { label: "ACLs",        value: `${broker.aclCount}`,           color: "#1e3a5f" },
                  { label: "mTLS",        value: broker.mtls ? "on" : "off",       color: broker.mtls ? "#16a34a" : "#94a3b8" },
                ].map((s) => (
                  <div key={s.label} className="rounded-xl p-3 text-center border"
                       style={{ background: "#f8fafc", borderColor: "#dce5ef" }}>
                    <div style={{ fontSize: 9, color: "#94a3b8", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 2 }}>
                      {s.label}
                    </div>
                    <div style={{ fontSize: 12, fontWeight: 700, color: s.color }}>{s.value}</div>
                  </div>
                ))}
              </div>
            </section>
          )}

          {/* ── Topics breakdown ── */}
          {topics.length > 0 && (
            <section>
              <div className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: "#94a3b8" }}>
                📋 Topics ({topics.length})
              </div>
              <div className="rounded-xl border overflow-hidden" style={{ borderColor: "#dce5ef" }}>
                <table className="w-full" style={{ borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ background: "#f8fafc", borderBottom: "1px solid #dce5ef" }}>
                      {["Topic", "Parts", "Lag", "Offset"].map((h) => (
                        <th key={h} className="text-left px-3 py-2"
                          style={{ fontSize: 9, color: "#94a3b8", fontWeight: 700, textTransform: "uppercase", letterSpacing: "0.5px" }}>
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {topics.map(([name, td], i) => {
                      return (
                        <tr key={name}
                          style={{ background: i % 2 === 0 ? "#fff" : "#f8fafc", borderBottom: i < topics.length - 1 ? "1px solid #f1f5f9" : "none" }}>
                          <td style={{ padding: "7px 12px", fontFamily: "monospace", fontSize: 10, color: "#1e3a5f", maxWidth: 180 }}>
                            <span style={{ display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={name}>
                              {name.split(".").slice(-2).join(".")}
                            </span>
                          </td>
                          <td style={{ padding: "7px 12px", textAlign: "center", fontSize: 11, fontWeight: 600, color: "#64748b" }}>{td.partitions}</td>
                          <td style={{ padding: "7px 12px", textAlign: "center", fontSize: 11, fontWeight: 700,
                                        color: td.lag > 100 ? "#dc2626" : td.lag > 0 ? "#d97706" : "#16a34a" }}>{td.lag}</td>
                          <td style={{ padding: "7px 12px", textAlign: "center", fontSize: 11, fontWeight: 600, color: "#64748b" }}>{td.offsetHigh}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* ── Consumer groups ── */}
          {groups.length > 0 && (
            <section>
              <div className="text-[10px] font-bold uppercase tracking-widest mb-2" style={{ color: "#94a3b8" }}>
                👥 Consumer Groups ({groups.length})
              </div>
              <div className="rounded-xl border overflow-hidden" style={{ borderColor: "#dce5ef" }}>
                {groups.map(([name, gd], i) => {
                  const stable = gd.rebalanceState?.toLowerCase() === "stable";
                  return (
                    <div key={name}
                      className="flex items-center justify-between px-4 py-2.5"
                      style={{ background: i % 2 === 0 ? "#fff" : "#f8fafc", borderBottom: i < groups.length - 1 ? "1px solid #f1f5f9" : "none" }}>
                      <span style={{ fontSize: 11, fontFamily: "monospace", fontWeight: 600, color: "#1e3a5f" }}>{name}</span>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 10, fontWeight: 700, color: gd.lag > 0 ? "#d97706" : "#16a34a" }}>
                          lag {gd.lag}
                        </span>
                        <span style={{
                          fontSize: 9, fontWeight: 600, padding: "2px 7px", borderRadius: 20,
                          background: stable ? "#dcfce7" : "#fef9c3",
                          color: stable ? "#16a34a" : "#b45309",
                          border: `1px solid ${stable ? "#86efac" : "#fde68a"}`,
                        }}>
                          {gd.rebalanceState}
                        </span>
                        <span style={{ fontSize: 10, color: "#94a3b8" }}>{gd.members}m</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>
          )}

          {/* Mock / no-data fallback */}
          {!isReal && !broker && (
            <div style={{ textAlign: "center", padding: "24px 0", fontSize: 13, color: "#94a3b8", fontStyle: "italic" }}>
              Running in simulator mode — no real cluster data.
            </div>
          )}

        </div>
      </div>
    </div>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const { state, trigger, approve, agentAction, reset, dismissEmailSummary, showLastSummary, triggerTopicAction, triggerTopicHeal } = useMeshStream();
  const phase = state.mralPhase ?? "idle";

  // Cluster polling — populates useClusterStore with real Aiven connection details
  useClusterPolling();
  const modeInfo = useClusterStore((s) => s.mode);

  // ── Topics state + live metrics animation ────────────────────────────────
  const [topics, setTopics] = useState<KafkaTopic[]>(INITIAL_TOPICS);
  const [selectedTopic, setSelectedTopic] = useState<KafkaTopic | null>(null);
  const [pendingDelete, setPendingDelete] = useState<KafkaTopic | null>(null);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [copyTemplate, setCopyTemplate] = useState<KafkaTopic | null>(null);
  // Track prev lag per topic for ▲▼ trend indicator
  const prevLagRef = useRef<Record<string, number>>({});

  // Live metric fluctuation — runs independently of scenarios
  useEffect(() => {
    const id = setInterval(() => {
      setTopics((prev) =>
        prev.map((t) => {
          // msg/s drifts ±12%
          const msgDelta = (Math.random() - 0.48) * 0.12 * t.msgPerSec;
          const newMsg = Math.max(1, Math.round(t.msgPerSec + msgDelta));
          // lag: critical topics tend to grow, healthy ones drain
          const lagBias = t.status === "critical" ? 0.12 : t.status === "degraded" ? 0.04 : -0.08;
          const lagDelta = (Math.random() - 0.5 + lagBias) * Math.max(200, t.lagTotal * 0.18);
          const newLag = Math.max(0, Math.round(t.lagTotal + lagDelta));
          const newStatus: KafkaTopic["status"] =
            newLag > 14000 ? "critical" : newLag > 2800 ? "degraded" : "healthy";
          return { ...t, lagTotal: newLag, msgPerSec: newMsg, status: newStatus };
        })
      );
    }, 2200);
    return () => clearInterval(id);
  }, []);

  // Background topic creation — new topic every 4–7 seconds (visible in real-time)
  useEffect(() => {
    const PREFIXES = ["payments","orders","users","inventory","analytics","notifications",
                      "events","logs","metrics","audit","billing","shipping","catalog","streams",
                      "customer","product","delivery","returns","subscriptions","webhooks"];
    const SUFFIXES = ["created.v1","updated.v2","deleted.v1","processed.v2","completed.v1",
                      "triggered.v1","synced.v2","failed.v1","queued.v1","dispatched.v2",
                      "validated.v1","enriched.v2","archived.v1","replayed.v1","deduplicated.v2"];
    let timer: ReturnType<typeof setTimeout>;
    const schedule = () => {
      timer = setTimeout(() => {
        const prefix = PREFIXES[Math.floor(Math.random() * PREFIXES.length)];
        const suffix = SUFFIXES[Math.floor(Math.random() * SUFFIXES.length)];
        const name   = `${prefix}.${suffix}`;
        setTopics(prev => {
          if (prev.some(t => t.name === name)) return prev;
          const now = Date.now();
          const newTopic: KafkaTopic = {
            id: `t-bg-${now}`,
            name,
            partitions:        [2, 4, 6, 8, 12][Math.floor(Math.random() * 5)],
            replicationFactor: Math.random() > 0.25 ? 3 : 2,
            retentionHours:    [24, 48, 72, 168][Math.floor(Math.random() * 4)],
            lagTotal:          Math.floor(Math.random() * 800),
            msgPerSec:         Math.floor(Math.random() * 240) + 5,
            status:            Math.random() > 0.85 ? "degraded" : "healthy",
            consumerGroups:    [],
            description:       `Auto-provisioned ${prefix} ${suffix.replace(/\.v\d$/, "")} stream`,
            createdAt:         now,
          };
          return [...prev, newTopic];
        });
        schedule();
      }, 4000 + Math.random() * 3000); // 4–7 s — visible in real-time
    };
    schedule();
    return () => clearTimeout(timer);
  }, []);

  const [extraOpen, setExtraOpen] = useState(true);
  const det = useMonitorDetection();
  const [topicsVisible, setTopicsVisible] = useState(20);

  // Wrapper around trigger() that auto-upserts scenario-relevant topics first
  const handleTrigger = (scenarioId: string) => {
    const autoTopics = SCENARIO_AUTO_TOPICS[scenarioId];
    if (autoTopics && autoTopics.length > 0) {
      setTopics(prev => {
        let next = [...prev];
        // Stagger createdAt so each auto-topic sorts above all existing ones
        const baseTs = Date.now();
        autoTopics.forEach((t, i) => {
          const exists = next.findIndex(x => x.name === t.name);
          if (exists >= 0) {
            // Bump createdAt so the updated topic rises to the top of the list
            next[exists] = { ...next[exists], lagTotal: t.lagTotal,
              msgPerSec: t.msgPerSec, status: t.status, createdAt: baseTs + i };
          } else {
            next = [{
              id: `t-scen-${baseTs}-${i}`,
              ...t,
              createdAt: baseTs + i,
            }, ...next];
          }
        });
        return next;
      });
      // Ensure panel shows at least first page
      setTopicsVisible(v => Math.max(v, 20));
    }
    trigger(scenarioId);
  };

  // ── Floating overlay states ────────────────────────────────────────────────
  const [brokerOpen, setBrokerOpen]           = useState(false);
  const [clusterStatsOpen, setClusterStatsOpen] = useState(false);

  // Persistent scenario history — survives page refreshes via localStorage.
  // Always start with [] so SSR and client hydration match, then load after mount.
  const [summaryHistory, setSummaryHistory]   = useState<EmailSummaryData[]>([]);
  const [historyMounted, setHistoryMounted]   = useState(false);
  const [viewHistorySummary, setViewHistorySummary] = useState<EmailSummaryData | null>(null);
  const [approvalToast, setApprovalToast] = useState<string | null>(null);
  const approvalToastRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Canvas height — +/- resizable
  // Default is 660 so the ephemeral REASON/ACT/LEARN sub-agent bubbles (y=545–635)
  // are visible without needing to press + first.
  const [canvasHeight, setCanvasHeight] = useState(660);

  // Load saved history from localStorage once after first client render
  useEffect(() => {
    try {
      const saved = localStorage.getItem("sre-scenario-history");
      if (saved) setSummaryHistory(JSON.parse(saved) as EmailSummaryData[]);
    } catch { /* corrupt data — ignore */ }
    setHistoryMounted(true);
  }, []); // runs once on mount

  // Persist history to localStorage whenever it changes (after mount)
  useEffect(() => {
    if (!historyMounted) return; // skip the initial empty render
    try {
      localStorage.setItem("sre-scenario-history", JSON.stringify(summaryHistory));
    } catch { /* quota exceeded — ignore */ }
  }, [summaryHistory, historyMounted]);

  // Keep a ref to the latest auditLog so the capture effect can read it
  // without needing auditLog in its dependency array (which would cause
  // repeated re-runs and make every scenario appear only once).
  const auditLogRef = useRef(state.auditLog);
  auditLogRef.current = state.auditLog;

  // Track which emailSummary object we last added so we don't double-add it.
  const lastAddedSummaryRef = useRef<EmailSummaryData | null>(null);

  // Capture completed scenario to history ───────────────────────────────────
  useEffect(() => {
    if (!state.emailSummary) return;
    // Same object reference = already captured (effect re-ran for another reason)
    if (state.emailSummary === lastAddedSummaryRef.current) return;
    lastAddedSummaryRef.current = state.emailSummary;

    const raw = state.emailSummary;
    // Hydrate liveEvents from auditLog when the scenario didn't populate them
    const hydrated: EmailSummaryData =
      raw.liveEvents && raw.liveEvents.length > 0
        ? raw
        : {
            ...raw,
            liveEvents: auditLogRef.current.slice(-40).map(r => ({
              type:    r.type,
              agent:   r.agent,
              summary: r.summary,
              ts:      r.ts,
            })),
          };

    // Prepend — no dedup so every run (even same scenario) gets its own entry
    setSummaryHistory(prev => [hydrated, ...prev].slice(0, 30));
  }, [state.emailSummary]);

  const handleTopicSave = (updated: KafkaTopic) => {
    const prev = topics.find((t) => t.id === updated.id)!;
    setTopics((ts) => ts.map((t) => t.id === updated.id ? updated : t));
    setSelectedTopic(null);
    const payload: TopicChangePayload = {
      operation: "edit",
      topic: { name: updated.name, partitions: updated.partitions, replicationFactor: updated.replicationFactor, retentionHours: updated.retentionHours },
      prevTopic: { name: prev.name, partitions: prev.partitions },
    };
    triggerTopicAction(payload);
  };

  const handleTopicCreate = (newTopic: KafkaTopic) => {
    // Stamp createdAt so manually-created topics sort to the top
    setTopics((ts) => [...ts, { ...newTopic, createdAt: Date.now() }]);
    setCreateModalOpen(false);
    setCopyTemplate(null);
    const payload: TopicChangePayload = {
      operation: "create",
      topic: { name: newTopic.name, partitions: newTopic.partitions, replicationFactor: newTopic.replicationFactor, retentionHours: newTopic.retentionHours },
    };
    triggerTopicAction(payload);
  };

  const handleTopicHeal = (topic: KafkaTopic) => {
    setSelectedTopic(null);  // close modal immediately so user sees the canvas animate
    if (topic.status === "healthy") return;
    const payload: TopicHealPayload = {
      topicName: topic.name,
      currentStatus: topic.status as "degraded" | "critical",
      lagTotal: topic.lagTotal,
      partitions: topic.partitions,
    };
    // onComplete callback: mark topic healthy with drained lag in topics state
    const onComplete = () => {
      setTopics((prev) => prev.map((t) =>
        t.id === topic.id
          ? { ...t, lagTotal: Math.round(t.lagTotal * (topic.status === "critical" ? 0.04 : 0.09)), status: "healthy" as KafkaTopic["status"] }
          : t
      ));
    };
    triggerTopicHeal(payload, onComplete);
  };

  const handleTopicDeleteConfirm = () => {
    if (!pendingDelete) return;
    const toDelete = pendingDelete;
    setTopics((ts) => ts.filter((t) => t.id !== toDelete.id));
    setPendingDelete(null);
    setSelectedTopic(null);
    const payload: TopicChangePayload = {
      operation: "delete",
      topic: { name: toDelete.name, partitions: toDelete.partitions, replicationFactor: toDelete.replicationFactor, retentionHours: toDelete.retentionHours },
    };
    triggerTopicAction(payload);
  };


  // Approval gate toast — shows when gates open, clears after 60s
  useEffect(() => {
    const n = state.pendingApprovals.length;
    if (n > 0) {
      setApprovalToast(n === 1 ? "🔐 1 approval gate pending — see right panel" : `🔐 ${n} approval gates pending — see right panel`);
      if (approvalToastRef.current) clearTimeout(approvalToastRef.current);
      approvalToastRef.current = setTimeout(() => setApprovalToast(null), 60_000);
    } else {
      setApprovalToast(null);
      if (approvalToastRef.current) { clearTimeout(approvalToastRef.current); approvalToastRef.current = null; }
    }
  }, [state.pendingApprovals.length]);
  return (
    <div className="h-screen overflow-hidden flex flex-col" style={{ background: "#f0f4f8" }}>

      {/* ── Nav bar — Arctic Navy ── */}
      <nav className="px-6 py-3 flex items-center justify-between sticky top-0 z-30"
           style={{ background: "#1e3a5f", boxShadow: "0 1px 0 rgba(255,255,255,0.06), 0 2px 8px rgba(0,0,0,0.18)" }}>
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg flex items-center justify-center text-white font-bold text-sm"
               style={{ background: "#1D9E75", border: "1px solid rgba(255,255,255,0.15)" }}>
            ⚡
          </div>
          <div>
            <div className="text-sm font-bold text-white tracking-tight">Agent Mesh SRE</div>
            <div className="text-[10px] font-medium" style={{ color: "#93c5fd" }}>Monitor · Reason · Act · Learn</div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {/* MRAL global phase */}
          <div className={clsx(
            "flex items-center gap-1.5 rounded-full px-3 py-1.5 border text-xs font-bold",
            MRAL_BG[phase] ?? MRAL_BG.idle
          )}>
            <div className="w-1.5 h-1.5 rounded-full" style={{ background: MRAL_DOT[phase] }} />
            {MRAL_LABELS[phase] ?? "IDLE"}
          </div>

          {/* Connection status */}
          <div className={clsx("flex items-center gap-1.5 rounded-full px-3 py-1.5 border text-xs font-semibold",
            state.connected
              ? "bg-emerald-50 border-emerald-200 text-emerald-700"
              : "bg-red-50 border-red-200 text-red-700")}>
            <div className={clsx("w-1.5 h-1.5 rounded-full",
              state.connected ? "animate-pulse bg-emerald-500" : "bg-red-500")} />
            {state.connected ? "Live" : "Reconnecting…"}
          </div>

          {/* Kafka mode — click to open Cluster Statistics panel */}
          {state.broker && (
            <button
              onClick={() => setClusterStatsOpen((o) => !o)}
              className="text-xs font-semibold rounded-full px-3 py-1.5 transition-all"
              style={{
                background: clusterStatsOpen ? "rgba(29,158,117,0.32)" : "rgba(29,158,117,0.18)",
                color: "#9ADFC8",
                border: "1px solid rgba(29,158,117,0.35)",
                cursor: "pointer",
                letterSpacing: "0.2px",
              }}
              onMouseEnter={e => (e.currentTarget.style.background = "rgba(29,158,117,0.28)")}
              onMouseLeave={e => (e.currentTarget.style.background = clusterStatsOpen ? "rgba(29,158,117,0.32)" : "rgba(29,158,117,0.18)")}
              title="View cluster statistics"
            >
              {state.broker.mode} mode ↗
            </button>
          )}

          <button onClick={reset}
            className="text-xs rounded-lg px-3 py-1.5 transition-colors"
            style={{ color: "rgba(255,255,255,0.75)", border: "1px solid rgba(255,255,255,0.18)", background: "rgba(255,255,255,0.08)" }}
            onMouseEnter={e => { (e.currentTarget.style.color = "#fff"); (e.currentTarget.style.background = "rgba(255,255,255,0.16)"); }}
            onMouseLeave={e => { (e.currentTarget.style.color = "rgba(255,255,255,0.75)"); (e.currentTarget.style.background = "rgba(255,255,255,0.08)"); }}>
            ↺ Reset
          </button>

          <UserMenu />
        </div>
      </nav>

      {/* ── Live activity banner — slides in when any agent is active ── */}
      <LiveActivityBanner agents={state.agents} />

      {/* ── Main layout ── */}
      <div className="flex flex-1 gap-0 overflow-hidden">

        {/* Left sidebar */}
        <aside className="w-72 shrink-0 flex flex-col gap-5 p-4 overflow-y-auto"
               style={{ background: "#f8fafc", borderRight: "1px solid #dce5ef" }}>

          {/* Pinned Scenarios */}
          <div>
            <div className="text-xs font-extrabold uppercase tracking-widest mb-3"
                 style={{ color: "#1e3a5f" }}>
              Common Scenarios
            </div>
            <div className="space-y-2">
              {PINNED_SCENARIOS.map((s) => (
                <button
                  key={s.id}
                  disabled={state.scenarioRunning}
                  onClick={() => handleTrigger(s.id)}
                  className="w-full text-left rounded-xl p-3.5 border transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-sm"
                  style={{ background: "#fff", borderColor: s.color + "28", borderLeftWidth: 3, borderLeftColor: s.color, ...scenarioRing(s.id, det) }}
                  onMouseEnter={e => { if (!(e.currentTarget as HTMLButtonElement).disabled) { (e.currentTarget as HTMLElement).style.background = s.color + "08"; (e.currentTarget as HTMLElement).style.boxShadow = `0 3px 12px ${s.color}18`; }}}
                  onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = "#fff"; (e.currentTarget as HTMLElement).style.boxShadow = ""; }}
                >
                  <div className="flex items-start justify-between gap-1.5">
                    <span className="text-[13px] font-bold leading-snug" style={{ color: "#1e3a5f" }}>
                      {s.label}
                    </span>
                    <span className="text-[10px] font-bold shrink-0 px-1.5 py-0.5 rounded-md"
                      style={{ background: s.color + "15", color: s.color, border: `1px solid ${s.color}35` }}>
                      {s.badge}
                    </span>
                    <DetBadge id={s.id} det={det} />
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Collapsible extra scenarios */}
          <div>
            <button
              onClick={() => setExtraOpen((o) => !o)}
              className="w-full flex items-center justify-between text-xs font-extrabold uppercase tracking-widest mb-3 transition-colors"
              style={{ color: "#1e3a5f" }}
            >
              <span>More Scenarios <span className="font-semibold normal-case text-[11px]" style={{ color: "#64748b" }}>({EXTRA_SCENARIOS.length})</span></span>
              <span className={clsx("transition-transform text-sm", extraOpen && "rotate-180")} style={{ color: "#64748b" }}>▾</span>
            </button>
            {extraOpen && (
              <div className="space-y-2">
                {EXTRA_SCENARIOS.map((s) => (
                  <button
                    key={s.id}
                    disabled={state.scenarioRunning}
                    onClick={() => handleTrigger(s.id)}
                    className="w-full text-left rounded-xl p-3 border transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-sm"
                    style={{ background: "#fff", borderColor: det.detected.has(s.id) ? s.color + "40" : "#dce5ef", ...scenarioRing(s.id, det) }}
                    onMouseEnter={e => { if (!(e.currentTarget as HTMLButtonElement).disabled) { (e.currentTarget.style.borderColor = "#1D9E75"); (e.currentTarget.style.background = "#f0faf6"); }}}
                    onMouseLeave={e => { (e.currentTarget.style.borderColor = "#dce5ef"); (e.currentTarget.style.background = "#fff"); }}
                  >
                    <div className="flex items-start justify-between gap-1.5">
                      <span className="text-[13px] font-bold leading-tight" style={{ color: "#1e3a5f" }}>
                        {s.label}
                      </span>
                      <span className="text-[10px] font-bold shrink-0 px-1.5 py-0.5 rounded-md"
                        style={{ background: s.color + "15", color: s.color, border: `1px solid ${s.color}30` }}>
                        {s.badge}
                      </span>
                      <DetBadge id={s.id} det={det} />
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Incident queue — warning yellow */}
          {state.incidentQueueDepth > 0 && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl p-3">
              <div className="text-xs font-bold text-amber-700 mb-1">⚠ Incident Queue</div>
              <div className="text-2xl font-black text-amber-600">{state.incidentQueueDepth}</div>
              <div className="text-[10px] text-amber-500">pending on ops.incidents.v1</div>
            </div>
          )}

          {/* Kafka Topics panel */}
          <TopicsPanel
            monDet={det}
            topics={topics}
            prevLagRef={prevLagRef}
            onSelect={setSelectedTopic}
            onCreateNew={() => setCreateModalOpen(true)}
            visibleCount={topicsVisible}
            onShowMore={() => setTopicsVisible(v => v + 20)}
          />
        </aside>

        {/* Middle panel — large canvas + floating overlays + scenario history */}
        <main className="flex-1 flex flex-col overflow-hidden" style={{ background: "#ffffff" }}>

          {/* 1 ── Agent canvas — large, with floating overlays ── */}
          <div className="shrink-0 relative"
               style={{
                 height: canvasHeight,
                 backgroundImage: "radial-gradient(circle, #b8cfe0 1.5px, transparent 1.5px)",
                 backgroundSize: "28px 28px",
                 background: "radial-gradient(circle, #b8cfe0 1.5px, transparent 1.5px) #eef4f9",
                 transition: "height 0.25s ease",
               }}>
            <AgentCanvas
              agents={state.agents}
              broker={state.broker}
              activeParticles={state.particles}
              onKill={(id) => agentAction(id, "kill")}
              onRestart={(id) => agentAction(id, "restart")}
            />

            {/* ── Floating Broker Info toggle ─────────────────────── */}
            <button
              onClick={() => setBrokerOpen(o => !o)}
              style={{
                position: "absolute", top: 10, left: 10, zIndex: 10,
                background: brokerOpen ? "#1e3a5f" : "rgba(30,58,95,0.82)",
                color: "#fff",
                border: "1px solid rgba(147,197,253,0.4)",
                borderRadius: 10, padding: "6px 12px",
                fontSize: 12, fontWeight: 700, cursor: "pointer",
                backdropFilter: "blur(6px)",
                boxShadow: "0 3px 12px rgba(0,0,0,0.22)",
                transition: "all 0.2s ease",
                letterSpacing: "0.3px",
              }}
            >
              {brokerOpen ? "× Close" : "⬡ Broker Info"}
            </button>

            {/* ── Canvas resize +/- buttons ────────────────────────── */}
            <div style={{
              position: "absolute", top: 10, right: 10, zIndex: 10,
              display: "flex", gap: 4,
            }}>
              {[
                { label: "−", title: "Shrink canvas", delta: -80, disabled: canvasHeight <= 380 },
                { label: "+", title: "Expand canvas", delta:  80, disabled: canvasHeight >= 960 },
              ].map(btn => (
                <button
                  key={btn.label}
                  title={btn.title}
                  disabled={btn.disabled}
                  onClick={() => setCanvasHeight(h => Math.min(960, Math.max(380, h + btn.delta)))}
                  style={{
                    width: 28, height: 28,
                    background: "rgba(30,58,95,0.78)",
                    color: btn.disabled ? "rgba(255,255,255,0.3)" : "#fff",
                    border: "1px solid rgba(147,197,253,0.35)",
                    borderRadius: 8,
                    fontSize: 16, fontWeight: 700,
                    lineHeight: "1",
                    cursor: btn.disabled ? "default" : "pointer",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    backdropFilter: "blur(6px)",
                    boxShadow: "0 2px 8px rgba(0,0,0,0.20)",
                    transition: "background 0.15s",
                  }}
                >
                  {btn.label}
                </button>
              ))}
            </div>

            {/* ── Broker details popup ────────────────────────────── */}
            {brokerOpen && state.broker && (
              <div style={{
                position: "absolute", top: 48, left: 10, zIndex: 20,
                background: "#fff",
                border: "2px solid #3b82f6",
                borderRadius: 16, padding: "16px 18px",
                boxShadow: "0 10px 32px rgba(59,130,246,0.22)",
                minWidth: 210,
                animation: "slideDown 0.2s ease-out",
              }}>
                <div style={{ fontSize: 14, fontWeight: 800, color: "#3b82f6", textTransform: "uppercase", letterSpacing: "0.8px", marginBottom: 12 }}>
                  ⬡ BROKER
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
                  {[
                    { label: "Mode",             value: state.broker.mode,               valueColor: "#1e3a5f" },
                    { label: "Brokers Online",    value: String(state.broker.brokersOnline), valueColor: "#16a34a" },
                    { label: "Controller Epoch",  value: String(state.broker.controllerEpoch), valueColor: "#1e3a5f" },
                    { label: "Topics",            value: String(Object.keys(state.broker.topics).length), valueColor: "#2563eb" },
                  ].map(row => (
                    <div key={row.label} style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{ fontSize: 12, color: "#64748b" }}>{row.label}</span>
                      <span style={{ fontSize: 12, fontWeight: 700, color: row.valueColor }}>{row.value}</span>
                    </div>
                  ))}
                  <div style={{ borderTop: "1px solid #e2e8f0", paddingTop: 8, marginTop: 2 }}>
                    <span style={{
                      fontSize: 12, fontWeight: 700,
                      color: (state.broker.mtls !== false || state.broker.sasl !== false) ? "#16a34a" : "#f97316",
                    }}>
                      {state.broker.mtls !== false ? "mTLS + SASL" : "SASL"} {(state.broker.mtls !== false || state.broker.sasl !== false) ? "✓ Secure" : "✗ Unsecured"}
                    </span>
                  </div>
                </div>
              </div>
            )}

          </div>

          {/* Notification bar removed — scenario results go straight to history list */}

          {/* 3 ── Live feed (while running) or Scenario history ──────────── */}
          <div className="flex-1 overflow-hidden min-h-0 flex flex-col" style={{ background: "#f8fafc", borderTop: "1px solid #dce5ef" }}>
            {state.scenarioRunning && !state.emailSummary ? (
              /* ── Live feed fills the area while scenario is active ── */
              <>
                <div style={{
                  display: "flex", alignItems: "center", gap: 8, padding: "10px 16px",
                  background: "#f0f4f8", borderBottom: "1px solid #dce5ef", flexShrink: 0,
                }}>
                  <span style={{
                    width: 9, height: 9, borderRadius: "50%", background: "#1D9E75",
                    display: "inline-block", animation: "pulse 2s infinite", flexShrink: 0,
                  }} />
                  <span style={{ fontSize: 12, fontWeight: 800, color: "#1e3a5f", textTransform: "uppercase", letterSpacing: "0.7px" }}>
                    Live Events Feed
                  </span>
                  <span style={{ fontSize: 12, color: "#94a3b8" }}>· {state.auditLog.length} events</span>
                </div>
                <div style={{ flex: 1, overflowY: "auto", padding: "8px 12px", display: "flex", flexDirection: "column", gap: 5 }}>
                  {[...state.auditLog].reverse().map((r) => {
                    const color = AUDIT_COLOR[r.type] ?? "#94a3b8";
                    return (
                      <div key={r.id} style={{
                        display: "flex", alignItems: "flex-start", gap: 8, padding: "7px 10px",
                        borderRadius: 10, background: color + "07", border: `1px solid ${color}22`,
                      }}>
                        <span style={{
                          fontSize: 9, fontWeight: 700, padding: "2px 6px", borderRadius: 4,
                          background: color + "18", color, border: `1px solid ${color}30`,
                          flexShrink: 0, textTransform: "uppercase", marginTop: 1,
                        }}>
                          {r.type}
                        </span>
                        <span style={{ fontSize: 11, color: "#94a3b8", fontWeight: 600, flexShrink: 0 }}>
                          [{r.agent}]
                        </span>
                        <span style={{ fontSize: 12, color: "#334155", lineHeight: 1.45 }}>
                          {r.summary}
                        </span>
                      </div>
                    );
                  })}
                  {state.auditLog.length === 0 && (
                    <p style={{ fontSize: 12, color: "#94a3b8", textAlign: "center", padding: "20px 0", fontStyle: "italic" }}>
                      Waiting for events…
                    </p>
                  )}
                </div>
              </>
            ) : summaryHistory.length > 0 ? (
              /* ── History list after scenario ends ── */
              <div className="overflow-y-auto flex-1">
                <ScenarioHistoryBar history={summaryHistory} onView={setViewHistorySummary} />
              </div>
            ) : (
              /* ── Empty state ── */
              <div className="flex flex-col items-center justify-center h-full gap-3 py-8">
                <div style={{ fontSize: 32, opacity: 0.2 }}>📋</div>
                <p className="text-sm font-medium" style={{ color: "#94a3b8" }}>
                  Trigger a scenario to see live events here
                </p>
                <p style={{ fontSize: 11, color: "#b0bec8" }}>
                  Each completed cycle adds a clickable one-line summary below
                </p>
              </div>
            )}
          </div>
        </main>

        {/* Right sidebar — audit log */}
        <aside className="w-80 shrink-0 flex flex-col p-4 overflow-hidden"
               style={{ background: "#f8fafc", borderLeft: "1px solid #dce5ef" }}>
          {/* ── Pending Approvals panel (always visible) ── */}
          <div id="pending-approvals-panel" style={{
            marginBottom:12, paddingBottom:12, borderBottom:"1px solid #e2e8f0",
          }}>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:8}}>
              <span style={{
                width:8,height:8,borderRadius:"50%",display:"inline-block",
                background:state.pendingApprovals.length>0?"#f59e0b":"#22d3ee",
              }}/>
              <span style={{fontSize:11,fontWeight:800,color:"#1e3a5f",
                letterSpacing:"0.06em",textTransform:"uppercase"}}>
                Pending Approvals
              </span>
              {state.pendingApprovals.length>0&&(
                <span style={{marginLeft:4,background:"#fef3c7",color:"#92400e",
                  borderRadius:20,padding:"1px 8px",fontWeight:700,fontSize:10,
                  border:"1px solid #fcd34d"}}>{state.pendingApprovals.length}</span>
              )}
            </div>
            {state.pendingApprovals.length===0?(
              <div style={{textAlign:"center",padding:"8px 0"}}>
                <p style={{fontSize:12,color:"#94a3b8"}}>✓ No pending approvals</p>
                <p style={{fontSize:10,color:"#b0bec8",marginTop:2}}>Approval gates appear here automatically</p>
              </div>
            ):(state.pendingApprovals.map((ap,idx)=>(
              <div key={ap.id} style={{
                border:`1px solid ${idx===0?"#fcd34d":"#e2e8f0"}`,
                borderLeft:`3px solid ${idx===0?"#f59e0b":"#94a3b8"}`,
                borderRadius:10,padding:"10px 12px",marginBottom:8,
                background:idx===0?"#fffbeb":"#f8fafc",
              }}>
                <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:6}}>
                  <span style={{fontSize:12}}>{idx===0?"🔐":"⏳"}</span>
                  <span style={{fontSize:12,fontWeight:700,color:"#1e3a5f",flex:1}}>
                    {ap.scenarioId?.replace(/-/g," ").replace(/\b\w/g,(c:string)=>c.toUpperCase())||"Approval Required"}
                  </span>
                  <span style={{fontSize:10,fontWeight:700,padding:"2px 6px",borderRadius:20,
                    background:idx===0?"#fef3c7":"#f1f5f9",color:idx===0?"#92400e":"#64748b",
                    border:`1px solid ${idx===0?"#fcd34d":"#e2e8f0"}`}}>
                    {idx===0?"Active":`#${idx+1}`}
                  </span>
                </div>
                {ap.reason&&(
                  <p style={{fontSize:11,color:"#64748b",marginBottom:8,lineHeight:1.4,
                    background:"#f1f5f9",padding:"4px 8px",borderRadius:6}}>
                    {String(ap.reason).slice(0,110)}{String(ap.reason).length>110?"…":""}
                  </p>
                )}
                <div style={{display:"flex",gap:6}}>
                  <button onClick={()=>approve(ap.id,"approve")} style={{
                    flex:1,padding:"6px 0",borderRadius:7,border:"none",
                    cursor:"pointer",background:"#16a34a",color:"#fff",fontWeight:700,fontSize:11
                  }}>✓ Approve</button>
                  <button onClick={()=>approve(ap.id,"reject")} style={{
                    flex:1,padding:"6px 0",borderRadius:7,border:"1px solid #e2e8f0",
                    cursor:"pointer",background:"#fff",color:"#dc2626",fontWeight:700,fontSize:11
                  }}>✗ Reject</button>
                </div>
              </div>
            )))}
          </div>
          <div className="flex-1 overflow-hidden">
            <AuditLogPanel log={state.auditLog} />
          </div>

          {/* Lessons learned — emerald Arctic */}
          {state.lessons.length > 0 && (
            <div className="shrink-0 mt-4 pt-3" style={{ borderTop: "1px solid #dce5ef" }}>
              <div className="flex items-center gap-1.5 mb-3">
                <span className="w-2 h-2 rounded-full" style={{ background: "#1D9E75" }} />
                <span className="text-xs font-bold uppercase tracking-widest"
                     style={{ color: "#1e3a5f", opacity: 0.7 }}>
                  Lessons Learned
                </span>
                <span className="text-xs font-normal ml-1" style={{ color: "#94a3b8" }}>({state.lessons.length})</span>
              </div>
              <div className="space-y-2 max-h-44 overflow-y-auto">
                {[...state.lessons].reverse().slice(0, 6).map((l) => (
                  <div key={l.id}
                    className="rounded-xl px-3 py-3"
                    style={{ background: "linear-gradient(135deg,#e6f5f0,#f0faf7)", border: "1px solid #a3d9c8" }}>
                    <div className="text-xs font-bold truncate mb-1" style={{ color: "#0F6E56" }}>
                      [{l.scenarioId}] {l.actionTaken}
                    </div>
                    <div className="text-xs leading-relaxed" style={{ color: "#475569" }}>
                      {l.notes.slice(0, 90)}{l.notes.length > 90 ? "…" : ""}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </aside>
      </div>

      {/* Overlays */}
      {console.log("[Dashboard] pendingApprovals:", state.pendingApprovals)}
      {/* ── Approval gate toast ── */}
      {approvalToast && (
        <div style={{
          position:"fixed", bottom:80, left:"50%", transform:"translateX(-50%)",
          zIndex:9999, background:"#1e3a5f", color:"#fff",
          padding:"11px 20px", borderRadius:12,
          boxShadow:"0 4px 24px rgba(0,0,0,0.35)",
          fontSize:13, fontWeight:600,
          display:"flex", alignItems:"center", gap:10,
          border:"1px solid #f59e0b",
        }}>
          <span>{approvalToast}</span>
          <button onClick={()=>setApprovalToast(null)}
            style={{background:"none",border:"none",color:"#94a3b8",
              cursor:"pointer",fontSize:18,lineHeight:1,padding:0}}>×</button>
        </div>
      )}
      {/* <ApprovalGate approvals={state.pendingApprovals} onDecide={approve} /> */}
      { /* ScenarioEndModal suppressed — review via Scenario History bar */ }
      {viewHistorySummary && (
        <ScenarioEndModal data={viewHistorySummary} onClose={() => setViewHistorySummary(null)} />
      )}
      {selectedTopic && !pendingDelete && (
        <TopicModal
          topic={selectedTopic}
          scenarioRunning={state.scenarioRunning}
          onClose={() => setSelectedTopic(null)}
          onSave={handleTopicSave}
          onDelete={(t) => setPendingDelete(t)}
          onHeal={handleTopicHeal}
          onCopy={(t) => { setCopyTemplate(t); setSelectedTopic(null); }}
        />
      )}
      {pendingDelete && (
        <DeleteConfirmModal
          topic={pendingDelete}
          onConfirm={handleTopicDeleteConfirm}
          onCancel={() => setPendingDelete(null)}
        />
      )}
      {(createModalOpen || !!copyTemplate) && (
        <CreateTopicModal
          existingTopics={topics}
          templateTopic={copyTemplate ?? undefined}
          onClose={() => { setCreateModalOpen(false); setCopyTemplate(null); }}
          onCreate={handleTopicCreate}
        />
      )}
      {clusterStatsOpen && (
        <ClusterStatsModal
          modeInfo={modeInfo}
          broker={state.broker}
          onClose={() => setClusterStatsOpen(false)}
        />
      )}
      <ToastStack toasts={state.toasts} />
    </div>
  );
}
