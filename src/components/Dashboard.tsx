"use client";

import dynamic from "next/dynamic";
import { useMeshStream } from "./useMeshStream";
import type { ApprovalRequest, AuditRecord, MCPToolCall, AgentState } from "@/lib/types";
import type { EmailSummaryData } from "./useMeshStream";
import clsx from "clsx";
import { useSession, signOut } from "next-auth/react";
import { useState } from "react";

const AgentCanvas = dynamic(() => import("./AgentCanvas"), { ssr: false });

// ── Scenario definitions ──────────────────────────────────────────────────────

const SCENARIOS = [
  { id: "lag-spike",           label: "Consumer Lag Spike",         badge: "KIP-848", color: "#2563eb" },
  { id: "controller-failover", label: "KRaft Controller Failover",  badge: "KRaft",   color: "#7c3aed" },
  { id: "share-group",         label: "Share Group Rebalance",      badge: "KIP-932", color: "#ea580c" },
  { id: "benign-rebalance",    label: "False-Positive Suppression", badge: "KIP-848", color: "#16a34a" },
] as const;

// ── Semantic colour maps ──────────────────────────────────────────────────────

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

// ── Toast stack ───────────────────────────────────────────────────────────────

function ToastStack({ toasts }: { toasts: { id: number; message: string; kind: string }[] }) {
  const styles: Record<string, { bg: string; border: string; text: string; dot: string }> = {
    info:    { bg: "bg-blue-50",   border: "border-blue-200",  text: "text-blue-800",  dot: "bg-blue-500"  },
    success: { bg: "bg-green-50",  border: "border-green-200", text: "text-green-800", dot: "bg-green-500" },
    warning: { bg: "bg-amber-50",  border: "border-amber-200", text: "text-amber-800", dot: "bg-amber-500" },
    error:   { bg: "bg-red-50",    border: "border-red-200",   text: "text-red-700",   dot: "bg-red-500"   },
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
            <span className="text-xs text-slate-500 truncate flex-1">{detail}</span>
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

// ── Scenario-end modal (email sent / rejection notice) ────────────────────────

function ScenarioEndModal({ data, onClose }: { data: EmailSummaryData; onClose: () => void }) {
  if (!data.approved) {
    // Rejection path
    return (
      <div className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm flex items-center justify-center p-4
                      animate-[fadeIn_0.2s_ease-out]">
        <div className="bg-white rounded-2xl shadow-2xl max-w-sm w-full overflow-hidden animate-[slideUp_0.25s_ease-out]">
          <div className="bg-red-50 border-b border-red-200 px-6 py-5 text-center">
            <div className="text-4xl mb-2">🚫</div>
            <h2 className="text-lg font-bold text-red-700">Action Rejected</h2>
            <p className="text-sm text-red-600 mt-1">No further action was taken</p>
          </div>
          <div className="px-6 py-5">
            <div className="bg-slate-50 rounded-xl border border-slate-200 px-4 py-3 mb-5 space-y-1.5">
              <div className="flex justify-between text-xs">
                <span className="text-slate-500 font-medium">Scenario</span>
                <span className="text-slate-700 font-semibold">{data.scenarioLabel}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-500 font-medium">Proposed action</span>
                <span className="text-slate-700 font-mono">{data.action}</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-500 font-medium">Cluster modified</span>
                <span className="font-bold text-red-600">No</span>
              </div>
              <div className="flex justify-between text-xs">
                <span className="text-slate-500 font-medium">Email sent</span>
                <span className="font-bold text-slate-500">No</span>
              </div>
            </div>
            <button onClick={onClose}
              className="w-full py-3 rounded-xl bg-slate-800 hover:bg-slate-900 text-white font-bold text-sm transition-colors">
              OK, understood
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Approved path — show email result
  const emailOk    = data.sent;
  const emailLabel = emailOk
    ? "Sent to surajcs@gmail.com"
    : data.emailError === "smtp_not_configured"
    ? "Skipped — SMTP not configured in Vercel"
    : data.emailError === "network_error"
    ? "Failed — network error"
    : `Failed — ${data.emailError ?? "unknown"}`;

  return (
    <div className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm flex items-center justify-center p-4
                    animate-[fadeIn_0.2s_ease-out]">
      <div className="bg-white rounded-2xl shadow-2xl max-w-md w-full overflow-hidden animate-[slideUp_0.25s_ease-out]">
        {/* Header */}
        <div className="bg-gradient-to-r from-blue-600 to-blue-700 px-6 py-5">
          <div className="flex items-center gap-3">
            <div className="w-11 h-11 bg-white/20 rounded-xl flex items-center justify-center text-2xl">
              {emailOk ? "✉️" : "✅"}
            </div>
            <div>
              <h2 className="text-base font-bold text-white">Scenario Complete</h2>
              <p className="text-xs text-blue-200 mt-0.5">{data.scenarioLabel}</p>
            </div>
          </div>
        </div>

        {/* Body */}
        <div className="px-6 py-5">
          <div className="rounded-xl border border-slate-200 overflow-hidden mb-5">
            <table className="w-full text-sm">
              <tbody>
                <tr className="border-b border-slate-100">
                  <td className="px-4 py-2.5 text-xs text-slate-500 font-medium bg-slate-50 w-32">Action taken</td>
                  <td className="px-4 py-2.5 text-xs font-mono text-slate-800">{data.action}</td>
                </tr>
                {data.lagBefore > 0 && (
                  <tr className="border-b border-slate-100">
                    <td className="px-4 py-2.5 text-xs text-slate-500 font-medium bg-slate-50">Lag resolved</td>
                    <td className="px-4 py-2.5 text-xs font-bold text-slate-800">
                      {data.lagBefore.toLocaleString()} → {data.lagAfter.toLocaleString()} msgs
                    </td>
                  </tr>
                )}
                <tr className="border-b border-slate-100">
                  <td className="px-4 py-2.5 text-xs text-slate-500 font-medium bg-slate-50">Outcome</td>
                  <td className="px-4 py-2.5">
                    <span className="inline-flex items-center gap-1 text-xs font-bold text-emerald-700
                                     bg-emerald-50 border border-emerald-200 rounded-full px-2.5 py-0.5">
                      ✅ SUCCESS
                    </span>
                  </td>
                </tr>
                {data.approvedBy && (
                  <tr className="border-b border-slate-100">
                    <td className="px-4 py-2.5 text-xs text-slate-500 font-medium bg-slate-50">Approved by</td>
                    <td className="px-4 py-2.5 text-xs text-slate-700">{data.approvedBy}</td>
                  </tr>
                )}
                <tr>
                  <td className="px-4 py-2.5 text-xs text-slate-500 font-medium bg-slate-50">Email</td>
                  <td className="px-4 py-2.5">
                    <span className={clsx(
                      "text-xs font-medium",
                      emailOk ? "text-emerald-600" : "text-amber-600"
                    )}>
                      {emailOk ? "✉️ " : "⚠️ "}{emailLabel}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          {!emailOk && data.emailError === "smtp_not_configured" && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl px-4 py-3 mb-4 text-xs text-amber-700">
              <strong>To enable emails:</strong> add SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS and
              NOTIFICATION_EMAIL in Vercel → Settings → Environment Variables, then redeploy.
            </div>
          )}

          <button onClick={onClose}
            className="w-full py-3 rounded-xl bg-blue-600 hover:bg-blue-700 text-white font-bold text-sm transition-colors shadow-sm">
            Got it
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
        className="flex items-center gap-2 rounded-full border border-blue-200 bg-white pl-1.5 pr-3 py-1
                   hover:border-blue-400 hover:bg-blue-50 transition-colors shadow-sm">
        {image
          ? <img src={image} alt={name} referrerPolicy="no-referrer" className="w-6 h-6 rounded-full" />
          : <div className="w-6 h-6 rounded-full bg-blue-600 flex items-center justify-center text-[9px] font-bold text-white">
              {initials}
            </div>
        }
        <span className="text-xs text-slate-700 font-medium max-w-[100px] truncate hidden sm:block">{name}</span>
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1.5 w-56 rounded-xl border border-slate-200 bg-white shadow-xl z-50">
          <div className="px-4 py-3 border-b border-slate-100">
            <div className="text-xs font-semibold text-slate-800 truncate">{name}</div>
            <div className="text-[10px] text-slate-400 truncate">{email}</div>
          </div>
          <button onClick={() => signOut({ callbackUrl: "/login" })}
            className="w-full text-left px-4 py-2.5 text-xs text-slate-600 hover:bg-slate-50 hover:text-slate-900 rounded-b-xl transition-colors">
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
      <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-2 px-1">
        Audit Log <span className="text-slate-300 font-normal normal-case">({log.length})</span>
      </div>
      <div className="flex-1 overflow-y-auto space-y-1 pr-0.5">
        {[...log].reverse().map((r) => (
          <div key={r.id} className="flex gap-2 items-start text-[10px] leading-snug py-0.5">
            <span className="shrink-0 font-bold uppercase w-14 text-right"
              style={{ color: AUDIT_COLOR[r.type] ?? "#94a3b8" }}>
              {r.type.slice(0, 7)}
            </span>
            <span className="text-slate-600 truncate">[{r.agent}] {r.summary}</span>
          </div>
        ))}
        {log.length === 0 && (
          <p className="text-[11px] text-slate-400 italic px-1 pt-2">No events yet — trigger a scenario.</p>
        )}
      </div>
    </div>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const { state, trigger, approve, agentAction, reset, dismissEmailSummary } = useMeshStream();
  const phase = state.mralPhase ?? "idle";

  return (
    <div className="min-h-screen bg-slate-100 flex flex-col">

      {/* ── Nav bar — professional deep blue ── */}
      <nav className="bg-blue-800 px-6 py-3 flex items-center justify-between sticky top-0 z-30 shadow-md">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-white/15 rounded-lg flex items-center justify-center text-white font-bold text-sm border border-white/20">
            ⚡
          </div>
          <div>
            <div className="text-sm font-bold text-white tracking-tight">Agent Mesh SRE</div>
            <div className="text-[10px] text-blue-200 font-medium">Monitor · Reason · Act · Learn</div>
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
              ? "bg-green-50 border-green-200 text-green-700"
              : "bg-red-50 border-red-200 text-red-700")}>
            <div className={clsx("w-1.5 h-1.5 rounded-full",
              state.connected ? "animate-pulse bg-green-500" : "bg-red-500")} />
            {state.connected ? "Live" : "Reconnecting…"}
          </div>

          {/* Kafka mode */}
          {state.broker && (
            <div className="text-xs font-semibold text-blue-700 bg-blue-50 border border-blue-200 rounded-full px-3 py-1.5">
              {state.broker.mode} mode
            </div>
          )}

          <button onClick={reset}
            className="text-xs text-white/80 hover:text-white border border-white/20 hover:border-white/40
                       bg-white/10 hover:bg-white/20 rounded-lg px-3 py-1.5 transition-colors">
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
        <aside className="w-64 shrink-0 bg-white border-r border-slate-200 flex flex-col gap-5 p-4 overflow-y-auto shadow-sm">

          {/* Scenarios */}
          <div>
            <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-3">
              Scenarios
            </div>
            <div className="space-y-2">
              {SCENARIOS.map((s) => (
                <button
                  key={s.id}
                  disabled={state.scenarioRunning}
                  onClick={() => trigger(s.id)}
                  className="w-full text-left rounded-xl p-3 border transition-all
                             disabled:opacity-40 disabled:cursor-not-allowed
                             bg-white border-slate-200 hover:border-blue-400 hover:bg-blue-50
                             shadow-sm group"
                >
                  <div className="flex items-start justify-between gap-1.5">
                    <span className="text-xs font-semibold text-slate-700 group-hover:text-blue-800 leading-tight">
                      {s.label}
                    </span>
                    <span className="text-[9px] font-bold shrink-0 px-1.5 py-0.5 rounded-md"
                      style={{ background: s.color + "15", color: s.color, border: `1px solid ${s.color}30` }}>
                      {s.badge}
                    </span>
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Broker panel */}
          {state.broker && (
            <div>
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-3">
                Broker
              </div>
              <div className="space-y-1.5 bg-slate-50 rounded-xl p-3 border border-slate-200">
                {Object.entries(state.broker.topics).map(([topic, t]) => (
                  <div key={topic} className="flex items-center justify-between gap-2">
                    <span className="text-[9px] text-slate-500 truncate">{topic.split(".").pop()}</span>
                    <span className={clsx("text-[9px] font-bold",
                      (t as { lag: number }).lag > 0 ? "text-amber-600" : "text-slate-400")}>
                      lag {(t as { lag: number }).lag.toLocaleString()}
                    </span>
                  </div>
                ))}
                <div className="border-t border-slate-200 mt-2 pt-2 space-y-1">
                  {Object.entries(state.broker.consumerGroups).map(([group, g]) => (
                    <div key={group} className="flex items-center justify-between">
                      <span className="text-[9px] text-slate-500 truncate">{group}</span>
                      <span className={clsx("text-[9px] font-bold",
                        (g as { lag: number }).lag > 5000 ? "text-red-600" :
                        (g as { lag: number }).lag > 0   ? "text-amber-600" : "text-emerald-600")}>
                        {(g as { lag: number }).lag.toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Incident queue — warning yellow */}
          {state.incidentQueueDepth > 0 && (
            <div className="bg-amber-50 border border-amber-200 rounded-xl p-3">
              <div className="text-xs font-bold text-amber-700 mb-1">⚠ Incident Queue</div>
              <div className="text-2xl font-black text-amber-600">{state.incidentQueueDepth}</div>
              <div className="text-[10px] text-amber-500">pending on ops.incidents.v1</div>
            </div>
          )}
        </aside>

        {/* Canvas */}
        <main className="flex-1 flex flex-col overflow-hidden bg-slate-50">
          <div className="flex-1 p-4 min-h-0">
            <AgentCanvas
              agents={state.agents}
              broker={state.broker}
              activeParticles={state.particles}
              onKill={(id) => agentAction(id, "kill")}
              onRestart={(id) => agentAction(id, "restart")}
            />
          </div>

          {/* Notifications strip — light green */}
          {state.notifications.length > 0 && (
            <div className="shrink-0 border-t border-green-100 bg-green-50 px-4 py-2">
              <div className="flex gap-2 overflow-x-auto pb-0.5">
                {[...state.notifications].reverse().slice(0, 5).map((n) => (
                  <div key={n.id}
                    className="shrink-0 flex items-center gap-2 bg-white border border-green-200
                               rounded-lg px-3 py-1.5 text-xs shadow-sm">
                    <span>{n.channel === "slack" ? "💬" : n.channel === "itsm" ? "🎫" : "✉️"}</span>
                    <span className="text-green-800 font-medium max-w-[260px] truncate">{n.message}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </main>

        {/* Right sidebar — audit log + lessons */}
        <aside className="w-72 shrink-0 bg-white border-l border-slate-200 flex flex-col p-4 overflow-hidden shadow-sm">
          <div className="flex-1 overflow-hidden">
            <AuditLogPanel log={state.auditLog} />
          </div>

          {/* Lessons learned — light cyan */}
          {state.lessons.length > 0 && (
            <div className="shrink-0 mt-4 border-t border-slate-200 pt-3">
              <div className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-2">
                Lessons Learned ({state.lessons.length})
              </div>
              <div className="space-y-1.5 max-h-36 overflow-y-auto">
                {[...state.lessons].reverse().slice(0, 5).map((l) => (
                  <div key={l.id}
                    className="text-[10px] bg-cyan-50 rounded-lg px-2.5 py-2 border border-cyan-100">
                    <div className="text-cyan-700 font-semibold truncate">[{l.scenarioId}] {l.actionTaken}</div>
                    <div className="text-slate-500 truncate mt-0.5">{l.notes.slice(0, 60)}…</div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </aside>
      </div>

      {/* Overlays */}
      <ApprovalGate approvals={state.pendingApprovals} onDecide={approve} />
      {state.emailSummary && (
        <ScenarioEndModal data={state.emailSummary} onClose={dismissEmailSummary} />
      )}
      <ToastStack toasts={state.toasts} />
    </div>
  );
}
