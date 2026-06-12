"use client";

import { useState } from "react";
import { useMesh } from "@/lib/store";
import { postJson } from "@/lib/sse-client";
import { cn, relTime } from "@/lib/utils";
import { AGENTS } from "@/lib/agents-config";
import { Check, ChevronRight, Clock, ShieldAlert, X } from "lucide-react";
import { JsonPretty } from "../ui/JsonPretty";
import type { ApprovalRequest } from "@/lib/types";

// ── Detail / Approve-Reject modal ─────────────────────────────────────────────

function ApprovalDetailModal({
  approval,
  onClose,
  onDecide,
}: {
  approval: ApprovalRequest;
  onClose: () => void;
  onDecide: (id: string, decision: "approved" | "rejected") => Promise<void>;
}) {
  const [deciding, setDeciding] = useState<"approved" | "rejected" | null>(null);

  async function handle(decision: "approved" | "rejected") {
    setDeciding(decision);
    await onDecide(approval.id, decision);
    onClose();
  }

  const agentName = AGENTS[approval.proposedBy ?? approval.agent]?.name ?? (approval.proposedBy ?? approval.agent);

  return (
    /* Backdrop */
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      {/* Panel */}
      <div className="relative w-full max-w-xl mx-4 rounded-2xl border border-amber-500/40 bg-[#0f1624] shadow-2xl shadow-black/60 flex flex-col max-h-[90vh]">

        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-white/10">
          <div className="flex items-center gap-2">
            <ShieldAlert size={15} className="text-amber-300 shrink-0" />
            <span className="text-[13px] font-semibold text-amber-200 tracking-wide">
              Policy Gate — Awaiting Decision
            </span>
          </div>
          <button
            onClick={onClose}
            className="text-fg-dim hover:text-fg-base transition-colors p-1 rounded-md hover:bg-white/10"
            aria-label="Close"
          >
            <X size={14} />
          </button>
        </div>

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">

          {/* Meta row */}
          <div className="grid grid-cols-2 gap-3">
            <MetaItem label="Approval ID" value={approval.id} mono />
            <MetaItem label="Requested" value={relTime(approval.createdAt)} />
            <MetaItem label="Proposed by" value={agentName} />
            <MetaItem label="Scenario" value={approval.scenarioId} mono />
          </div>

          {/* Reason / rationale */}
          {approval.reason && (
            <Section title="Rationale">
              <p className="text-[12px] text-fg-muted leading-relaxed">{approval.reason}</p>
            </Section>
          )}

          {/* Tool call */}
          <Section title="Proposed MCP Tool Call">
            <div className="flex items-center gap-2 mb-2">
              <span className="tag !text-violet-300 !border-violet-500/40 font-mono">
                {approval.toolCall.params.name}
              </span>
              <span className="text-[10px] font-mono text-fg-dim">
                id: {approval.toolCall.id}
              </span>
            </div>
            <JsonPretty value={approval.toolCall.params.arguments} />
          </Section>

          {/* Full raw payload */}
          <Section title="Full JSON-RPC Payload">
            <JsonPretty value={approval.toolCall} />
          </Section>

          {/* Agent info */}
          {AGENTS[approval.agent] && (
            <Section title="Requesting Agent">
              <div className="text-[12px] text-fg-muted space-y-0.5">
                <div>
                  <span className="text-fg-dim">Name: </span>
                  <span className="text-fg-base">{AGENTS[approval.agent].name}</span>
                </div>
                <div>
                  <span className="text-fg-dim">Role: </span>
                  <span className="text-fg-base">{AGENTS[approval.agent].role}</span>
                </div>
              </div>
            </Section>
          )}

          {/* Warning banner */}
          <div className="rounded-lg border border-amber-500/30 bg-amber-500/[0.05] px-3 py-2.5">
            <p className="text-[11px] text-amber-200/80 leading-relaxed">
              <strong className="text-amber-300">This action mutates live infrastructure.</strong>{" "}
              Approving will immediately execute the tool call above on the connected Kafka cluster.
              Rejection is fully audited and no changes will be made.
            </p>
          </div>
        </div>

        {/* Footer actions */}
        <div className="flex gap-3 px-5 py-4 border-t border-white/10">
          <button
            onClick={() => handle("rejected")}
            disabled={deciding !== null}
            className="btn btn-danger flex-1 justify-center gap-1.5 py-2"
          >
            {deciding === "rejected" ? (
              <Clock size={13} className="animate-spin" />
            ) : (
              <X size={13} />
            )}
            Reject
          </button>
          <button
            onClick={() => handle("approved")}
            disabled={deciding !== null}
            className="btn btn-success flex-1 justify-center gap-1.5 py-2"
          >
            {deciding === "approved" ? (
              <Clock size={13} className="animate-spin" />
            ) : (
              <Check size={13} />
            )}
            Approve
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Sub-components ────────────────────────────────────────────────────────────

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-[10px] font-mono uppercase tracking-widest text-fg-dim mb-1.5">{title}</div>
      <div className="rounded-lg border border-white/10 bg-bg-elev p-3">{children}</div>
    </div>
  );
}

function MetaItem({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="rounded-lg border border-white/10 bg-bg-elev px-3 py-2">
      <div className="text-[10px] text-fg-dim mb-0.5">{label}</div>
      <div className={cn("text-[11.5px] text-fg-base truncate", mono && "font-mono")}>{value}</div>
    </div>
  );
}

function SectionHeader({ icon, title, subtitle }: { icon: React.ReactNode; title: string; subtitle?: string }) {
  return (
    <div className="px-3 py-2.5 border-b border-white/10">
      <div className="flex items-center gap-1.5">
        {icon}
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base">{title}</div>
      </div>
      {subtitle && <div className="text-[11px] text-fg-muted mt-1">{subtitle}</div>}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export function ApprovalQueue() {
  const approvals = useMesh((s) => s.approvals);
  const [reviewing, setReviewing] = useState<ApprovalRequest | null>(null);

  const pending = approvals.filter((a) => a.status === "pending");
  const past = approvals.filter((a) => a.status !== "pending").slice(-5);

  async function decide(id: string, decision: "approved" | "rejected") {
    await postJson("/api/approvals", { id, decision, actor: "vp-engineering@stage" });
  }

  return (
    <>
      <div className="flex flex-col h-full">
        <SectionHeader
          icon={<ShieldAlert size={13} className="text-amber-300" />}
          title="Policy approval gates"
          subtitle="Every infra-mutating MCP tool call routes through here. Human-in-the-loop, audit-ready."
        />

        <div className="flex-1 overflow-y-auto px-3 py-2 space-y-2">
          {pending.length === 0 && past.length === 0 && (
            <div className="text-[12px] text-fg-dim italic px-2 py-6 text-center">
              No approvals yet. Run a scenario that triggers a policy-gated tool call.
            </div>
          )}

          {/* ── Pending ── */}
          {pending.map((a) => (
            <div
              key={a.id}
              className="rounded-xl border border-amber-500/40 bg-amber-500/[0.06] p-3 animate-pulse-once"
            >
              <div className="flex items-start justify-between gap-2 mb-1.5">
                <div className="text-[11.5px] font-semibold text-amber-200 flex items-center gap-1.5">
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
                  PENDING — requires approval
                </div>
                <div className="text-[10px] font-mono text-fg-dim shrink-0">{relTime(a.createdAt)}</div>
              </div>

              <div className="text-[11.5px] text-fg-muted mb-1">
                Proposed by{" "}
                <span className="text-fg-base">
                  {AGENTS[a.proposedBy ?? a.agent]?.name ?? (a.proposedBy ?? a.agent)}
                </span>
              </div>

              <div className="font-mono text-[11px] text-violet-300 bg-violet-500/10 border border-violet-500/20 rounded px-2 py-1 mb-3 truncate">
                {a.toolCall.params.name}
              </div>

              <button
                onClick={() => setReviewing(a)}
                className="btn w-full justify-center gap-1.5 border border-amber-500/50 bg-amber-500/10 text-amber-200 hover:bg-amber-500/20 transition-colors text-[12px] py-1.5"
              >
                Review &amp; Decide
                <ChevronRight size={13} />
              </button>
            </div>
          ))}

          {/* ── Past ── */}
          {past.map((a) => (
            <div key={a.id} className="rounded-lg border border-white/10 bg-bg-elev p-2.5">
              <div className="flex items-center justify-between text-[11px]">
                <span
                  className={cn(
                    "tag",
                    a.status === "approved"
                      ? "!text-emerald-300 !border-emerald-500/40"
                      : "!text-rose-300 !border-rose-500/40"
                  )}
                >
                  {a.status.toUpperCase()}
                </span>
                <span className="text-fg-dim font-mono text-[10px]">
                  {relTime(a.decidedAt ?? a.createdAt)}
                </span>
              </div>
              <div className="text-[11px] text-fg-muted mt-1.5 truncate">
                {a.toolCall.params.name}
                {a.decidedBy && <span className="text-fg-dim"> · {a.decidedBy}</span>}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Modal — rendered outside the scroll container */}
      {reviewing && (
        <ApprovalDetailModal
          approval={reviewing}
          onClose={() => setReviewing(null)}
          onDecide={decide}
        />
      )}
    </>
  );
}
