"use client";

import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import {
  Activity,
  Bot,
  CircleAlert,
  CircleCheck,
  Loader2,
  Network,
  Pause,
  Power,
  Sparkles,
  RotateCw,
} from "lucide-react";
import type { AgentDefinition, AgentRuntimeState } from "@/lib/types";
import { cn } from "@/lib/utils";
import { useMesh } from "@/lib/store";

type AgentNodeData = {
  def: AgentDefinition;
  state?: AgentRuntimeState;
};

const ACCENT_TO_RGB: Record<AgentDefinition["accent"], string> = {
  cyan: "34, 211, 238",
  violet: "167, 139, 250",
  emerald: "52, 211, 153",
  amber: "251, 191, 36",
  rose: "251, 113, 133",
};

function statusBadge(state?: AgentRuntimeState) {
  const s = state?.status ?? "offline";
  switch (s) {
    case "online":
      return { label: "ONLINE", color: "#34d399", icon: CircleCheck };
    case "starting":
      return { label: "STARTING", color: "#fbbf24", icon: Loader2 };
    case "reasoning":
      return { label: "REASONING", color: "#a78bfa", icon: Sparkles };
    case "acting":
      return { label: "ACTING", color: "#22d3ee", icon: Activity };
    case "learning":
      return { label: "LEARNING", color: "#a78bfa", icon: Sparkles };
    case "awaiting-approval":
      return { label: "AWAITING APPROVAL", color: "#fbbf24", icon: Pause };
    case "crashed":
      return { label: "CRASHED", color: "#fb7185", icon: CircleAlert };
    case "replaying":
      return { label: "REPLAYING", color: "#22d3ee", icon: RotateCw };
    case "offline":
    default:
      return { label: "OFFLINE", color: "#5c6479", icon: Power };
  }
}

function AgentNodeImpl({ data, selected }: NodeProps) {
  const { def, state } = data as AgentNodeData;
  const rgb = ACCENT_TO_RGB[def.accent];
  const badge = statusBadge(state);
  const Icon = badge.icon;
  const select = useMesh((s) => s.select);
  const totalLag = state ? Object.values(state.consumerLag).reduce((a, b) => a + b, 0) : 0;
  const isCrashed = state?.status === "crashed";
  const isActive = state?.status === "reasoning" || state?.status === "acting" || state?.status === "learning" || state?.status === "replaying";

  return (
    <div
      className="relative"
      onClick={() => select({ kind: "agent", id: def.id as import("@/lib/types").AgentId })}
    >
      <Handle type="target" position={Position.Left} style={{ background: `rgba(${rgb}, 0.9)` }} />

      <div
        className={cn(
          "w-[260px] rounded-2xl border bg-bg-elev p-4 transition-all",
          "shadow-[0_8px_32px_rgba(0,0,0,0.5)]",
          selected ? "ring-2 ring-offset-0" : "",
          isCrashed ? "border-rose-500/60" : "border-white/10 hover:border-white/20"
        )}
        style={{
          background: `linear-gradient(165deg, rgba(${rgb}, 0.08) 0%, rgba(20, 24, 38, 0.95) 60%)`,
          boxShadow: isActive
            ? `0 0 0 1px rgba(${rgb}, 0.5), 0 0 32px rgba(${rgb}, 0.25), 0 12px 32px rgba(0,0,0,0.4)`
            : isCrashed
            ? `0 0 0 1px rgba(248, 113, 113, 0.5), 0 0 24px rgba(248, 113, 113, 0.18)`
            : `0 0 0 1px rgba(${rgb}, 0.18), 0 12px 32px rgba(0,0,0,0.4)`,
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <div
              className={cn("w-9 h-9 rounded-lg flex items-center justify-center", isActive && "agent-pulse")}
              style={{ background: `rgba(${rgb}, 0.2)`, border: `1px solid rgba(${rgb}, 0.4)` }}
            >
              <Bot size={18} style={{ color: `rgb(${rgb})` }} />
            </div>
            <div className="leading-tight">
              <div className="text-[13px] font-semibold text-fg-base">{def.name}</div>
              <div className="text-[10.5px] uppercase tracking-wider text-fg-dim font-mono">
                {def.role}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full" style={{ background: `${badge.color}1f` }}>
            <Icon size={10} style={{ color: badge.color }} className={badge.label === "STARTING" || badge.label === "REPLAYING" ? "animate-spin" : ""} />
            <span className="text-[9.5px] font-mono font-semibold tracking-wider" style={{ color: badge.color }}>
              {badge.label}
            </span>
          </div>
        </div>

        {/* Subtitle */}
        <div className="text-[11.5px] text-fg-muted mb-3 leading-snug min-h-[28px]">
          {def.subtitle}
        </div>

        {/* Stats grid */}
        <div className="grid grid-cols-3 gap-2 mb-3">
          <Stat label="Processed" value={state?.processed ?? 0} accent={def.accent} />
          <Stat label="Lag" value={totalLag} accent={def.accent} warn={totalLag > 0} />
          <Stat label="Tools" value={def.tools.length} accent={def.accent} />
        </div>

        {/* Topics line */}
        <div className="flex items-center gap-1.5 text-[10px] text-fg-dim font-mono">
          <Network size={10} />
          <span>
            {def.consumes.length}↓ {def.produces.length}↑
          </span>
          <span className="ml-auto">{def.id}</span>
        </div>
      </div>

      <Handle type="source" position={Position.Right} style={{ background: `rgba(${rgb}, 0.9)` }} />

      {/* Loop indicator (for monitor agent) */}
      {def.id === "monitor-agent" && (
        <Handle
          id="self-out"
          type="source"
          position={Position.Top}
          style={{ background: `rgba(${rgb}, 0.6)`, left: "70%" }}
        />
      )}
      {def.id === "monitor-agent" && (
        <Handle
          id="self-in"
          type="target"
          position={Position.Top}
          style={{ background: `rgba(${rgb}, 0.6)`, left: "30%" }}
        />
      )}
    </div>
  );
}

function Stat({
  label,
  value,
  accent,
  warn,
}: {
  label: string;
  value: number;
  accent: AgentDefinition["accent"];
  warn?: boolean;
}) {
  const rgb = ACCENT_TO_RGB[accent];
  return (
    <div className="rounded-lg px-2 py-1.5" style={{ background: warn ? "rgba(251, 191, 36, 0.08)" : `rgba(${rgb}, 0.06)`, border: `1px solid ${warn ? "rgba(251, 191, 36, 0.25)" : `rgba(${rgb}, 0.18)`}` }}>
      <div className="text-[8.5px] uppercase tracking-wider font-mono text-fg-dim">{label}</div>
      <div className="text-[13px] font-mono font-semibold text-fg-base mt-0.5">
        {value.toLocaleString()}
      </div>
    </div>
  );
}

export const AgentNode = memo(AgentNodeImpl);
