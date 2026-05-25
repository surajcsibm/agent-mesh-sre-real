"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronDown, ChevronUp, Eraser, Terminal } from "lucide-react";
import { useMesh } from "@/lib/store";
import { cn, relTime } from "@/lib/utils";
import { AGENTS } from "@/lib/agents-config";
import type { AgentId } from "@/lib/types";

const AGENT_COLOR: Record<AgentId | "system", string> = {
  "intake-agent": "#22d3ee",
  "monitor-agent": "#a78bfa",
  "writer-agent": "#34d399",
  "notification-agent": "#fbbf24",
  intake: "#22d3ee",
  monitor: "#a78bfa",
  writer: "#34d399",
  notification: "#fbbf24",
  system: "#94a3b8",
};

export function EventLog() {
  const lines = useMesh((s) => s.logLines);
  const audit = useMesh((s) => s.audit);
  const clearLogs = useMesh((s) => s.clearLogs);
  const [open, setOpen] = useState(true);
  const [tab, setTab] = useState<"logs" | "audit">("logs");
  const ref = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom
  useEffect(() => {
    if (open && ref.current) {
      ref.current.scrollTop = ref.current.scrollHeight;
    }
  }, [lines.length, audit.length, open, tab]);

  return (
    <div className="border-t border-white/10 bg-bg-elev/60 backdrop-blur-md">
      <div className="flex items-center px-3 py-1.5 gap-3">
        <div className="flex items-center gap-1.5">
          <Terminal size={12} className="text-fg-muted" />
          <button
            onClick={() => setTab("logs")}
            className={cn(
              "text-[11px] font-mono uppercase tracking-wider px-2 py-0.5 rounded transition-colors",
              tab === "logs" ? "text-fg-base bg-white/[0.06]" : "text-fg-dim hover:text-fg-base"
            )}
          >
            Live agent log
          </button>
          <button
            onClick={() => setTab("audit")}
            className={cn(
              "text-[11px] font-mono uppercase tracking-wider px-2 py-0.5 rounded transition-colors",
              tab === "audit" ? "text-fg-base bg-white/[0.06]" : "text-fg-dim hover:text-fg-base"
            )}
          >
            ops.actions.audit.v1
          </button>
        </div>
        <span className="text-[10px] text-fg-dim font-mono">
          {tab === "logs" ? `${lines.length} lines` : `${audit.length} entries`}
        </span>
        <div className="ml-auto flex items-center gap-1.5">
          {tab === "logs" && (
            <button onClick={clearLogs} className="btn !py-0.5 !px-1.5 !text-[10px]" title="Clear log">
              <Eraser size={10} />
              Clear
            </button>
          )}
          <button onClick={() => setOpen((o) => !o)} className="btn !py-0.5 !px-1.5 !text-[10px]">
            {open ? <ChevronDown size={10} /> : <ChevronUp size={10} />}
            {open ? "Hide" : "Show"}
          </button>
        </div>
      </div>
      {open && (
        <div ref={ref} className="h-[180px] overflow-y-auto px-3 pb-2 font-mono text-[11.5px] leading-snug">
          {tab === "logs" ? (
            lines.length === 0 ? (
              <div className="text-fg-dim italic py-4">Logs will stream here as scenarios run.</div>
            ) : (
              lines.map((l) => (
                <div key={l.id} className="flex gap-2 py-0.5">
                  <span className="text-fg-dim w-[68px] shrink-0">{relTime(l.ts)}</span>
                  <span className="w-[140px] shrink-0" style={{ color: AGENT_COLOR[l.agent] }}>
                    [{AGENTS[l.agent as AgentId]?.name ?? "system"}]
                  </span>
                  <span
                    className={cn(
                      "shrink-0 w-[40px] uppercase",
                      l.level === "warn" ? "text-amber-300" : l.level === "error" ? "text-rose-300" : "text-fg-dim"
                    )}
                  >
                    {l.level}
                  </span>
                  <span className="text-fg-base">{l.message}</span>
                </div>
              ))
            )
          ) : (
            audit.length === 0 ? (
              <div className="text-fg-dim italic py-4">No audit entries yet.</div>
            ) : (
              audit.slice(-200).map((e) => (
                <div key={e.id} className="flex gap-2 py-0.5">
                  <span className="text-fg-dim w-[68px] shrink-0">{relTime(e.ts)}</span>
                  <span className="w-[140px] shrink-0" style={{ color: AGENT_COLOR[e.agent] }}>
                    [{AGENTS[e.agent]?.name ?? e.agent}]
                  </span>
                  <span className="shrink-0 w-[120px] text-fg-muted">{e.kind}</span>
                  <span className="text-fg-base truncate">{e.detail}</span>
                </div>
              ))
            )
          )}
        </div>
      )}
    </div>
  );
}
