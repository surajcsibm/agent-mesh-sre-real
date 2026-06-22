"use client";

import { useState } from "react";
import {
  BookOpenText,
  ClipboardList,
  FileCode,
  Hammer,
  ScrollText,
  Send,
  Sparkles,
} from "lucide-react";
import { useMesh } from "@/lib/store";
import { AGENTS } from "@/lib/agents-config";
import { ASYNCAPI_SPECS } from "@/lib/asyncapi";
import { MCP_TOOLS } from "@/lib/mcp";
import type { TopicName, AgentId } from "@/lib/types";
import { cn, relTime } from "@/lib/utils";
import { JsonPretty } from "../ui/JsonPretty";

type Tab =
  | "selection"
  | "asyncapi"
  | "mcp"
  | "lessons"
  | "audit"
  | "notifications";

const TABS: { key: Tab; label: string; icon: React.ReactNode }[] = [
  { key: "selection", label: "Inspector", icon: <Sparkles size={12} /> },
  { key: "asyncapi", label: "AsyncAPI", icon: <FileCode size={12} /> },
  { key: "mcp", label: "MCP", icon: <Hammer size={12} /> },
  { key: "lessons", label: "Lessons", icon: <BookOpenText size={12} /> },
  { key: "audit", label: "Audit", icon: <ScrollText size={12} /> },
  { key: "notifications", label: "Outbound", icon: <Send size={12} /> },
];

export function Inspector() {
  const [tab, setTab] = useState<Tab>("selection");

  return (
    <div className="flex flex-col h-full">
      {/* Tabs */}
      <div className="flex border-b border-white/10 px-1.5 pt-1.5 gap-0.5">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "px-2.5 py-1.5 text-[11px] font-medium rounded-t-md transition-colors flex items-center gap-1.5",
              tab === t.key
                ? "bg-white/[0.06] text-fg-base border border-white/10 border-b-transparent"
                : "text-fg-muted hover:text-fg-base hover:bg-white/[0.03]"
            )}
          >
            {t.icon}
            {t.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-hidden">
        {tab === "selection" && <SelectionTab />}
        {tab === "asyncapi" && <AsyncApiTab />}
        {tab === "mcp" && <McpTab />}
        {tab === "lessons" && <LessonsTab />}
        {tab === "audit" && <AuditTab />}
        {tab === "notifications" && <NotificationsTab />}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Selection (agent or edge)                                          */
/* ------------------------------------------------------------------ */

function SelectionTab() {
  const sel = useMesh((s) => s.selection);
  if (!sel) {
    return (
      <div className="p-6 text-[12px] text-fg-dim text-center italic">
        Click an agent node or a Kafka topic edge on the canvas to inspect it.
      </div>
    );
  }
  if (sel.kind === "agent") return <AgentInspector id={sel.id} />;
  return <EdgeInspector topic={sel.topic} />;
}

function AgentInspector({ id }: { id: AgentId }) {
  const def = AGENTS[id];
  const state = useMesh((s) => s.agents?.[id]);
  const tools = MCP_TOOLS.filter((t) => t.owner === id);

  return (
    <div className="overflow-y-auto h-full p-3 space-y-4">
      <div>
        <div className="text-[10px] uppercase font-mono tracking-wider text-fg-dim mb-1">{def.role}</div>
        <div className="text-[16px] font-semibold text-fg-base">{def.name}</div>
        <div className="text-[12px] text-fg-muted mt-1.5">{def.description}</div>
      </div>

      {/* Status grid */}
      <div className="grid grid-cols-2 gap-2">
        <Stat label="Status" value={state?.status ?? "—"} />
        <Stat label="Processed" value={state?.processed?.toLocaleString() ?? "0"} />
        <Stat
          label="Lag (sum)"
          value={
            state
              ? Object.values(state.consumerLag).reduce((a, b) => a + b, 0).toLocaleString()
              : "0"
          }
        />
        <Stat label="Inflight" value={state?.inflight ?? 0} />
      </div>

      {/* Topics */}
      {def.consumes.length > 0 && (
        <div>
          <SubHeader>Consumes</SubHeader>
          <ul className="space-y-1">
            {def.consumes.map((t) => (
              <li key={t} className="flex items-center justify-between text-[11.5px] font-mono">
                <span className="text-fg-base">{t}</span>
                <span className="tag">lag {state?.consumerLag[t] ?? 0}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
      {def.produces.length > 0 && (
        <div>
          <SubHeader>Produces</SubHeader>
          <ul className="space-y-1">
            {def.produces.map((t) => (
              <li key={t} className="text-[11.5px] font-mono text-fg-base">
                {t}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* MCP tools */}
      <div>
        <SubHeader>MCP tools</SubHeader>
        <div className="space-y-2">
          {tools.map((t) => (
            <div key={t.name} className="rounded-lg border border-white/10 bg-bg-elev p-2.5">
              <div className="flex items-center justify-between">
                <code className="text-[11.5px] !bg-transparent !p-0 text-fg-base">{t.name}</code>
                {t.requiresApproval && (
                  <span className="tag !text-amber-300 !border-amber-500/40">policy-gated</span>
                )}
              </div>
              <div className="text-[11px] text-fg-muted mt-1 leading-snug">{t.description}</div>
              <div className="flex flex-wrap gap-1 mt-1.5">
                {t.policyTags.map((tag) => (
                  <span key={tag} className="tag">{tag}</span>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Reasoning + Action (Monitor only) */}
      {id === "monitor-agent" && state?.lastReasoning && (
        <div>
          <SubHeader>Last reasoning output</SubHeader>
          <JsonPretty value={state.lastReasoning} />
        </div>
      )}
      {id === "monitor-agent" && state?.lastAction && (
        <div>
          <SubHeader>Last action</SubHeader>
          <JsonPretty value={state.lastAction} />
        </div>
      )}
    </div>
  );
}

function EdgeInspector({ topic }: { topic: TopicName }) {
  const spec = ASYNCAPI_SPECS.find((s) => s.topicName === topic);
  const records = useMesh((s) => s.recentTopicRecords[topic] ?? []);
  const meta = useMesh((s) => s.topics?.[topic]);

  return (
    <div className="overflow-y-auto h-full p-3 space-y-4">
      <div>
        <div className="text-[10px] uppercase font-mono tracking-wider text-fg-dim mb-1">Topic</div>
        <div className="text-[15px] font-semibold text-fg-base">{topic}</div>
        <div className="text-[12px] text-fg-muted mt-1.5">{spec?.info.description ?? topic}</div>
      </div>

      <div className="grid grid-cols-3 gap-2">
        <Stat label="Partitions" value={meta?.partitions ?? "—"} />
        <Stat label="LEO (sum)" value={meta?.logEndOffset ?? 0} />
        <Stat label="Records" value={records.length} />
      </div>

      <div>
        <SubHeader>AsyncAPI 3.0 channel</SubHeader>
        <JsonPretty value={spec} />
      </div>

      <div>
        <SubHeader>Recent records (newest first)</SubHeader>
        {records.length === 0 ? (
          <div className="text-[12px] text-fg-dim italic">No records yet.</div>
        ) : (
          <div className="space-y-2">
            {records.slice(0, 10).map((r, i) => (
              <details key={i} className="rounded-lg border border-white/10 bg-bg-elev">
                <summary className="cursor-pointer select-none px-2.5 py-1.5 flex items-center gap-2 text-[11px] font-mono">
                  <span className="tag">p{r.partition}</span>
                  <span className="text-fg-muted">o{r.offset}</span>
                  <span className="text-fg-base flex-1 truncate">{r.key}</span>
                  <span className="text-fg-dim">{relTime(r.timestamp ?? Date.now())}</span>
                </summary>
                <div className="p-2 border-t border-white/10">
                  <JsonPretty value={r.value} />
                </div>
              </details>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* AsyncAPI tab                                                       */
/* ------------------------------------------------------------------ */

function AsyncApiTab() {
  const [pick, setPick] = useState<TopicName>("ops.kafka.metrics.v1");
  const spec = ASYNCAPI_SPECS.find((s) => s.topicName === pick);
  const topics = ASYNCAPI_SPECS.map((s) => s.topicName as TopicName);

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2.5 border-b border-white/10">
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base mb-2">
          AsyncAPI 3.0 channel contracts
        </div>
        <div className="flex flex-wrap gap-1">
          {topics.map((t) => (
            <button
              key={t}
              onClick={() => setPick(t)}
              className={cn("tag transition-colors", pick === t && "!text-violet-300 !border-violet-500/40")}
            >
              {t}
            </button>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-3">
        <div className="text-[15px] font-semibold mb-1">{spec?.info.title ?? pick}</div>
        <div className="text-[11.5px] text-fg-muted mb-3">{spec?.info.description ?? pick}</div>
        <JsonPretty value={spec} />
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* MCP tab                                                            */
/* ------------------------------------------------------------------ */

function McpTab() {
  return (
    <div className="overflow-y-auto h-full p-3 space-y-3">
      <div>
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base">
          MCP tool registry
        </div>
        <div className="text-[11.5px] text-fg-muted mt-1">
          Each agent runs a JSON-RPC MCP server. Infrastructure-mutating tools are policy-gated.
        </div>
      </div>
      {MCP_TOOLS.map((t) => (
        <div key={t.name} className="rounded-lg border border-white/10 bg-bg-elev p-3">
          <div className="flex items-center justify-between gap-2">
            <code className="text-[12px] !bg-transparent !p-0 text-fg-base">{t.name}</code>
            {t.requiresApproval && (
              <span className="tag !text-amber-300 !border-amber-500/40">policy-gated</span>
            )}
          </div>
          <div className="text-[11px] text-fg-dim font-mono mt-0.5">owner: {t.owner}</div>
          <div className="text-[11.5px] text-fg-muted mt-1.5">{t.description}</div>
          <div className="flex flex-wrap gap-1 mt-2">
            {t.policyTags.map((tag) => (
              <span key={tag} className="tag">{tag}</span>
            ))}
          </div>
          <details className="mt-2">
            <summary className="text-[11px] text-fg-muted cursor-pointer">Schemas</summary>
            <div className="mt-1.5 space-y-1.5">
              <div>
                <div className="text-[10px] uppercase font-mono text-fg-dim mb-1">input</div>
                <JsonPretty value={t.inputSchema} />
              </div>
              <div>
                <div className="text-[10px] uppercase font-mono text-fg-dim mb-1">output</div>
                <JsonPretty value={t.outputSchema} />
              </div>
            </div>
          </details>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Lessons tab — the LEARN component                                  */
/* ------------------------------------------------------------------ */

function LessonsTab() {
  const lessons = useMesh((s) => s.lessons);
  return (
    <div className="overflow-y-auto h-full p-3 space-y-3">
      <div>
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base">
          Lessons learned (ops.lessons.v1)
        </div>
        <div className="text-[11.5px] text-fg-muted mt-1">
          Each completed remediation publishes a Lesson 60s later. The next reasoning prompt is seeded with the last 3 entries.
        </div>
      </div>
      {lessons.length === 0 && (
        <div className="text-[12px] text-fg-dim italic">No lessons yet — run a scenario.</div>
      )}
      {[...lessons].reverse().map((l) => (
        <div key={l.id} className="rounded-lg border border-white/10 bg-bg-elev p-3">
          <div className="flex items-center justify-between text-[11px]">
            <span className="tag !text-violet-300 !border-violet-500/40">{l.scenarioId}</span>
            <span className="text-fg-dim font-mono">{relTime(l.ts)}</span>
          </div>
          <div className="text-[12px] mt-1.5">
            <span className="text-fg-base">{l.actionTaken}</span>{" "}
            <span className={cn("font-medium", l.effective ? "text-emerald-300" : "text-rose-300")}>
              {l.effective ? "✓ effective" : "✗ ineffective"}
            </span>
          </div>
          <div className="text-[11px] text-fg-muted mt-1 font-mono">
            lag {(l.lagBefore ?? 0).toLocaleString()} → {(l.lagAfter ?? 0).toLocaleString()}
            {l.adjustedThreshold != null && (
              <span> · threshold→{l.adjustedThreshold.toLocaleString()}</span>
            )}
          </div>
          <div className="text-[11px] text-fg-muted mt-1.5 italic">{l.notes}</div>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Audit                                                              */
/* ------------------------------------------------------------------ */

function AuditTab() {
  const audit = useMesh((s) => s.audit);
  return (
    <div className="overflow-y-auto h-full">
      <div className="px-3 py-2.5 border-b border-white/10">
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base">
          Audit topic (ops.actions.audit.v1)
        </div>
        <div className="text-[11.5px] text-fg-muted mt-1">
          First-class Kafka topic. Durable, replayable, compliance-ready.
        </div>
      </div>
      <ul className="divide-y divide-white/5">
        {[...audit].reverse().slice(0, 80).map((e) => (
          <li key={e.id} className="px-3 py-1.5 text-[11px] hover:bg-white/[0.02]">
            <div className="flex items-center gap-1.5 font-mono">
              <span className="tag">{e.kind}</span>
              <span className="text-fg-muted">{e.agent}</span>
              <span className="ml-auto text-fg-dim">{relTime(e.ts)}</span>
            </div>
            <div className="text-fg-base mt-0.5 truncate">{String(e.detail ?? "")}</div>
          </li>
        ))}
      </ul>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Outbound notifications                                             */
/* ------------------------------------------------------------------ */

function NotificationsTab() {
  const notes = useMesh((s) => s.notifications);
  return (
    <div className="overflow-y-auto h-full">
      <div className="px-3 py-2.5 border-b border-white/10">
        <div className="text-[11px] font-mono uppercase tracking-wider text-fg-base">
          Outbound notifications
        </div>
        <div className="text-[11.5px] text-fg-muted mt-1">
          Slack + ITSM messages emitted by the Notification Agent.
        </div>
      </div>
      <ul className="divide-y divide-white/5">
        {[...notes].reverse().map((n) => (
          <li key={n.id} className="px-3 py-2.5 text-[11.5px]">
            <div className="flex items-center gap-1.5">
              <span className="tag">{n.channel}</span>
              <span className="text-fg-base font-medium">{n.title}</span>
              <span className="ml-auto text-fg-dim font-mono text-[10px]">{relTime(n.ts)}</span>
            </div>
            <div className="text-fg-muted mt-1 whitespace-pre-line">{n.body}</div>
          </li>
        ))}
        {notes.length === 0 && (
          <li className="text-[12px] text-fg-dim italic px-3 py-6 text-center">
            No outbound notifications yet.
          </li>
        )}
      </ul>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* small helpers                                                      */
/* ------------------------------------------------------------------ */

function Stat({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-white/10 bg-bg-elev px-3 py-2">
      <div className="text-[9.5px] uppercase font-mono tracking-wider text-fg-dim">{label}</div>
      <div className="text-[13px] font-mono font-semibold text-fg-base mt-0.5">{value}</div>
    </div>
  );
}

function SubHeader({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] uppercase tracking-wider font-mono text-fg-dim mb-2">
      {children}
    </div>
  );
}
