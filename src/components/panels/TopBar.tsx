"use client";
import * as React from "react";

import Link from "next/link";
import {
  Activity,
  Eye,
  Boxes,
  CircuitBoard,
  Cpu,
  Database,
  KeyRound,
  Layers,
  Lock,
  Network,
  Radio,
  ShieldCheck,
  Users,
  Wand2,
} from "lucide-react";
import { useMesh } from "@/lib/store";
import { useClusterStore } from "@/lib/cluster-status";

export function TopBar() {
  const connected = useMesh((s) => s.connected);
  const sim = useMesh((s) => s.cluster);
  const audit = useMesh((s) => s.audit.length);
  const mode = useClusterStore((s) => s.mode);
  const snap = useClusterStore((s) => s.snapshot);
  const connect = useClusterStore((s) => s.connectInfo);

  // Strimzi/k8s path: mode=real + snapshot shows cluster ready
  const isReal = mode?.mode === "real" && !!snap?.cluster.ready;
  const realPartial = mode?.mode === "real" && !snap?.cluster.ready && !!mode?.kubeAvailable;
  const kubeReady = !!mode?.kubeAvailable && !!connect?.strimzi?.present;
  // Direct Kafka path (Aiven / RedPanda / Confluent): mode=real + bootstrap set + no k8s
  const isDirectKafka = mode?.mode === "real" && !!mode?.kafka?.bootstrapInternal && !mode?.kubeAvailable;

  return (
    <header className="border-b border-white/10 bg-bg-elev/60 backdrop-blur-md">
      <div className="flex items-center justify-between px-5 py-3 gap-6">
        {/* Brand */}
        <div className="flex items-center gap-3 min-w-0">
          <div
            className="w-9 h-9 rounded-xl flex items-center justify-center"
            style={{
              background: "linear-gradient(135deg, rgba(34, 211, 238, 0.25), rgba(167, 139, 250, 0.25))",
              border: "1px solid rgba(167, 139, 250, 0.3)",
            }}
          >
            <Network size={18} className="text-fg-base" />
          </div>
          <div className="leading-tight min-w-0">
            <div className="text-[13.5px] font-semibold text-fg-base truncate">
              Agent Mesh on Streaming World
            </div>
            <div className="text-[10.5px] uppercase tracking-wider font-mono text-fg-dim truncate">
              MCP-Governed Kafka SRE — API Days demo
            </div>
          </div>
        </div>

        {/* Mode badge — biggest, deliberately punchy */}
        <ModeBadge
          mode={mode?.mode ?? "mock"}
          ready={isReal}
          partial={realPartial}
          kubeReady={kubeReady}
          directKafka={isDirectKafka}
          bootstrapHost={mode?.kafka?.bootstrapInternal?.split(":")[0]}
        />

        {/* Cluster strip */}
        <div className="flex items-center gap-2 overflow-x-auto">
          <Pill icon={<Radio size={11} />} label="Live SSE" tone={connected ? "ok" : "warn"} value={connected ? "connected" : "disconnected"} />

          {isReal && snap ? (
            <RealStrip snapshot={snap} audit={audit} />
          ) : isDirectKafka && mode?.kafka ? (
            <DirectKafkaStrip kafka={mode.kafka} audit={audit} />
          ) : sim ? (
            <MockStrip sim={sim} audit={audit} />
          ) : null}

          <Link
            href="/setup"
            className="flex items-center gap-1.5 rounded-md px-2 py-1 text-[10.5px] font-mono whitespace-nowrap border border-white/10 hover:border-white/30 hover:bg-white/5 transition-colors text-fg-base"
            title={isReal ? "Re-run cluster setup" : "Configure or connect to a real Kafka cluster"}
          >
            <Wand2 size={11} className="text-violet-400" />
            <span className="text-fg-dim uppercase tracking-wider">Cluster</span>
            <span className="text-fg-base font-semibold">{isReal ? "connected" : "setup"}</span>
          </Link>
        </div>
      </div>
    </header>
  );
}

function ModeBadge({
  mode,
  ready,
  partial,
  kubeReady,
  directKafka,
  bootstrapHost,
}: {
  mode: "mock" | "real";
  ready: boolean;
  partial: boolean;
  kubeReady: boolean;
  directKafka?: boolean;
  bootstrapHost?: string;
}) {
  const label = mode === "real"
    ? directKafka ? "Aiven Kafka · Connected"
    : ready ? "Real cluster"
    : partial ? "Real (provisioning)"
    : "Real (no cluster yet)"
    : "Simulator";
  const sub = mode === "real"
    ? directKafka ? (bootstrapHost ?? "Direct Kafka · SASL/SCRAM-256")
    : ready ? "Strimzi-managed Kafka 4.2 KRaft"
    : "Awaiting Kafka CR Ready"
    : kubeReady ? "Kube + Strimzi reachable — switch in Setup" : "In-process Kafka simulator";
  const c = mode === "real"
    ? (directKafka || ready) ? "#22c55e" : "#fbbf24"
    : "#a78bfa";
  return (
    <div
      className="hidden md:flex flex-col items-start rounded-lg px-3 py-1.5"
      style={{
        background: `linear-gradient(135deg, ${c}22, ${c}10)`,
        border: `1px solid ${c}40`,
      }}
    >
      <div className="flex items-center gap-1.5">
        <span className="agent-pulse rounded-full block" style={{ width: 8, height: 8, background: c, boxShadow: `0 0 12px ${c}` }} />
        <span className="text-[11px] font-semibold uppercase tracking-wider" style={{ color: c }}>{label}</span>
      </div>
      <div className="text-[9.5px] uppercase tracking-wider font-mono text-fg-dim">{sub}</div>
    </div>
  );
}

function RealStrip({
  snapshot,
  audit,
}: {
  snapshot: import("@/lib/cluster-status").ClusterSnapshot;
  audit: number;
}) {
  const c = snapshot.cluster;
  const readyTopics = snapshot.topics.filter((t) => t.ready).length;
  const readyUsers = snapshot.users.filter((u) => u.ready).length;
  const readyPools = snapshot.nodePools.filter((p) => p.ready).length;
  return (
    <>
      <Pill
        icon={<Cpu size={11} />}
        label="Kafka"
        value={c.kafkaVersion ? `${c.kafkaVersion} (${c.metadataVersion ?? "KRaft"})` : "unknown"}
        tone="ok"
      />
      <Pill
        icon={<Database size={11} />}
        label="NodePools"
        value={`${readyPools}/${snapshot.nodePools.length}`}
        tone={readyPools === snapshot.nodePools.length ? "ok" : "warn"}
      />
      <Pill icon={<Layers size={11} />} label="Topics" value={`${readyTopics}/${snapshot.totals.topics}`} tone={readyTopics === snapshot.totals.topics ? "ok" : "warn"} />
      <Pill icon={<Users size={11} />} label="Users" value={`${readyUsers}/${snapshot.totals.users}`} tone={readyUsers === snapshot.totals.users ? "ok" : "warn"} />
      <Pill icon={<Lock size={11} />} label="mTLS" value="on" tone="ok" />
      <Pill icon={<KeyRound size={11} />} label="SCRAM" value="on" tone="ok" />
      <Pill icon={<ShieldCheck size={11} />} label="ACLs" value="simple" tone="ok" />
      <Pill icon={<Activity size={11} />} label="Audit" value={`${audit} writes`} tone="info" />
      {c.clusterId ? (
        <Pill icon={<Boxes size={11} />} label="ClusterId" value={c.clusterId.slice(0, 8) + "…"} tone="info" />
      ) : null}
    </>
  );
}

function DirectKafkaStrip({
  kafka,
  audit,
}: {
  kafka: NonNullable<import("@/lib/cluster-status").ModeInfo["kafka"]>;
  audit: number;
}) {
  const host = kafka.bootstrapInternal?.split(":")[0] ?? "unknown";
  const port = kafka.bootstrapInternal?.split(":")[1] ?? "";
  return (
    <>
      <Pill icon={<Cpu size={11} />} label="Kafka" value="Aiven · SASL/SCRAM-256" tone="ok" />
      <Pill icon={<Database size={11} />} label="Broker" value={`${host.split(".")[0]}${port ? `:${port}` : ""}`} tone="ok" />
      <Pill icon={<Lock size={11} />} label="TLS" value="custom CA" tone="ok" />
      <Pill icon={<KeyRound size={11} />} label="Auth" value={kafka.username ?? "avnadmin"} tone="ok" />
      <Pill icon={<ShieldCheck size={11} />} label="CA cert" value={kafka.hasCaCert ? "✓" : "✗"} tone={kafka.hasCaCert ? "ok" : "warn"} />
      <Pill icon={<Activity size={11} />} label="Audit" value={`${audit} writes`} tone="info" />
      <MonitorPollPill />
    </>
  );
}

function MockStrip({
  sim,
  audit,
}: {
  sim: NonNullable<ReturnType<typeof useMesh.getState>["cluster"]>;
  audit: number;
}) {
  return (
    <>
      <Pill icon={<Cpu size={11} />} label="Mode" value={sim.mode} tone="ok" />
      <Pill
        icon={<CircuitBoard size={11} />}
        label="Controller"
        value={`broker-${sim.controllerId} (epoch ${sim.controllerEpoch})`}
        tone="info"
      />
      <Pill
        icon={<Database size={11} />}
        label="Brokers"
        value={`${sim.brokers.filter((b) => b.status === "online").length}/${sim.brokers.length}`}
        tone="ok"
      />
      <Pill icon={<Lock size={11} />} label="mTLS" value={sim.security.mTLS ? "on" : "off"} tone={sim.security.mTLS ? "ok" : "warn"} />
      <Pill icon={<KeyRound size={11} />} label="SASL/SCRAM" value={sim.security.saslScram ? "on" : "off"} tone={sim.security.saslScram ? "ok" : "warn"} />
      <Pill icon={<ShieldCheck size={11} />} label="ACLs" value={String(sim.security.aclsActive)} tone="ok" />
      <Pill icon={<Activity size={11} />} label="Audit" value={`${audit} writes`} tone="info" />
      <MonitorPollPill />
    </>
  );
}

function Pill({
  icon,
  label,
  value,
  tone,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  tone: "ok" | "warn" | "info";
}) {
  const color = tone === "ok" ? "#34d399" : tone === "warn" ? "#fbbf24" : "#22d3ee";
  return (
    <div
      className="flex items-center gap-1.5 rounded-md px-2 py-1 text-[10.5px] font-mono whitespace-nowrap"
      style={{
        background: `${color}10`,
        border: `1px solid ${color}30`,
        color: "var(--color-fg-base)",
      }}
    >
      <span style={{ color }}>{icon}</span>
      <span className="text-fg-dim uppercase tracking-wider">{label}</span>
      <span className="text-fg-base font-semibold">{value}</span>
    </div>
  );
}


// ── Monitor polling status pill ───────────────────────────────────────────────
function MonitorPollPill() {
  const [poll, setPoll] = React.useState<{
    running: boolean; cycleCount: number; lastPollAt: number | null;
    detectedThisCycle: Array<{ scenarioId: string; gate: string }>;
  } | null>(null);

  React.useEffect(() => {
    let alive = true;
    const refresh = async () => {
      try {
        const res = await fetch("/api/mesh/poll");
        if (res.ok && alive) setPoll((await res.json()).poll);
      } catch { /* polling endpoint not yet available */ }
    };
    refresh();
    const iv = setInterval(refresh, 5_000);
    return () => { alive = false; clearInterval(iv); };
  }, []);

  if (!poll) return null;
  const hasDetections = (poll.detectedThisCycle?.length ?? 0) > 0;
  const c = poll.running ? (hasDetections ? "#fbbf24" : "#22d3ee") : "#94a3b8";
  const label = poll.running
    ? hasDetections
      ? `${poll.detectedThisCycle.length} detected`
      : `cycle ${poll.cycleCount}`
    : "idle";
  const ago = poll.lastPollAt
    ? `${Math.round((Date.now() - poll.lastPollAt) / 1000)}s ago`
    : "pending";

  return (
    <div
      className="flex items-center gap-1.5 rounded-md px-2 py-1 text-[10.5px] font-mono whitespace-nowrap"
      style={{ background: `${c}15`, border: `1px solid ${c}35` }}
      title={`Monitor polling loop · last poll ${ago}`}
    >
      <span style={{ color: c, display: "flex" }}>
        {poll.running
          ? <span className="agent-pulse rounded-full block" style={{ width: 7, height: 7, background: c, boxShadow: `0 0 8px ${c}` }} />
          : <Eye size={11} />
        }
      </span>
      <span className="text-fg-dim uppercase tracking-wider">Monitor</span>
      <span className="font-semibold" style={{ color: c }}>{label}</span>
    </div>
  );
}

