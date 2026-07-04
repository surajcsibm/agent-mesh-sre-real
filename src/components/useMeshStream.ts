"use client";

import { useEffect, useReducer, useRef } from "react";
import type {
  AgentState, BrokerState, MralPhase, ApprovalRequest,
  AuditRecord, LessonRecord, NotificationRecord, BusEvent,
} from "@/lib/types";
import { runClientScenario, resolvePendingApproval, runTopicManagement, runTopicHeal, setBrokerMode, type ScenarioKey, type SimAction, type EmailSummaryData, type TopicChangePayload, type TopicHealPayload } from "@/lib/client-sim";

export interface MeshClientState {
  agents: AgentState[];
  broker: BrokerState | null;
  mralPhase: MralPhase;
  pendingApprovals: ApprovalRequest[];
  auditLog: AuditRecord[];
  lessons: LessonRecord[];
  notifications: NotificationRecord[];
  incidentQueueDepth: number;
  scenarioRunning: boolean;
  toasts: { id: number; message: string; kind: string }[];
  particles: { id: string; edgeId: string; fromNode: string; toNode: string; ts: number }[];
  connected: boolean;
  emailSummary: EmailSummaryData | null;
  lastEmailSummary: EmailSummaryData | null;
}

export type { EmailSummaryData, TopicChangePayload, TopicHealPayload };

type Action =
  | { type: "state"; payload: Omit<MeshClientState, "toasts" | "particles" | "connected" | "auditLog" | "lessons" | "notifications" | "emailSummary"> & { auditLog?: AuditRecord[]; lessons?: LessonRecord[]; notifications?: NotificationRecord[]; scenarioRunning?: boolean } }
  | { type: "audit"; record: AuditRecord }
  | { type: "toast"; message: string; kind: string; id: number }
  | { type: "dismissToast"; id: number }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string; id: string }
  | { type: "clearParticle"; id: string }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord }
  | { type: "connected"; value: boolean }
  | { type: "emailSummary"; data: EmailSummaryData | null }
  | { type: "lastEmailSummary"; data: EmailSummaryData | null }
  | { type: "add_pending_approval"; payload: import("@/lib/types").ApprovalRequest };

const initial: MeshClientState = {
  agents: [], broker: null, mralPhase: "idle",
  pendingApprovals: [], auditLog: [], lessons: [], notifications: [],
  incidentQueueDepth: 0, scenarioRunning: false,
  toasts: [], particles: [], connected: false,
  emailSummary: null,
  lastEmailSummary: null,
};

function reducer(state: MeshClientState, action: Action): MeshClientState {
  switch (action.type) {
    case "state": return {
      ...state,
      ...action.payload,
      auditLog:         action.payload.auditLog         ?? state.auditLog,
      lessons:          action.payload.lessons          ?? state.lessons,
      notifications:    action.payload.notifications    ?? state.notifications,
      scenarioRunning:  action.payload.scenarioRunning  ?? state.scenarioRunning,
      // Preserve existing pendingApprovals if SSE payload doesn't include them
      pendingApprovals: action.payload.pendingApprovals ?? state.pendingApprovals,
    };
    case "add_pending_approval":
      return { ...state, pendingApprovals: [...(state.pendingApprovals || []), (action as { type: string; payload: unknown }).payload as import("@/lib/types").ApprovalRequest] };
    case "audit": return { ...state, auditLog: [...state.auditLog.slice(-199), action.record] };
    case "toast": return { ...state, toasts: [...state.toasts, { id: action.id, message: action.message, kind: action.kind }] };
    case "dismissToast": return { ...state, toasts: state.toasts.filter((t) => t.id !== action.id) };
    case "particle": return { ...state, particles: [...state.particles, { id: action.id, edgeId: action.edgeId, fromNode: action.fromNode, toNode: action.toNode, ts: Date.now() }] };
    case "clearParticle": return { ...state, particles: state.particles.filter((p) => p.id !== action.id) };
    case "notification": return { ...state, notifications: [...state.notifications.slice(-49), action.record] };
    case "lesson": return { ...state, lessons: [...state.lessons.slice(-19), action.record] };
    case "connected": return { ...state, connected: action.value };
    case "emailSummary": return {
      ...state,
      emailSummary: action.data,
      // Persist non-null data as lastEmailSummary so it can be re-shown
      lastEmailSummary: action.data ?? state.lastEmailSummary,
    };
    case "lastEmailSummary": return { ...state, emailSummary: action.data };
    default: return state;
  }
}

let toastId = 0;
let particleId = 0;

export function useMeshStream() {
  const [state, dispatch] = useReducer(reducer, initial);
  const esRef = useRef<EventSource | null>(null);
  // Ref always holds the latest non-null emailSummary so showLastSummary is never stale,
  // regardless of when in the render cycle the button is clicked.
  const lastSummaryRef = useRef<EmailSummaryData | null>(null);
  if (state.emailSummary !== null) {
    lastSummaryRef.current = state.emailSummary;
  }

  useEffect(() => {
    let retryMs = 1000;
    let retryTimer: ReturnType<typeof setTimeout>;

    function connect() {
      const es = new EventSource("/api/mesh/stream");
      esRef.current = es;

      es.onopen = () => { dispatch({ type: "connected", value: true }); retryMs = 1000; };
      es.onerror = () => {
        dispatch({ type: "connected", value: false });
        es.close();
        retryTimer = setTimeout(connect, Math.min(retryMs, 10000));
        retryMs *= 1.5;
      };

      es.onmessage = (e) => {
        const event = JSON.parse(e.data) as BusEvent & { auditLog?: AuditRecord[]; lessons?: LessonRecord[]; notifications?: NotificationRecord[]; scenarioRunning?: boolean };
        switch (event.type) {
          case "state": {
            // Sync broker mode into client-sim so MRAL scenario steps preserve REAL state.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const brokerMode = (event as any).broker?.mode;
            if (brokerMode === "REAL" || brokerMode === "MOCK") setBrokerMode(brokerMode);
            // Preserve pendingApprovals set by client-sim — SSE state updates
            // must NOT zero them out or the approval gate disappears immediately.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const ssePayload = { ...(event as any) };
            delete ssePayload.pendingApprovals;
            dispatch({ type: "state", payload: ssePayload });
            break;
          }
          case "auto-trigger-scenario": {
            // Autonomous Monitor triggered a scenario — run the full client-side MRAL.
            // runClientScenario handles: animation, approval gates, notifications, email.
            const sid = (event as { type: "auto-trigger-scenario"; scenarioId: string }).scenarioId;
            if (sid && !(window as unknown as Record<string, unknown>).__simPaused) runClientScenario(sid as ScenarioKey, dispatch as (a: SimAction) => void);
            break;
          }
          case "auto-topic-heal": {
            // Monitor detected an unhealthy topic and scheduled autonomous healing.
            const { topicName, currentStatus, lagTotal, partitions } =
              event as { type: "auto-topic-heal"; topicName: string; currentStatus: "degraded" | "critical"; lagTotal: number; partitions: number };
            runTopicHeal({ topicName, currentStatus, lagTotal, partitions }, dispatch as (a: SimAction) => void);
            break;
          }
          case "approval-new": {
            // Autonomous Monitor trigger raised a policy gate.
            // Add the approval to state.pendingApprovals so the Dashboard modal shows.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const ap = (event as any).payload;
            if (ap) dispatch({ type: "add_pending_approval", payload: ap });
            break;
          }
          case "approval-update": {
            // Server resolved the approval (approved or rejected) — clear the modal.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const upd = (event as any).payload;
            const keep = upd?.status === "pending" ? [upd] : [];
            if (keep.length > 0) {
              dispatch({ type: "add_pending_approval", payload: keep[0] });
            }
            break;
          }
          case "audit":
            dispatch({ type: "audit", record: event.record });
            break;
          case "toast": {
            const id = ++toastId;
            dispatch({ type: "toast", message: event.message, kind: event.kind, id });
            setTimeout(() => dispatch({ type: "dismissToast", id }), 4500);
            break;
          }
          case "particle": {
            const id = `p-${++particleId}`;
            dispatch({ type: "particle", edgeId: event.edgeId, fromNode: event.fromNode, toNode: event.toNode, id });
            setTimeout(() => dispatch({ type: "clearParticle", id }), 1200);
            break;
          }
          case "notification":
            dispatch({ type: "notification", record: event.record });
            break;
          case "lesson":
            dispatch({ type: "lesson", record: event.record });
            break;
        }
      };
    }

    connect();
    return () => { esRef.current?.close(); clearTimeout(retryTimer); };
  }, []);

  const trigger = async (scenarioId: string) => {
    // The client-side simulation handles ALL visual animation and audit records.
    // We do NOT also call /api/mesh/scenario because:
    //   • On localhost  → SSE would deliver server-side events on top of the
    //     client-sim events, producing duplicate audit log entries.
    //   • On Vercel     → serverless isolation means the SSE stream and the
    //     scenario trigger run in different instances; server events never
    //     arrive anyway, so the double-call is pure noise.
    // For REAL Kafka mode (actual broker mutations), wire up a separate
    // "exec-only" endpoint that fires mutations without pushing SSE events.
    runClientScenario(scenarioId as ScenarioKey, dispatch as (a: SimAction) => void);
  };

  const approve = async (id: string, decision: "approve" | "reject") => {
    // Route the decision into the client-side simulation immediately so the
    // scenario branches on approve vs reject without waiting for the server.
    resolvePendingApproval(id, decision === "approve");
    // Also notify the server (no-op on Vercel serverless, but keeps real-mode in sync).
    fetch("/api/mesh/approve", { method: "POST", headers: { "Content-Type": "application/json" }, credentials: "include", body: JSON.stringify({ id, decision }) }).catch(() => {});
  };

  const agentAction = async (agentId: string, action: "kill" | "restart") => {
    // Show immediate popup — user shouldn't have to hunt for feedback
    const id = ++toastId;
    const agentLabel = agentId.replace("-agent", "");
    if (action === "kill") {
      dispatch({ type: "toast", message: `⚰️ ${agentLabel} agent killed — click Restart to resume`, kind: "error", id });
      setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
    } else {
      dispatch({ type: "toast", message: `✅ ${agentLabel} agent restarted successfully`, kind: "success", id });
      setTimeout(() => dispatch({ type: "dismissToast", id }), 4500);
    }
    fetch("/api/mesh/agent", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ agentId, action }) }).catch(() => {});
  };

  const reset = async () => {
    await fetch("/api/mesh/reset", { method: "POST" });
  };

  const dismissEmailSummary = () => dispatch({ type: "emailSummary", data: null });

  const showLastSummary = () => {
    // Use the ref — never stale, always holds the last non-null emailSummary
    if (lastSummaryRef.current) {
      dispatch({ type: "emailSummary", data: lastSummaryRef.current });
    }
  };

  const triggerTopicAction = async (payload: TopicChangePayload) => {
    // "create" and "delete" hit the real cluster (kafka-admin-cfk.ts via
    // /api/admin/topics) before the MRAL animation plays. If the real call
    // fails, show an error and skip the animation rather than narrating a
    // change that didn't actually happen.
    //
    // "edit" stays simulation-only: real Kafka can't decrease partition
    // count or arbitrarily change replication factor without reassignment
    // tooling this app doesn't have. Only retention could be made real
    // later via alterConfigs — not yet wired.
    if (payload.operation === "edit" && payload.prevTopic && payload.prevTopic.partitions < payload.topic.partitions) {
      // Real partition increase. The UI's min-bound clamp (Dashboard.tsx)
      // already prevents submitting a decrease, but this check is a second,
      // independent guard against any path that could bypass the UI clamp
      // (e.g. a direct API call) — increaseTopicPartitions() itself will
      // also reject a decrease at the Kafka Admin API level as a third layer.
      try {
        const res = await fetch("/api/mesh/exec", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({
            action: "increase-topic-partitions",
            topicName: payload.topic.name,
            newPartitionCount: payload.topic.partitions,
          }),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok || body.error) {
          const id = ++toastId;
          dispatch({ type: "toast", message: `❌ Real partition increase failed for ${payload.topic.name}: ${body.error ?? res.statusText}`, kind: "error", id });
          setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
        }
      } catch (e) {
        const id = ++toastId;
        dispatch({ type: "toast", message: `❌ Real partition increase failed for ${payload.topic.name}: ${(e as Error).message}`, kind: "error", id });
        setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
      }
    } else if (payload.operation === "edit" && payload.prevTopic && payload.prevTopic.partitions === payload.topic.partitions) {
      // Only retention can be verified safe to make real here: TopicChangePayload's
      // prevTopic only tracks name+partitions, not the prior replication factor, so
      // we can't detect an RF change from this payload alone. Partition-count changes
      // are excluded outright — Kafka's Admin API can't decrease partitions, and this
      // app has no reassignment tooling for increases either. If partitions match,
      // we treat this as a retention-only edit and make the real alterConfigs call;
      // any RF change riding along in the same edit will NOT be reflected on the
      // real cluster even though the animation may narrate it. Known limitation.
      try {
        const res = await fetch("/api/mesh/exec", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({
            action: "update-topic-retention",
            topicName: payload.topic.name,
            retentionMs: payload.topic.retentionHours * 3600000,
          }),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok || body.error) {
          const id = ++toastId;
          dispatch({ type: "toast", message: `❌ Real cluster retention update failed for ${payload.topic.name}: ${body.error ?? res.statusText}`, kind: "error", id });
          setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
        }
      } catch (e) {
        const id = ++toastId;
        dispatch({ type: "toast", message: `❌ Real cluster retention update failed for ${payload.topic.name}: ${(e as Error).message}`, kind: "error", id });
        setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
      }
    } else if (payload.operation === "create" || payload.operation === "delete") {
      try {
        const res = await fetch("/api/admin/topics", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify(
            payload.operation === "create"
              ? {
                  action: "create",
                  name: payload.topic.name,
                  partitions: payload.topic.partitions,
                  replication: payload.topic.replicationFactor,
                  retentionMs: payload.topic.retentionHours * 3600000,
                }
              : { action: "delete", name: payload.topic.name }
          ),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok || body.error) {
          const id = ++toastId;
          dispatch({ type: "toast", message: `❌ Real cluster ${payload.operation} failed for ${payload.topic.name}: ${body.error ?? res.statusText}`, kind: "error", id });
          setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
          return; // don't run the MRAL animation for something that didn't happen
        }
      } catch (e) {
        const id = ++toastId;
        dispatch({ type: "toast", message: `❌ Real cluster ${payload.operation} failed for ${payload.topic.name}: ${(e as Error).message}`, kind: "error", id });
        setTimeout(() => dispatch({ type: "dismissToast", id }), 7000);
        return;
      }
    }
    runTopicManagement(payload, dispatch as (a: SimAction) => void);
  };

  const triggerTopicHeal = (payload: TopicHealPayload, onComplete?: () => void) => {
    runTopicHeal(payload, dispatch as (a: SimAction) => void, onComplete);
  };

  return { state, trigger, approve, agentAction, reset, dismissEmailSummary, showLastSummary, triggerTopicAction, triggerTopicHeal };
}
