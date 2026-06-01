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
  | { type: "lastEmailSummary"; data: EmailSummaryData | null };

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
      auditLog:      action.payload.auditLog      ?? state.auditLog,
      lessons:       action.payload.lessons       ?? state.lessons,
      notifications: action.payload.notifications ?? state.notifications,
      scenarioRunning: action.payload.scenarioRunning ?? state.scenarioRunning,
    };
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
            // Strip pendingApprovals from SSE payloads — approvals are managed
            // exclusively by client-sim. On localhost the Next.js dev server
            // retains scenario state across page refreshes in globalThis, so the
            // SSE stream would replay stale approvals and double-show the gate.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            dispatch({ type: "state", payload: { ...(event as any), pendingApprovals: [] } });
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
    resolvePendingApproval(decision === "approve");
    // Also notify the server (no-op on Vercel serverless, but keeps real-mode in sync).
    fetch("/api/mesh/approve", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, decision }) }).catch(() => {});
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

  const triggerTopicAction = (payload: TopicChangePayload) => {
    runTopicManagement(payload, dispatch as (a: SimAction) => void);
  };

  const triggerTopicHeal = (payload: TopicHealPayload, onComplete?: () => void) => {
    runTopicHeal(payload, dispatch as (a: SimAction) => void, onComplete);
  };

  return { state, trigger, approve, agentAction, reset, dismissEmailSummary, showLastSummary, triggerTopicAction, triggerTopicHeal };
}
