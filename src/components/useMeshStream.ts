"use client";

import { useEffect, useReducer, useRef } from "react";
import type {
  AgentState, BrokerState, MralPhase, ApprovalRequest,
  AuditRecord, LessonRecord, NotificationRecord, BusEvent,
} from "@/lib/types";
import { runClientScenario, type ScenarioKey, type SimAction } from "@/lib/client-sim";

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
}

type Action =
  | { type: "state"; payload: Omit<MeshClientState, "toasts" | "particles" | "connected" | "auditLog" | "lessons" | "notifications"> & { auditLog?: AuditRecord[]; lessons?: LessonRecord[]; notifications?: NotificationRecord[]; scenarioRunning?: boolean } }
  | { type: "audit"; record: AuditRecord }
  | { type: "toast"; message: string; kind: string; id: number }
  | { type: "dismissToast"; id: number }
  | { type: "particle"; edgeId: string; fromNode: string; toNode: string; id: string }
  | { type: "clearParticle"; id: string }
  | { type: "notification"; record: NotificationRecord }
  | { type: "lesson"; record: LessonRecord }
  | { type: "connected"; value: boolean };

const initial: MeshClientState = {
  agents: [], broker: null, mralPhase: "idle",
  pendingApprovals: [], auditLog: [], lessons: [], notifications: [],
  incidentQueueDepth: 0, scenarioRunning: false,
  toasts: [], particles: [], connected: false,
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
    default: return state;
  }
}

let toastId = 0;
let particleId = 0;

export function useMeshStream() {
  const [state, dispatch] = useReducer(reducer, initial);
  const esRef = useRef<EventSource | null>(null);

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
          case "state":
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            dispatch({ type: "state", payload: event as any });
            break;
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
    // Always run the client-side simulation so Vercel deployments work
    // (serverless instances don't share globalThis state, so SSE from a
    // different instance won't carry the server-side events to this client).
    runClientScenario(scenarioId as ScenarioKey, dispatch as (a: SimAction) => void);

    // Also notify the server (fires real Kafka mutations when in REAL mode).
    fetch("/api/mesh/scenario", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: scenarioId }),
    }).catch(() => { /* server-side fire-and-forget; client sim already running */ });
  };

  const approve = async (id: string, decision: "approve" | "reject") => {
    await fetch("/api/mesh/approve", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, decision }) });
  };

  const agentAction = async (agentId: string, action: "kill" | "restart") => {
    await fetch("/api/mesh/agent", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ agentId, action }) });
  };

  const reset = async () => {
    await fetch("/api/mesh/reset", { method: "POST" });
  };

  return { state, trigger, approve, agentAction, reset };
}
