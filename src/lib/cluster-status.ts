"use client";

/**
 * Polls the runtime mode + cluster snapshot from the server, exposing it to
 * components through a small Zustand store. Used by the top bar, the setup
 * wizard, and the scenario panel.
 */
import { create } from "zustand";
import { useEffect } from "react";

export type AppMode = "mock" | "real";

export type SnapshotEntry = {
  kind: string;
  name: string;
  ready: boolean;
  reason?: string;
  message?: string;
};

export type ClusterSnapshot = {
  fetchedAt: string;
  cluster: {
    name: string;
    namespace: string;
    exists: boolean;
    ready: boolean;
    kafkaVersion?: string;
    metadataVersion?: string;
    kafkaListeners: { name: string; bootstrap?: string }[];
    conditions: { type: string; status: string; reason?: string; message?: string }[];
    clusterId?: string;
  };
  nodePools: SnapshotEntry[];
  topics: SnapshotEntry[];
  users: SnapshotEntry[];
  totals: { topics: number; users: number; nodePools: number };
};

export type ConnectInfo = {
  ok: boolean;
  connected?: boolean;
  context?: string;
  user?: string;
  serverUrl?: string;
  inCluster?: boolean;
  strimzi?: { present: boolean; version?: string };
  hint?: string;
  error?: string;
};

export type ModeInfo = {
  mode: AppMode;
  kubeAvailable: boolean;
  lastVerifiedAt?: string;
  cluster: { namespace: string; name: string };
  kafka: {
    bootstrapInternal?: string;
    bootstrapExternal?: string;
    username?: string;
    hasPassword: boolean;
    hasCaCert: boolean;
    saslMechanism?: string;
  } | null;
};

type ClusterStore = {
  mode: ModeInfo | null;
  snapshot: ClusterSnapshot | null;
  connectInfo: ConnectInfo | null;
  isPolling: boolean;
  setMode: (m: ModeInfo) => void;
  setSnapshot: (s: ClusterSnapshot) => void;
  setConnect: (c: ConnectInfo) => void;
  /** Force an immediate fetch of /api/cluster/snapshot. Useful after an
   *  `apply` finishes so dependent UI (TopBar, Deploy modal) doesn't have
   *  to wait for the next 5-second tick. */
  refreshSnapshot: () => Promise<void>;
};

export const useClusterStore = create<ClusterStore>((set) => ({
  mode: null,
  snapshot: null,
  connectInfo: null,
  isPolling: false,
  setMode: (mode) => set({ mode }),
  setSnapshot: (snapshot) => set({ snapshot }),
  setConnect: (connectInfo) => set({ connectInfo }),
  refreshSnapshot: async () => {
    try {
      const r = await fetch("/api/cluster/snapshot", { cache: "no-store" });
      if (!r.ok) return;
      const j = (await r.json()) as { ok: boolean; snapshot?: ClusterSnapshot };
      if (j.ok && j.snapshot) set({ snapshot: j.snapshot });
    } catch {
      /* offline; the polling loop will retry */
    }
  },
}));

/** Hook: polls /api/cluster/mode every 5s and /api/cluster/snapshot every 5s
 *  when mode is real. Mount once at the app root. */
export function useClusterPolling() {
  const setMode = useClusterStore((s) => s.setMode);
  const setSnapshot = useClusterStore((s) => s.setSnapshot);
  const setConnect = useClusterStore((s) => s.setConnect);

  useEffect(() => {
    let stopped = false;

    async function fetchMode() {
      try {
        const r = await fetch("/api/cluster/mode", { cache: "no-store" });
        if (!r.ok) return;
        const j = (await r.json()) as ModeInfo;
        if (!stopped) setMode(j);
      } catch {
        /* offline; will retry */
      }
    }

    async function fetchSnapshot() {
      try {
        const r = await fetch("/api/cluster/snapshot", { cache: "no-store" });
        if (!r.ok) return;
        const j = (await r.json()) as { ok: boolean; snapshot?: ClusterSnapshot };
        if (j.ok && j.snapshot && !stopped) setSnapshot(j.snapshot);
      } catch {
        /* offline; will retry */
      }
    }

    async function fetchConnect() {
      try {
        const r = await fetch("/api/cluster/connect", { cache: "no-store" });
        if (!r.ok) return;
        const j = (await r.json()) as ConnectInfo;
        if (!stopped) setConnect(j);
      } catch {
        /* offline */
      }
    }

    fetchMode();
    fetchConnect();
    const modeTimer = setInterval(fetchMode, 5000);
    const connectTimer = setInterval(fetchConnect, 15000);

    let snapshotTimer: ReturnType<typeof setInterval> | null = null;
    const ensureSnapshotTimer = () => {
      const m = useClusterStore.getState().mode;
      const want = m?.kubeAvailable === true;
      if (want && !snapshotTimer) {
        fetchSnapshot();
        snapshotTimer = setInterval(fetchSnapshot, 5000);
      }
      if (!want && snapshotTimer) {
        clearInterval(snapshotTimer);
        snapshotTimer = null;
      }
    };
    const supervisor = setInterval(ensureSnapshotTimer, 2000);

    return () => {
      stopped = true;
      clearInterval(modeTimer);
      clearInterval(connectTimer);
      clearInterval(supervisor);
      if (snapshotTimer) clearInterval(snapshotTimer);
    };
  }, [setMode, setSnapshot, setConnect]);
}

/** Convenience selector: derived "is real-and-Ready" state. */
export function useIsRealReady() {
  return useClusterStore((s) => s.mode?.mode === "real" && !!s.snapshot?.cluster.ready);
}
