/**
 * POST /api/cluster/uninstall — tear down everything in deploy/base in
 * reverse order. Strimzi operator itself is left alone.
 */
import { NextResponse } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import { INSTALL_STEPS, readStepYaml, DEFAULT_CONFIG } from "@/lib/k8s/strimzi";
import { parseAllDocuments } from "yaml";
import { getRuntime, setMode } from "@/lib/runtime-mode";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST() {
  try {
    const c = await getK8s();
    const cfg = {
      namespace: getRuntime().cluster.namespace || DEFAULT_CONFIG.namespace,
      cluster: getRuntime().cluster.name || DEFAULT_CONFIG.cluster,
    };

    const removed: { kind: string; name: string; ok: boolean; message?: string }[] = [];
    // Reverse order so dependents are removed first.
    for (const step of [...INSTALL_STEPS].reverse()) {
      const yamlText = await readStepYaml(step, cfg);
      const docs = parseAllDocuments(yamlText)
        .map((d) => d.toJS())
        .filter((d) => d && typeof d === "object" && d.kind);
      for (const obj of docs) {
        const kind = obj.kind as string;
        const name = obj.metadata?.name as string;
        try {
          if (!obj.metadata?.name) continue;
          const header = {
            apiVersion: obj.apiVersion as string,
            kind,
            metadata: {
              name,
              namespace: obj.metadata?.namespace as string | undefined,
            },
          };
          // Use the underlying objectApi via the client's exposed read/delete-equivalent.
          // We mimic by calling delete via custom API for CR + core for builtins.
          await deleteAny(c, header);
          removed.push({ kind, name, ok: true });
        } catch (e: unknown) {
          removed.push({ kind, name, ok: false, message: safeErr(e).message });
        }
      }
    }

    setMode("mock");
    return NextResponse.json({ ok: true, removed });
  } catch (e: unknown) {
    return NextResponse.json(
      { ok: false, error: safeErr(e).message },
      { status: 503 }
    );
  }
}

import type { K8sClient } from "@/lib/k8s/client";
import { safeErr } from "@/lib/log-safe";
async function deleteAny(
  c: K8sClient,
  header: { apiVersion: string; kind: string; metadata: { name: string; namespace?: string } }
): Promise<void> {
  // KubernetesObjectApi.delete handles built-in & CR uniformly.
  // The client class doesn't expose objectApi.delete directly, so we go
  // through its kc and rebuild a one-shot client for this call.
  // Cheap and isolated — uninstall is called rarely.
  const k8s = await import("@kubernetes/client-node");
  const api = k8s.KubernetesObjectApi.makeApiClient(c.kc);
  try {
    await api.delete(header);
  } catch (e: unknown) {
    const code = (e as { statusCode?: number; code?: number }).statusCode ??
                 (e as { code?: number }).code;
    if (code !== 404) throw e;
  }
}
