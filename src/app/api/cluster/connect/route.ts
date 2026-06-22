/**
 * GET /api/cluster/connect — pings the active K8s context.
 * POST /api/cluster/connect — same, but force re-detect.
 */
import { NextResponse } from "next/server";
import { getK8s, lastK8sError, resetK8s } from "@/lib/k8s/holder";
import { setKubeAvailable } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

async function probe() {
  try {
    const c = await getK8s();
    const ping = await c.ping();
    const strimzi = ping.connected ? await c.hasStrimziCrds() : { present: false };
    setKubeAvailable(!!ping.connected);
    return {
      ok: ping.connected,
      ...ping,
      strimzi,
      hint:
        !ping.connected
          ? "Run `oc login` (or set KUBECONFIG) and retry."
          : !strimzi.present
            ? "Strimzi CRDs not found. Install Strimzi 0.51.0 from OperatorHub."
            : undefined,
    };
  } catch (e: unknown) {
    setKubeAvailable(false);
    return {
      ok: false,
      connected: false,
      inCluster: false,
      error: safeErr(e).message,
      hint:
        lastK8sError() ??
        "Could not load a Kubernetes config. Run `oc login` (or set KUBECONFIG) and retry.",
    };
  }
}

export async function GET() {
  return NextResponse.json(await probe());
}

export async function POST() {
  resetK8s();
  return NextResponse.json(await probe());
}
