/**
 * GET /api/cluster/snapshot — full state of Kafka, Topics, Users, NodePools
 * in the demo namespace. Driven by the live nav bar and the wizard.
 */
import { NextResponse } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import { snapshotCluster, DEFAULT_CONFIG } from "@/lib/k8s/strimzi";
import { getRuntime } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const c = await getK8s();
    const cfg = {
      namespace: getRuntime().cluster.namespace || DEFAULT_CONFIG.namespace,
      cluster: getRuntime().cluster.name || DEFAULT_CONFIG.cluster,
    };
    const snap = await snapshotCluster(c, cfg);
    return NextResponse.json({ ok: true, snapshot: snap });
  } catch (e: unknown) {
    return NextResponse.json(
      { ok: false, error: safeErr(e).message },
      { status: 503 }
    );
  }
}
