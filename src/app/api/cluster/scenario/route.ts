/**
 * POST /api/cluster/scenario — run a real-cluster scenario by mutating
 * Strimzi-managed workloads (kubectl scale / oc set env / oc delete pod).
 *
 * Body: { kind: "lag-spike" | "controller-failover" | "share-group-rebalance" | "partition-imbalance" | "reset" }
 *
 * The /api/scenario route delegates here when runtime mode is `real`.
 */
import { NextRequest, NextResponse } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import { runScenario, ScenarioKind } from "@/lib/k8s/scenarios";
import { getRuntime } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  try {
    const body = (await req.json()) as { kind?: ScenarioKind };
    if (!body?.kind) {
      return NextResponse.json({ ok: false, error: "missing kind" }, { status: 400 });
    }
    const c = await getK8s();
    const r = getRuntime();
    const result = await runScenario(c, body.kind, r.cluster.namespace, r.cluster.name);
    return NextResponse.json({ ok: result.ok, result });
  } catch (e: unknown) {
    return NextResponse.json(
      { ok: false, error: safeErr(e).message },
      { status: 503 }
    );
  }
}
