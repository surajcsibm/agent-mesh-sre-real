import { NextResponse } from "next/server";
import { getMesh } from "@/lib/mesh";
import { getRuntime } from "@/lib/runtime-mode";
import { getK8s } from "@/lib/k8s/holder";
import { runScenario, ScenarioKind } from "@/lib/k8s/scenarios";
import { safeErr } from "@/lib/log-safe";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const ALLOWED = new Set<ScenarioKind>([
  "lag-spike",
  "controller-failover",
  "share-group-rebalance",
  "partition-imbalance",
]);

export async function POST(req: Request): Promise<Response> {
  const body = (await req.json().catch(() => ({}))) as { kind?: ScenarioKind };
  if (!body.kind || !ALLOWED.has(body.kind)) {
    return NextResponse.json(
      { error: "kind must be one of " + Array.from(ALLOWED).join(", ") },
      { status: 400 }
    );
  }

  // Always drive the in-process simulator so the canvas keeps animating.
  // (When the real KafkaJS backend lands in Step 4, the simulator becomes
  // a fallback, but for now both paths run side-by-side so the demo never
  // "looks frozen" while the real cluster is mutating in the background.)
  const mesh = getMesh();
  const simOut = mesh.triggerScenario(body.kind as never);

  // In real mode, also mutate the live Strimzi-managed cluster.
  const r = getRuntime();
  let realResult: Awaited<ReturnType<typeof runScenario>> | null = null;
  if (r.mode === "real") {
    try {
      const c = await getK8s();
      realResult = await runScenario(c, body.kind, r.cluster.namespace, r.cluster.name);
    } catch (e: unknown) {
      // Real cluster failure is non-fatal: the simulator already responded.
      const msg = safeErr(e).message;
      return NextResponse.json({
        ok: true,
        sim: simOut,
        realError: msg,
        warning: "Real-mode scenario dispatch failed; simulator still ran.",
      });
    }
  }

  return NextResponse.json({
    ok: true,
    sim: simOut,
    real: realResult,
    mode: r.mode,
  });
}
