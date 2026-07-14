/**
 * GET /api/cluster/metrics
 * 
 * Collect REAL metrics from the Kafka cluster.
 * Returns actual broker count, controller epoch, topic info, consumer group lag, etc.
 * 
 * This endpoint only works when KAFKA_MODE=real is set.
 */
import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(): Promise<Response> {
  const rt = getRuntime();
  
  if (rt.mode !== "real") {
    return NextResponse.json({
      ok: false,
      error: "Cluster metrics only available in REAL mode. Set KAFKA_MODE=real in .env.local",
      mode: rt.mode,
    }, { status: 400 });
  }

  try {
    const { collectRealClusterMetrics } = await import("@/lib/real-kafka-client");
    const metrics = await collectRealClusterMetrics();
    
    return NextResponse.json({
      ok: true,
      mode: "real",
      metrics,
    });
  } catch (err) {
    return NextResponse.json({
      ok: false,
      error: err instanceof Error ? err.message : String(err),
      mode: rt.mode,
    }, { status: 503 });
  }
}
