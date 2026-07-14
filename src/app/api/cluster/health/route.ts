/**
 * GET /api/cluster/health
 * 
 * Quick health check for the Kafka cluster connection.
 * Works in both MOCK and REAL modes.
 */
import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(): Promise<Response> {
  const rt = getRuntime();
  const startTime = Date.now();

  if (rt.mode !== "real") {
    return NextResponse.json({
      ok: true,
      mode: "mock",
      message: "Running in MOCK mode - no real cluster connection",
      latencyMs: Date.now() - startTime,
    });
  }

  try {
    const { checkRealClusterHealth } = await import("@/lib/real-kafka-client");
    const health = await checkRealClusterHealth();
    
    return NextResponse.json({
      ok: health.healthy,
      mode: "real",
      brokerCount: health.brokerCount,
      controllerId: health.controllerId,
      error: health.error,
      latencyMs: Date.now() - startTime,
    });
  } catch (err) {
    return NextResponse.json({
      ok: false,
      mode: "real",
      error: err instanceof Error ? err.message : String(err),
      latencyMs: Date.now() - startTime,
    }, { status: 503 });
  }
}
