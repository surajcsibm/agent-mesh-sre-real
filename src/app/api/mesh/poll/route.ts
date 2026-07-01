import { NextResponse } from "next/server";
import { getMonitorPollState, getLatestSnapshot, startMonitorPolling, stopMonitorPolling, runPollCycle } from "@/lib/monitor-poll";
import { startAnomalySimulation, getAnomalySimState } from "@/lib/anomaly-sim";
import { safeErr } from "@/lib/log-safe";

export async function GET() {
  // Run a real poll cycle directly on each request. This decouples real
  // Kafka admin metrics collection from both the SSE stream's 60s timeout
  // ceiling and the setInterval loop's cold-start fragility on serverless.
  try {
    await runPollCycle();
  } catch (e) {
    console.warn("[api/mesh/poll] runPollCycle error:", safeErr(e));
  }
  return NextResponse.json({ poll: getMonitorPollState(), latestSnapshot: getLatestSnapshot(), anomalySim: getAnomalySimState() });
}

export async function POST(req: Request) {
  const { action } = await req.json();
  if (action === "start") { startMonitorPolling(); return NextResponse.json({ ok: true, action: "started" }); }
  if (action === "stop")  { stopMonitorPolling();  return NextResponse.json({ ok: true, action: "stopped" }); }
  return NextResponse.json({ error: "Unknown action" }, { status: 400 });
}
