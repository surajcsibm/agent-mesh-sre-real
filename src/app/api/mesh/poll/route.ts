import { NextResponse } from "next/server";
import { getMonitorPollState, getLatestSnapshot, startMonitorPolling, stopMonitorPolling } from "@/lib/monitor-poll";

export async function GET() {
  return NextResponse.json({ poll: getMonitorPollState(), latestSnapshot: getLatestSnapshot() });
}

export async function POST(req: Request) {
  const { action } = await req.json();
  if (action === "start") { startMonitorPolling(); return NextResponse.json({ ok: true, action: "started" }); }
  if (action === "stop")  { stopMonitorPolling();  return NextResponse.json({ ok: true, action: "stopped" }); }
  return NextResponse.json({ error: "Unknown action" }, { status: 400 });
}
