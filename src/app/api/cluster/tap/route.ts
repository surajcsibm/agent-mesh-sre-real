/**
 * GET  /api/cluster/tap — current tap status (connected? throughput?)
 * POST /api/cluster/tap — { action: "enable" | "disable" }
 *
 * The tap bridges the in-process simulator with the real Strimzi-managed
 * Kafka cluster. Enabled automatically once /api/cluster/install completes,
 * but can be toggled manually here as well.
 */
import { NextRequest, NextResponse } from "next/server";
import { getKafkaTap } from "@/lib/kafka/tap";
import { getRuntime } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  const tap = getKafkaTap();
  return NextResponse.json({ ok: true, status: tap.getStatus(), mode: getRuntime().mode });
}

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => ({}))) as { action?: "enable" | "disable" };
  const tap = getKafkaTap();
  if (body.action === "disable") {
    await tap.disable();
    return NextResponse.json({ ok: true, status: tap.getStatus() });
  }
  if (body.action === "enable") {
    if (getRuntime().mode !== "real") {
      return NextResponse.json(
        { ok: false, error: "Mode must be 'real' before enabling the tap. Run the Setup Wizard or POST /api/cluster/credentials first." },
        { status: 409 }
      );
    }
    try {
      await tap.enable();
      return NextResponse.json({ ok: true, status: tap.getStatus() });
    } catch (e) {
      return NextResponse.json(
        { ok: false, error: safeErr(e).message, status: tap.getStatus() },
        { status: 500 }
      );
    }
  }
  return NextResponse.json({ ok: false, error: "action must be enable|disable" }, { status: 400 });
}
