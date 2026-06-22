import { NextResponse } from "next/server";
import { killAgent, restartAgent } from "@/lib/mesh";
import type { ServerAgentId } from "@/lib/types";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const VALID: ServerAgentId[] = ["intake", "monitor", "writer", "notification"];

export async function POST(req: Request): Promise<Response> {
  const body = (await req.json().catch(() => ({}))) as { id?: ServerAgentId; op?: "kill" | "restart" };
  if (!body.id || !VALID.includes(body.id) || (body.op !== "kill" && body.op !== "restart")) {
    return NextResponse.json({ error: "id and op (kill|restart) required" }, { status: 400 });
  }
  if (body.op === "kill") killAgent(body.id);
  else await restartAgent(body.id);
  return NextResponse.json({ ok: true });
}
