import { killAgent, restartAgent } from "@/lib/mesh";
import { NextResponse }             from "next/server";
import type { ServerAgentId }             from "@/lib/types";

export const dynamic = "force-dynamic";

export async function POST(req: Request) {
  const { agentId, action } = await req.json() as { agentId: ServerAgentId; action: "kill" | "restart" };
  if (action === "kill")    return NextResponse.json(killAgent(agentId));
  if (action === "restart") return NextResponse.json(await restartAgent(agentId));
  return NextResponse.json({ error: "Unknown action" }, { status: 400 });
}
