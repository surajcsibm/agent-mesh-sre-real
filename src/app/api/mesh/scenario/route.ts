import { triggerScenario } from "@/lib/mesh";
import { NextResponse }     from "next/server";
import { z } from "zod";
import { safeErr } from "@/lib/log-safe";

export const dynamic = "force-dynamic";

const ScenarioRequest = z.object({
  id: z.enum(["lag-spike", "controller-failover", "share-group", "benign-rebalance"]),
});

export async function POST(req: Request) {
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Request body must be valid JSON" }, { status: 400 });
  }

  const parsed = ScenarioRequest.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid request", details: parsed.error.flatten().fieldErrors },
      { status: 400 }
    );
  }

  try {
    const result = triggerScenario(parsed.data.id);
    return NextResponse.json(result);
  } catch (e) {
    console.error("[api/mesh/scenario] triggerScenario failed:", safeErr(e));
    return NextResponse.json(
      { error: "Failed to trigger scenario", details: safeErr(e).message },
      { status: 500 }
    );
  }
}
