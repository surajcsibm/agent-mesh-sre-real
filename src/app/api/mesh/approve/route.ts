import { resolveApproval } from "@/lib/mesh";
import { NextResponse }     from "next/server";
import { z } from "zod";
import { safeErr } from "@/lib/log-safe";

export const dynamic = "force-dynamic";

const ApproveRequest = z.object({
  id: z.string().min(1, "id is required"),
  decision: z.enum(["approve", "reject"]),
  actor: z.string().min(1).max(200).optional(),
});

export async function POST(req: Request) {
  let body: unknown;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Request body must be valid JSON" }, { status: 400 });
  }

  const parsed = ApproveRequest.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid payload", details: parsed.error.flatten().fieldErrors },
      { status: 400 }
    );
  }

  try {
    const { id, decision, actor } = parsed.data;
    const ok = resolveApproval(id, decision, actor ?? "ops-engineer");
    if (!ok) {
      return NextResponse.json({ ok: false, error: "Approval not found or already resolved" }, { status: 404 });
    }
    return NextResponse.json({ ok: true });
  } catch (e) {
    console.error("[api/mesh/approve] resolveApproval failed:", safeErr(e));
    return NextResponse.json(
      { ok: false, error: "Failed to resolve approval", details: safeErr(e).message },
      { status: 500 }
    );
  }
}
