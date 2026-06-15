import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";
import {
  listTopics,
  createTopic,
  deleteTopic,
  describeTopic,
  ensureRequiredTopics,
  REQUIRED_TOPICS,
} from "@/lib/kafka-admin-cfk";

export const dynamic = "force-dynamic";

/** GET /api/admin/topics — list all topics + describe required ones */
export async function GET() {
  const rt = getRuntime();
  if (rt.mode !== "real") {
    // Return mock data in MOCK mode so UI still works
    return NextResponse.json({
      mode: "MOCK",
      topics: REQUIRED_TOPICS.map(t => ({ ...t, exists: true })),
    });
  }
  try {
    const existing = await listTopics();
    const existingSet = new Set(existing);
    const topics = REQUIRED_TOPICS.map(t => ({
      name: t.name,
      partitions: t.partitions,
      retentionMs: t.retentionMs,
      cleanupPolicy: t.cleanupPolicy,
      exists: existingSet.has(t.name),
    }));
    // Include any extra topics not in REQUIRED_TOPICS
    const extra = existing.filter(n => !REQUIRED_TOPICS.some(r => r.name === n));
    return NextResponse.json({ mode: "REAL", topics, extra });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}

/** POST /api/admin/topics — create, delete, or ensure-all */
export async function POST(req: Request) {
  const rt = getRuntime();
  if (rt.mode !== "real") {
    return NextResponse.json({ ok: true, mode: "MOCK", note: "No-op in MOCK mode" });
  }

  const body = await req.json() as {
    action: "create" | "delete" | "describe" | "ensure-all";
    name?: string;
    partitions?: number;
    replication?: number;
    retentionMs?: number;
    cleanupPolicy?: "delete" | "compact";
  };

  try {
    switch (body.action) {
      case "ensure-all": {
        const result = await ensureRequiredTopics();
        return NextResponse.json({ ok: true, ...result });
      }
      case "create": {
        if (!body.name) return NextResponse.json({ error: "name required" }, { status: 400 });
        await createTopic({
          name: body.name,
          partitions: body.partitions ?? 3,
          replication: body.replication ?? 2,
          retentionMs: body.retentionMs,
          cleanupPolicy: body.cleanupPolicy,
        });
        return NextResponse.json({ ok: true, created: body.name });
      }
      case "delete": {
        if (!body.name) return NextResponse.json({ error: "name required" }, { status: 400 });
        await deleteTopic(body.name);
        return NextResponse.json({ ok: true, deleted: body.name });
      }
      case "describe": {
        if (!body.name) return NextResponse.json({ error: "name required" }, { status: 400 });
        const info = await describeTopic(body.name);
        return NextResponse.json({ ok: true, ...info });
      }
      default:
        return NextResponse.json({ error: "Unknown action" }, { status: 400 });
    }
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }
}