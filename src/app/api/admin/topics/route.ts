import { NextResponse } from "next/server";
import { z } from "zod";
import { getRuntime } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";
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
      { error: safeErr(e).message },
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

  let rawBody: unknown;
  try {
    rawBody = await req.json();
  } catch {
    return NextResponse.json({ error: "Request body must be valid JSON" }, { status: 400 });
  }

  // Kafka topic names are restricted to this character set in practice, and
  // names starting with "_" are internal/reserved (e.g. __consumer_offsets,
  // __transaction_state) — the same convention listTopics() already filters
  // on elsewhere in kafka-admin-cfk.ts. Blocking it here too means a caller
  // can never target an internal topic via this route, even by accident.
  const AdminTopicsRequest = z.object({
    action: z.enum(["create", "delete", "describe", "ensure-all"]),
    name: z.string()
      .min(1)
      .max(249)
      .regex(/^[a-zA-Z0-9._-]+$/, "Topic name may only contain letters, numbers, '.', '_', and '-'")
      .refine((n) => !n.startsWith("_"), "Cannot target internal/reserved Kafka topics (names starting with _)")
      .optional(),
    partitions: z.number().int().min(1).max(1000).optional(),
    replication: z.number().int().min(1).max(32).optional(),
    retentionMs: z.number().int().min(-1).optional(),
    cleanupPolicy: z.enum(["delete", "compact"]).optional(),
  }).superRefine((data, ctx) => {
    if ((data.action === "create" || data.action === "delete" || data.action === "describe") && !data.name) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["name"], message: "name is required for this action" });
    }
  });

  const parsed = AdminTopicsRequest.safeParse(rawBody);
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid payload", details: parsed.error.flatten().fieldErrors },
      { status: 400 }
    );
  }
  const body = parsed.data;

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
      { error: safeErr(e).message },
      { status: 500 }
    );
  }
}