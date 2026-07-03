import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";
import { kafkaProduceAudit } from "@/lib/kafka";
import { updateTopicRetention } from "@/lib/kafka-admin-cfk";
import { safeErr } from "@/lib/log-safe";
import type { AuditRecord } from "@/lib/types";

export const dynamic = "force-dynamic";

/**
 * POST /api/mesh/exec
 *
 * "Exec-only" endpoint for real-cluster mutations triggered by scenario
 * animations in client-sim.ts. Fires the real Kafka action without pushing
 * SSE events \u2014 the animation already drives UI state client-side. This is
 * the endpoint referenced (but never built) in useMeshStream.ts's trigger()
 * comment: "For REAL Kafka mode ... wire up a separate exec-only endpoint."
 *
 * Supported actions:
 *   ack-controller-failover \u2014 real AuditRecord write to ops.actions.audit.v1.
 *     No cluster mutation \u2014 pure audit-log append acknowledging an already-
 *     observed KRaft election.
 *   update-topic-retention \u2014 real admin.alterConfigs call via
 *     kafka-admin-cfk.ts. Only used when a topic edit changes retention with
 *     no partition-count change (partition/RF changes stay simulation-only \u2014
 *     the Kafka Admin API cannot decrease partitions or arbitrarily change
 *     replication factor without reassignment tooling this app doesn't have).
 */
export async function POST(req: Request) {
  const rt = getRuntime();
  if (rt.mode !== "real") {
    return NextResponse.json({ ok: true, mode: "MOCK", note: "No-op in MOCK mode" });
  }

  const body = (await req.json()) as {
    action: string;
    record?: Partial<AuditRecord>;
    topicName?: string;
    retentionMs?: number;
  };

  try {
    switch (body.action) {
      case "ack-controller-failover": {
        const record: AuditRecord = {
          id: body.record?.id ?? crypto.randomUUID(),
          ts: Date.now(),
          type: "tool-call",
          agent: "monitor",
          summary: body.record?.summary ?? "Acknowledged controller failover",
          topic: "ops.actions.audit.v1",
        };
        kafkaProduceAudit(record);
        return NextResponse.json({ ok: true, action: body.action });
      }
      case "update-topic-retention": {
        if (!body.topicName || body.retentionMs === undefined) {
          return NextResponse.json({ error: "topicName and retentionMs required" }, { status: 400 });
        }
        await updateTopicRetention(body.topicName, body.retentionMs);
        return NextResponse.json({ ok: true, action: body.action, topicName: body.topicName });
      }
      default:
        return NextResponse.json({ error: "Unknown action" }, { status: 400 });
    }
  } catch (e) {
    return NextResponse.json({ error: safeErr(e).message }, { status: 500 });
  }
}
