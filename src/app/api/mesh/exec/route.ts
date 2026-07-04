import { NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";
import { kafkaProduceAudit } from "@/lib/kafka";
import { updateTopicRetention, increaseTopicPartitions, changeTopicReplicationFactor } from "@/lib/kafka-admin-cfk";
import { getK8s } from "@/lib/k8s/holder";
import { safeErr } from "@/lib/log-safe";
import type { AuditRecord } from "@/lib/types";

const DEMO_CONSUMER_DEPLOYMENT = "demo-consumer";
const DEMO_CONSUMER_NAMESPACE = "confluent";
const DEMO_CONSUMER_LABEL = "app=demo-consumer";

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
      // "describe-share-group" removed — confirmed non-functional. The
      // WebSocket exec upgrade to DigitalOcean's managed control-plane
      // gateway is rejected with a 403 independent of RBAC/auth (verified:
      // the identical scoped credential succeeds via kubectl directly,
      // ruling out permissions; the request headers were confirmed
      // correctly formed, ruling out a missing/malformed auth header).
      // REST-style Admin API calls to this same server work fine from
      // Vercel — only the raw WebSocket-upgrade code path in
      // @kubernetes/client-node's Exec class is affected, most likely by
      // gateway-level handling of Connection:Upgrade traffic that differs
      // from plain HTTPS. Share-Group Rebalance now uses honest narrative
      // text instead (see runShareGroupRebalance in client-sim.ts) rather
      // than a real remote action for this scenario's "Act" step.
      case "change-topic-replication-factor": {
        const newRF = (body as { newReplicationFactor?: number }).newReplicationFactor;
        if (!body.topicName || newRF === undefined) {
          return NextResponse.json({ error: "topicName and newReplicationFactor required" }, { status: 400 });
        }
        const rfResult = await changeTopicReplicationFactor(body.topicName, newRF);
        return NextResponse.json({ ok: true, action: body.action, topicName: body.topicName, newReplicationFactor: newRF, ...rfResult });
      }
      case "increase-topic-partitions": {
        const newCount = (body as { newPartitionCount?: number }).newPartitionCount;
        if (!body.topicName || newCount === undefined) {
          return NextResponse.json({ error: "topicName and newPartitionCount required" }, { status: 400 });
        }
        await increaseTopicPartitions(body.topicName, newCount);
        return NextResponse.json({ ok: true, action: body.action, topicName: body.topicName, newPartitionCount: newCount });
      }
      case "scale-consumer-group": {
        const replicas = (body as { replicas?: number }).replicas;
        if (replicas === undefined || replicas < 0) {
          return NextResponse.json({ error: "replicas (>=0) required" }, { status: 400 });
        }
        const k8s = await getK8s();
        await k8s.scaleDeployment(DEMO_CONSUMER_DEPLOYMENT, DEMO_CONSUMER_NAMESPACE, replicas);
        return NextResponse.json({ ok: true, action: body.action, replicas });
      }
      case "restart-consumer-group": {
        const k8s = await getK8s();
        const podList = await k8s.listPods(DEMO_CONSUMER_NAMESPACE, DEMO_CONSUMER_LABEL);
        const pods = (podList as { body?: { items?: { metadata?: { name?: string } }[] } }).body?.items
          ?? (podList as { items?: { metadata?: { name?: string } }[] }).items
          ?? [];
        const names = pods.map((p) => p.metadata?.name).filter((n): n is string => !!n);
        for (const name of names) {
          await k8s.deletePod(name, DEMO_CONSUMER_NAMESPACE);
        }
        return NextResponse.json({ ok: true, action: body.action, restartedPods: names });
      }
      default:
        return NextResponse.json({ error: "Unknown action" }, { status: 400 });
    }
  } catch (e) {
    // JSON.stringify(e) on a plain Error produces "{}" — message/stack are
    // non-enumerable on Error.prototype and get skipped by default. Using
    // Object.getOwnPropertyNames(e) as the replacer forces inclusion of
    // those own-but-non-enumerable properties, so we actually see what
    // failed instead of an opaque empty object.
    let fullSerialized = "(unserializable)";
    try {
      fullSerialized = JSON.stringify(e, e instanceof Error ? Object.getOwnPropertyNames(e) : undefined);
    } catch { /* ignore */ }

    console.error("[api/mesh/exec] Full error detail:", fullSerialized, "raw:", e);

    return NextResponse.json({
      error: safeErr(e).message,
      debugFull: fullSerialized,
      debugType: typeof e,
      debugIsError: e instanceof Error,
    }, { status: 500 });
  }
}
