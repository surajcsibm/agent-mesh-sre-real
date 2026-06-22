/**
 * POST /api/cluster/apply — apply a Builder design to the live cluster as
 * Strimzi KafkaTopic + KafkaUser CRs. Streams progress as Server-Sent Events.
 *
 * Body: { design: BuilderDesign, namespace?: string, waitReady?: boolean }
 *
 * Events streamed (one event per SSE message, JSON-encoded):
 *   { kind: "phase",  phase: "topics"|"users",   total: number }
 *   { kind: "apply",  phase, name, resourceKind: "KafkaTopic"|"KafkaUser",
 *                     status: "applying"|"applied"|"error", message? }
 *   { kind: "ready",  phase, name, ready: boolean, message? }
 *   { kind: "snapshot", snapshot: ClusterSnapshot }
 *   { kind: "done",   ok: true }
 *   { kind: "error",  error: string }
 */
import { NextRequest } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import type { KafkaTopicResource, KafkaUserResource } from "@/lib/k8s/client";
import {
  buildTopicManifests,
  buildUserManifests,
  validateDesign,
  type TopicManifest,
  type UserManifest,
} from "@/lib/builder-codegen";
import type { BuilderDesign } from "@/lib/builder-store";
import {
  DEFAULT_CONFIG,
  snapshotCluster,
  waitForReady,
} from "@/lib/k8s/strimzi";
import { setKubeAvailable } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => ({}))) as {
    design?: BuilderDesign;
    namespace?: string;
    waitReady?: boolean;
  };

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const enc = new TextEncoder();
      const send = (obj: unknown) =>
        controller.enqueue(enc.encode(`data: ${JSON.stringify(obj)}\n\n`));

      try {
        if (!body.design || typeof body.design !== "object") {
          throw new Error("Request body must include a design.");
        }

        const v = validateDesign(body.design);
        if (!v.ok && body.design.topics.length === 0) {
          throw new Error("Design has no topics — nothing to apply.");
        }

        const namespace = body.namespace || DEFAULT_CONFIG.namespace;
        const waitReady = body.waitReady !== false; // default true
        const topics: TopicManifest[] = buildTopicManifests(body.design, namespace);
        const users: UserManifest[] = buildUserManifests(body.design, namespace);

        const client = await getK8s();
        setKubeAvailable(true);

        // -- Phase 1: KafkaTopic CRs --
        send({ kind: "phase", phase: "topics", total: topics.length });
        for (const t of topics) {
          send({ kind: "apply", phase: "topics", name: t.name, resourceKind: "KafkaTopic", status: "applying" });
          try {
            await client.applyYaml(t.yaml, "agent-mesh-builder");
            send({ kind: "apply", phase: "topics", name: t.name, resourceKind: "KafkaTopic", status: "applied" });
          } catch (e) {
            const msg = safeErr(e).message;
            send({ kind: "apply", phase: "topics", name: t.name, resourceKind: "KafkaTopic", status: "error", message: msg });
            throw new Error(`Failed to apply KafkaTopic/${t.name}: ${msg}`);
          }
        }

        // -- Phase 2: KafkaUser CRs --
        send({ kind: "phase", phase: "users", total: users.length });
        for (const u of users) {
          send({ kind: "apply", phase: "users", name: u.name, resourceKind: "KafkaUser", status: "applying" });
          try {
            await client.applyYaml(u.yaml, "agent-mesh-builder");
            send({ kind: "apply", phase: "users", name: u.name, resourceKind: "KafkaUser", status: "applied" });
          } catch (e) {
            const msg = safeErr(e).message;
            send({ kind: "apply", phase: "users", name: u.name, resourceKind: "KafkaUser", status: "error", message: msg });
            throw new Error(`Failed to apply KafkaUser/${u.name}: ${msg}`);
          }
        }

        // -- Phase 3: optionally wait for Ready --
        if (waitReady) {
          for (const t of topics) {
            const r = await waitForReady<KafkaTopicResource>(
              () => client.getKafkaTopic(t.name, namespace) as Promise<KafkaTopicResource | null>,
              { timeoutMs: 60_000, pollMs: 2_000 }
            );
            send({ kind: "ready", phase: "topics", name: t.name, ready: r.ready, message: r.message });
          }
          for (const u of users) {
            const r = await waitForReady<KafkaUserResource>(
              () => client.getKafkaUser(u.name, namespace) as Promise<KafkaUserResource | null>,
              { timeoutMs: 60_000, pollMs: 2_000 }
            );
            send({ kind: "ready", phase: "users", name: u.name, ready: r.ready, message: r.message });
          }
        }

        // -- Phase 4: refresh snapshot so the TopBar picks up the new CRs --
        const snap = await snapshotCluster(client, { namespace, cluster: DEFAULT_CONFIG.cluster });
        send({ kind: "snapshot", snapshot: snap });

        send({ kind: "done", ok: true });
      } catch (e: unknown) {
        send({ kind: "error", error: safeErr(e).message });
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-store, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
