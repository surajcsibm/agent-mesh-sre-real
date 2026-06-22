/**
 * POST /api/cluster/install — streams install progress as SSE.
 *
 * Body (all optional):
 *   { skipWait?: boolean }
 *
 * Events streamed:
 *   { kind: "step", step, phase, applyResults?, ready?, message?, durationMs? }
 *   { kind: "snapshot", snapshot }
 *   { kind: "credentials", credentials }
 *   { kind: "done", ok: true }
 *   { kind: "error", error }
 */
import { NextRequest } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import { getKafkaTap } from "@/lib/kafka/tap";
import {
  applyStep,
  DEFAULT_CONFIG,
  INSTALL_STEPS,
  readControllerCredentials,
  snapshotCluster,
  waitForReady,
} from "@/lib/k8s/strimzi";
import { getRuntime, setKafkaConnection, setKubeAvailable, setMode } from "@/lib/runtime-mode";
import { safeErr } from "@/lib/log-safe";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(req: NextRequest) {
  const body = (await req.json().catch(() => ({}))) as { skipWait?: boolean };
  const skipWait = !!body.skipWait;

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const enc = new TextEncoder();
      const send = (obj: unknown) =>
        controller.enqueue(enc.encode(`data: ${JSON.stringify(obj)}\n\n`));

      try {
        const client = await getK8s();
        setKubeAvailable(true);
        const cfg = {
          namespace: getRuntime().cluster.namespace || DEFAULT_CONFIG.namespace,
          cluster: getRuntime().cluster.name || DEFAULT_CONFIG.cluster,
        };

        // 1) Apply each step in sequence
        for (const step of INSTALL_STEPS) {
          const startedAt = Date.now();
          send({ kind: "step", step, phase: "applying", startedAt });
          const applyResults = await applyStep(client, step, cfg);
          send({
            kind: "step",
            step,
            phase: "applied",
            applyResults,
            durationMs: Date.now() - startedAt,
          });
        }

        // 2) Wait for the Kafka cluster to become Ready (longest step)
        if (!skipWait) {
          send({
            kind: "step",
            step: { id: "ready-kafka", file: "", description: `Wait for Kafka/${cfg.cluster} Ready` },
            phase: "waiting",
            startedAt: Date.now(),
          });
          const r = await waitForReady(
            () => client.getKafka(cfg.cluster, cfg.namespace),
            { timeoutMs: 600_000, pollMs: 3_000 }
          );
          send({
            kind: "step",
            step: { id: "ready-kafka", file: "", description: `Kafka/${cfg.cluster}` },
            phase: r.ready ? "ready" : "error",
            ready: r.ready,
            message: r.message,
          });
          if (!r.ready) throw new Error(`Kafka/${cfg.cluster} did not reach Ready: ${r.message}`);

          // 3) Wait for credentials secret to exist
          send({
            kind: "step",
            step: { id: "credentials", file: "", description: "Reading credentials" },
            phase: "waiting",
            startedAt: Date.now(),
          });
          const creds = await readControllerCredentials(client, cfg);
          if (creds.password && creds.bootstrapInternal) {
            setKafkaConnection({
              bootstrapInternal: creds.bootstrapInternal,
              bootstrapExternal: creds.bootstrapExternal,
              username: creds.username,
              password: creds.password,
              caCertPem: creds.caCertPem,
            });
            setMode("real");
            // Auto-enable the Kafka tap so the agents start producing into
            // and consuming from the live cluster immediately. Best-effort —
            // the wizard surfaces the failure but the install step itself
            // is still considered done.
            try {
              await getKafkaTap().enable();
            } catch (e) {
              send({
                kind: "step",
                step: { id: "credentials", file: "", description: "Tap connect failed" },
                phase: "error",
                message: safeErr(e).message,
              });
            }
          }
          send({
            kind: "credentials",
            credentials: {
              bootstrapInternal: creds.bootstrapInternal,
              bootstrapExternal: creds.bootstrapExternal,
              username: creds.username,
              hasPassword: !!creds.password,
              hasCaCert: !!creds.caCertPem,
            },
          });
        }

        // 4) Final snapshot
        const snap = await snapshotCluster(client, cfg);
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
