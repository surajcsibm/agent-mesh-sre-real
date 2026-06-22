/**
 * GET /api/cluster/credentials — read bootstrap+CA+password from cluster
 * secrets and refresh the runtime mode. Used after install or whenever the
 * UI needs to verify the local app is wired up to real Kafka.
 *
 * NB: returns secrets *only* when the request originates from localhost.
 * The dev demo runs the Next.js server on the same machine as the operator,
 * so this is a safe constraint and prevents accidentally leaking creds if
 * the Next.js dev server is exposed.
 */
import { NextRequest, NextResponse } from "next/server";
import { getK8s } from "@/lib/k8s/holder";
import { getKafkaTap } from "@/lib/kafka/tap";
import { readControllerCredentials, DEFAULT_CONFIG } from "@/lib/k8s/strimzi";
import { safeErr } from "@/lib/log-safe";
import {
  getRuntime,
  publicView,
  setKafkaConnection,
  setMode,
} from "@/lib/runtime-mode";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(req: NextRequest) {
  try {
    const c = await getK8s();
    const cfg = {
      namespace: getRuntime().cluster.namespace || DEFAULT_CONFIG.namespace,
      cluster: getRuntime().cluster.name || DEFAULT_CONFIG.cluster,
    };
    const creds = await readControllerCredentials(c, cfg);
    let tapStatus: ReturnType<ReturnType<typeof getKafkaTap>["getStatus"]> | null = null;
    if (creds.password && (creds.bootstrapInternal || creds.bootstrapExternal)) {
      setKafkaConnection({
        bootstrapInternal: creds.bootstrapInternal,
        bootstrapExternal: creds.bootstrapExternal,
        username: creds.username,
        password: creds.password,
        caCertPem: creds.caCertPem,
      });
      setMode("real");
      // Auto-enable the Kafka tap so agents start consuming/producing
      // against the real cluster immediately after credentials land.
      try {
        await getKafkaTap().enable();
      } catch {
        /* fall through; status will surface the error */
      }
      tapStatus = getKafkaTap().getStatus();
    }
    const isLocal = isLocalhost(req);
    return NextResponse.json({
      ok: true,
      runtime: publicView(),
      tap: tapStatus,
      // Only expose secrets to localhost callers (the laptop demo path).
      secrets: isLocal
        ? {
            password: creds.password,
            caCertPem: creds.caCertPem,
          }
        : null,
    });
  } catch (e: unknown) {
    return NextResponse.json(
      { ok: false, error: safeErr(e).message },
      { status: 503 }
    );
  }
}

function isLocalhost(req: NextRequest): boolean {
  const host = req.headers.get("host") ?? "";
  return host.startsWith("localhost") || host.startsWith("127.0.0.1") || host.startsWith("[::1]");
}
