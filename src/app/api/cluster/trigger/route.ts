/**
 * POST /api/cluster/trigger
 * 
 * Trigger REAL Kafka scenarios on the cluster.
 * This creates actual conditions (lag, messages, etc.) - NOT simulations.
 * 
 * Supported scenarios:
 * - lag-spike: Produce many messages to create real consumer lag
 * - produce-burst: Send a burst of messages to a topic
 * - health-check: Verify cluster connectivity
 * 
 * Body: { scenario: string, options?: object }
 */
import { NextRequest, NextResponse } from "next/server";
import { getRuntime } from "@/lib/runtime-mode";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

interface TriggerRequest {
  scenario: "lag-spike" | "produce-burst" | "health-check" | "get-lag";
  options?: {
    topic?: string;
    messageCount?: number;
    groupId?: string;
  };
}

export async function POST(req: NextRequest): Promise<Response> {
  const rt = getRuntime();
  
  if (rt.mode !== "real") {
    return NextResponse.json({
      ok: false,
      error: "Real triggers only available in REAL mode. Set KAFKA_MODE=real in .env.local",
      mode: rt.mode,
    }, { status: 400 });
  }

  let body: TriggerRequest;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body" }, { status: 400 });
  }

  if (!body.scenario) {
    return NextResponse.json({ ok: false, error: "Missing scenario field" }, { status: 400 });
  }

  try {
    const {
      triggerRealLagSpike,
      getRealConsumerLag,
      checkRealClusterHealth,
      produceRealMessage,
    } = await import("@/lib/real-kafka-client");

    switch (body.scenario) {
      case "lag-spike": {
        const topic = body.options?.topic || "demo.payments.events";
        const messageCount = body.options?.messageCount || 1000;
        const result = await triggerRealLagSpike(topic, messageCount);
        return NextResponse.json({
          ok: true,
          scenario: "lag-spike",
          result,
          message: `Produced ${result.produced} messages to ${result.topic} to create lag`,
        });
      }

      case "produce-burst": {
        const topic = body.options?.topic || "ops.kafka.metrics.v1";
        const messageCount = body.options?.messageCount || 100;
        const results = [];
        
        for (let i = 0; i < messageCount; i++) {
          const result = await produceRealMessage(topic, {
            type: "burst-message",
            index: i,
            ts: Date.now(),
            source: "agent-mesh-sre-trigger",
          }, `burst-${Date.now()}-${i}`);
          results.push(result);
        }
        
        return NextResponse.json({
          ok: true,
          scenario: "produce-burst",
          produced: results.length,
          topic,
          message: `Produced ${results.length} messages to ${topic}`,
        });
      }

      case "get-lag": {
        const groupId = body.options?.groupId || "payments-consumer";
        const topic = body.options?.topic || "demo.payments.events";
        const lag = await getRealConsumerLag(groupId, topic);
        return NextResponse.json({
          ok: true,
          scenario: "get-lag",
          result: lag,
        });
      }

      case "health-check": {
        const health = await checkRealClusterHealth();
        return NextResponse.json({
          ok: health.healthy,
          scenario: "health-check",
          result: health,
        });
      }

      default:
        return NextResponse.json({
          ok: false,
          error: `Unknown scenario: ${body.scenario}`,
          supportedScenarios: ["lag-spike", "produce-burst", "health-check", "get-lag"],
        }, { status: 400 });
    }
  } catch (err) {
    return NextResponse.json({
      ok: false,
      error: err instanceof Error ? err.message : String(err),
      scenario: body.scenario,
    }, { status: 503 });
  }
}
