// POST /api/notify — sends a scenario summary email.
// Called by the client-side simulation so email works on Vercel
// (where the server-side mesh singleton is isolated per instance).
import { sendAgentSummary } from "@/lib/emailer";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(req: Request) {
  const body = await req.json().catch(() => ({})) as {
    scenarioId?: string;
    scenarioLabel?: string;
    action?: string;
    lagBefore?: number;
    lagAfter?: number;
    approvedBy?: string;
  };

  const scenarioId    = body.scenarioId    ?? "demo";
  const scenarioLabel = body.scenarioLabel ?? "Demo Scenario";
  const lagBefore     = body.lagBefore     ?? 4200;
  const lagAfter      = body.lagAfter      ?? 0;
  const approvedBy    = body.approvedBy    ?? "vp-engineering@stage";
  const actionTaken   = body.action        ?? "kafka.scaleConsumers";

  const result = await sendAgentSummary({
    scenarioId,
    scenarioLabel,
    ts: Date.now(),
    reasoning: {
      rootCause:         `Consumer lag spike detected (${lagBefore} msgs behind)`,
      confidence:        0.94,
      kafkaFeatureCited: "KIP-848 Share Groups",
      rebalanceState:    "Rebalancing",
      controllerEpoch:   14,
      crossCorrelation: {
        brokers: "healthy", jvmHeap: "68%",
        networkInRate: "↑ 2.1×", rebalanceInProgress: true,
      },
      recommendedAction: actionTaken,
      requiresApproval:  true,
      rationale:         `Lag growth rate exceeded SLO threshold. ${actionTaken} resolved in ~10s.`,
      lessonsCited:      ["lesson-003"],
    },
    action: {
      approved:        true,
      approvedBy,
      outcome:         "success",
      detail:          `${actionTaken} executed successfully — lag ${lagBefore}→${lagAfter}`,
      lagBefore,
      lagAfter,
      toolCalled:      actionTaken,
      clusterMutation: "ConsumerGroupScaleOut",
    },
    lesson: {
      id:                `lesson-${Date.now()}`,
      ts:                Date.now(),
      scenarioId,
      actionTaken,
      effective:         true,
      lagBefore,
      lagAfter,
      adjustedThreshold: 150,
      notes:             "Threshold tightened from 300→150 msg/s for faster response next time.",
    },
    slackMessage: `*${scenarioLabel}* | Action: ${actionTaken} | Lag: ${lagBefore}→${lagAfter} | Approved by: ${approvedBy}`,
    itsmTicket:   `INC-${Date.now().toString().slice(-5)} opened: ${actionTaken} — ${scenarioId}`,
    approvedBy,
  });

  if (result.ok) {
    return NextResponse.json({ ok: true, messageId: result.messageId });
  }
  return NextResponse.json(
    { ok: false, error: result.error },
    { status: result.error === "smtp_not_configured" ? 501 : 500 }
  );
}
