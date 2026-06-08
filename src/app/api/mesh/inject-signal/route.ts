import { NextResponse }     from "next/server";
import { patchBrokerState, getSnapshot } from "@/lib/mesh";
import type { BrokerState } from "@/lib/types";

const EFFECTS: Record<string, (b: BrokerState) => void> = {
  "lag-spike":        (b) => { b.consumerGroups["payments-consumer"] = { lag: 24_000, rebalanceState: "stable", members: 3 }; },
  "controller-failover": (b) => { b.controllerEpoch += 1; },
  "share-group":      (b) => { b.consumerGroups["share-group-1"] = { lag: 15_000, rebalanceState: "stable", members: 2 }; },
  "benign-rebalance": (b) => { b.consumerGroups["payments-consumer"] = { lag: 800, rebalanceState: "rebalancing", members: 3 }; },
};

const EXTENDED = ["schema-mismatch","disk-saturation","under-replication","producer-timeout","consumer-session-timeout","compaction-lag"];

export async function POST(req: Request) {
  try {
    const { scenario } = (await req.json()) as { scenario?: string };
    if (!scenario) return NextResponse.json({ ok: false, error: "missing scenario" }, { status: 400 });

    const before = getSnapshot().broker.controllerEpoch;

    patchBrokerState((b) => {
      const effect = EFFECTS[scenario];
      if (effect) {
        effect(b);
      } else if (EXTENDED.includes(scenario)) {
        if (!(b as Record<string,unknown>).signalledScenarios)
          (b as Record<string,unknown>).signalledScenarios = {};
        ((b as Record<string,unknown>).signalledScenarios as Record<string,number>)[scenario] = Date.now();
      }
    });

    const after = getSnapshot().broker.controllerEpoch;

    return NextResponse.json({
      ok: true, scenario,
      epochBefore: before,
      epochAfter:  after,
      writeWorked: after !== before || !EFFECTS[scenario],
    });
  } catch (e) {
    return NextResponse.json({ ok: false, error: String(e) }, { status: 500 });
  }
}
