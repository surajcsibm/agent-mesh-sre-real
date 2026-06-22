/**
 * Real scenario executor. Each scenario maps to a sequence of cluster
 * mutations against the demo workloads in `deploy/base/05-demo-workloads.yaml`.
 *
 * Buttons in the UI and the matching shell scripts in `deploy/scenarios/`
 * dispatch through here in `real` mode.
 */
import "server-only";
import { K8sClient } from "./client";

export type ScenarioKind =
  | "lag-spike"
  | "controller-failover"
  | "share-group-rebalance"
  | "partition-imbalance"   // = benign rebalance, name kept for compatibility
  | "reset";

export type ScenarioStep = {
  description: string;
  command: string;          // shell-equivalent so the UI can show what's happening
  startedAt: number;
  finishedAt?: number;
  status: "running" | "ok" | "error";
  message?: string;
};

export type ScenarioResult = {
  kind: ScenarioKind;
  ranAt: string;
  steps: ScenarioStep[];
  ok: boolean;
};

export async function runScenario(
  client: K8sClient,
  kind: ScenarioKind,
  namespace = "agent-mesh-sre",
  cluster = "agent-mesh-kafka"
): Promise<ScenarioResult> {
  const steps: ScenarioStep[] = [];
  const exec = async (
    description: string,
    command: string,
    fn: () => Promise<unknown>
  ): Promise<void> => {
    const step: ScenarioStep = {
      description,
      command,
      startedAt: Date.now(),
      status: "running",
    };
    steps.push(step);
    try {
      await fn();
      step.status = "ok";
    } catch (e: unknown) {
      step.status = "error";
      step.message = errorMessage(e);
    } finally {
      step.finishedAt = Date.now();
    }
  };

  switch (kind) {
    case "lag-spike":
      await exec(
        "Inject 200ms artificial delay into slow-consumer",
        `oc set env deploy/slow-consumer -n ${namespace} DELAY_MS=200`,
        () => client.setDeploymentEnv("slow-consumer", namespace, "consumer", "DELAY_MS", "200")
      );
      await exec(
        "Scale fast-producer to 2 replicas",
        `oc scale deploy/fast-producer -n ${namespace} --replicas=2`,
        () => client.scaleDeployment("fast-producer", namespace, 2)
      );
      break;

    case "controller-failover": {
      let target = `${cluster}-controller-0`;
      await exec(
        "Identify active KRaft controller pod",
        `oc get pod -l strimzi.io/cluster=${cluster},strimzi.io/pool-name=controller -n ${namespace}`,
        async () => {
          const pods = await client.listPods(
            namespace,
            `strimzi.io/cluster=${cluster},strimzi.io/pool-name=controller`
          );
          const podList = (pods && typeof pods === 'object' && 'body' in pods) ? (pods as {body: {items: unknown[]}}).body.items : (pods as {items: unknown[]}).items;
          const annotated = (podList as {metadata?: {annotations?: Record<string,string>; name?: string}}[]).find(
            (p) => p.metadata?.annotations?.["strimzi.io/kraft-controller-id"]
          );
          target = annotated?.metadata?.name ?? target;
        }
      );
      await exec(
        `Delete controller pod ${target}`,
        `oc delete pod ${target} -n ${namespace} --grace-period=10`,
        () => client.deletePod(target, namespace, 10)
      );
      break;
    }

    case "share-group-rebalance": {
      let next = 2;
      await exec(
        "Read current share-group-consumer replicas",
        `oc get deploy/share-group-consumer -n ${namespace}`,
        async () => {
          const depRaw = await client.getDeployment("share-group-consumer", namespace);
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const dep = (depRaw && typeof depRaw === 'object' && 'body' in depRaw) ? (depRaw as any).body : depRaw as any;
          next = (dep?.spec?.replicas ?? 1) + 1;
        }
      );
      await exec(
        `Scale share-group-consumer to ${next} replicas (KIP-932 rebalance)`,
        `oc scale deploy/share-group-consumer -n ${namespace} --replicas=${next}`,
        () => client.scaleDeployment("share-group-consumer", namespace, next)
      );
      break;
    }

    case "partition-imbalance": {
      // Reuse the cooperative-consumer scaling path — this is the benign
      // KIP-848 rebalance scenario. Renamed in the UI but kept under this
      // string for backward compat with the simulator.
      let next = 3;
      await exec(
        "Read current cooperative-consumer replicas",
        `oc get deploy/cooperative-consumer -n ${namespace}`,
        async () => {
          const depRaw = await client.getDeployment("cooperative-consumer", namespace);
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const dep = (depRaw && typeof depRaw === "object" && "body" in depRaw) ? (depRaw as any).body : depRaw as any;
          next = (dep?.spec?.replicas ?? 2) + 1;
        }
      );
      await exec(
        `Scale cooperative-consumer to ${next} replicas (benign KIP-848 rebalance)`,
        `oc scale deploy/cooperative-consumer -n ${namespace} --replicas=${next}`,
        () => client.scaleDeployment("cooperative-consumer", namespace, next)
      );
      break;
    }

    case "reset":
      await exec(
        "Restore slow-consumer DELAY_MS=0",
        `oc set env deploy/slow-consumer -n ${namespace} DELAY_MS=0`,
        () => client.setDeploymentEnv("slow-consumer", namespace, "consumer", "DELAY_MS", "0")
      );
      await exec(
        "Reset fast-producer replicas to 1",
        `oc scale deploy/fast-producer -n ${namespace} --replicas=1`,
        () => client.scaleDeployment("fast-producer", namespace, 1)
      );
      await exec(
        "Reset slow-consumer replicas to 1",
        `oc scale deploy/slow-consumer -n ${namespace} --replicas=1`,
        () => client.scaleDeployment("slow-consumer", namespace, 1)
      );
      await exec(
        "Reset cooperative-consumer replicas to 2",
        `oc scale deploy/cooperative-consumer -n ${namespace} --replicas=2`,
        () => client.scaleDeployment("cooperative-consumer", namespace, 2)
      );
      await exec(
        "Reset share-group-consumer replicas to 1",
        `oc scale deploy/share-group-consumer -n ${namespace} --replicas=1`,
        () => client.scaleDeployment("share-group-consumer", namespace, 1)
      );
      break;
  }

  return {
    kind,
    ranAt: new Date().toISOString(),
    steps,
    ok: steps.every((s) => s.status === "ok"),
  };
}

function errorMessage(e: unknown): string {
  if (e == null) return "unknown";
  if (typeof e === "string") return e;
  if (typeof e === "object" && "message" in e) {
    const m = (e as { message?: unknown }).message;
    if (typeof m === "string") return m;
  }
  return JSON.stringify(e);
}
