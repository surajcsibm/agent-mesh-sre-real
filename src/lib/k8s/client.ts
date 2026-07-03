/**
 * Server-only K8s client adapter.
 *
 * - Auto-detects environment: tries in-cluster ServiceAccount first
 *   (when the app runs as a Pod), falls back to local kubeconfig
 *   (when the app runs on the laptop).
 * - Exposes high-level helpers the demo needs: server-side apply,
 *   wait-for-condition, scale, delete pod, set env, read secret.
 *
 * ⚠ This module imports `@kubernetes/client-node` which only works
 * in a Node.js runtime. Never import it from a Client Component.
 */
import "server-only";
import * as k8s from "@kubernetes/client-node";
import { parseAllDocuments } from "yaml";

export type ApplyResult = {
  group: string;
  version: string;
  kind: string;
  namespace?: string;
  name: string;
  action: "created" | "updated" | "unchanged" | "skipped" | "error";
  message?: string;
};

export type ConnectionInfo = {
  connected: boolean;
  context?: string;
  user?: string;
  serverUrl?: string;
  inCluster: boolean;
  error?: string;
};

export type ConditionStatus = {
  type: string;
  status: "True" | "False" | "Unknown";
  reason?: string;
  message?: string;
  lastTransitionTime?: string;
};

const STRIMZI_GROUP = "kafka.strimzi.io";
const STRIMZI_VERSION = "v1";

export class K8sClient {
  readonly kc: k8s.KubeConfig;
  readonly inCluster: boolean;
  private readonly core: k8s.CoreV1Api;
  private readonly apps: k8s.AppsV1Api;
  private readonly custom: k8s.CustomObjectsApi;
  private readonly objectApi: k8s.KubernetesObjectApi;

  private constructor(kc: k8s.KubeConfig, inCluster: boolean) {
    this.kc = kc;
    this.inCluster = inCluster;
    this.core = kc.makeApiClient(k8s.CoreV1Api);
    this.apps = kc.makeApiClient(k8s.AppsV1Api);
    this.custom = kc.makeApiClient(k8s.CustomObjectsApi);
    this.objectApi = k8s.KubernetesObjectApi.makeApiClient(kc);
  }

  /**
   * Try in-cluster auth, then a base64-encoded kubeconfig from
   * KUBECONFIG_BASE64 (used on Vercel, which has no local disk kubeconfig
   * and isn't running as a pod inside the target cluster), then finally
   * fall back to the local ~/.kube/config / KUBECONFIG env for local dev.
   * Throws if none of the three work.
   */
  static async detect(): Promise<K8sClient> {
    const kc = new k8s.KubeConfig();
    let inCluster = false;
    try {
      // ServiceAccount mounted at /var/run/secrets/kubernetes.io/serviceaccount
      kc.loadFromCluster();
      inCluster = true;
    } catch {
      if (process.env.KUBECONFIG_BASE64) {
        // Serverless (Vercel) path — scoped ServiceAccount kubeconfig,
        // base64-encoded so it can live safely as a single-line env var.
        const decoded = Buffer.from(process.env.KUBECONFIG_BASE64, "base64").toString("utf-8");
        kc.loadFromString(decoded);
      } else {
        // Local dev path — ~/.kube/config or KUBECONFIG env
        kc.loadFromDefault();
      }
    }
    if (!kc.getCurrentContext()) {
      throw new Error("No active kubeconfig context found");
    }
    return new K8sClient(kc, inCluster);
  }

  /** Ping the API server to verify connectivity. Never throws. */
  async ping(): Promise<ConnectionInfo> {
    const ctx = this.kc.getCurrentContext();
    const cluster = this.kc.getCurrentCluster();
    const user = this.kc.getCurrentUser();
    try {
      // Hit /version which every k8s API server exposes & is unauthenticated-safe
      await this.core.listNamespace();
      return {
        connected: true,
        context: ctx,
        user: user?.name,
        serverUrl: cluster?.server,
        inCluster: this.inCluster,
      };
    } catch (e: unknown) {
      return {
        connected: false,
        context: ctx,
        user: user?.name,
        serverUrl: cluster?.server,
        inCluster: this.inCluster,
        error: errMessage(e),
      };
    }
  }

  /** Returns whether Strimzi CRDs are present in the cluster. */
  async hasStrimziCrds(): Promise<{ present: boolean; version?: string }> {
    try {
      const ext = this.kc.makeApiClient(k8s.ApiextensionsV1Api);
      const crdsRes = await ext.listCustomResourceDefinition();
      const kafkaCrd = crdsRes.body.items.find((c) => c.metadata?.name === "kafkas.kafka.strimzi.io");
      if (!kafkaCrd) return { present: false };
      const version =
        kafkaCrd.metadata?.labels?.["app.kubernetes.io/version"] ??
        kafkaCrd.spec.versions[0]?.name;
      return { present: true, version };
    } catch (e) {
      throw new Error(`Failed to list CRDs: ${errMessage(e)}`);
    }
  }

  /** Idempotent server-side apply of every YAML document in the input. */
  async applyYaml(yamlText: string, fieldManager = "agent-mesh-sre"): Promise<ApplyResult[]> {
    const docs = parseAllDocuments(yamlText)
      .map((d) => d.toJS())
      .filter((d) => d && typeof d === "object" && d.kind);

    const results: ApplyResult[] = [];
    for (const doc of docs) {
      const r = await this.applyOne(doc as k8s.KubernetesObject, fieldManager);
      results.push(r);
    }
    return results;
  }

  private async applyOne(
    obj: k8s.KubernetesObject,
    fieldManager: string
  ): Promise<ApplyResult> {
    const kind = obj.kind ?? "Unknown";
    const name = obj.metadata?.name ?? "?";
    const namespace = obj.metadata?.namespace;
    const apiVersion = obj.apiVersion ?? "";
    const [group, version] = apiVersion.includes("/")
      ? apiVersion.split("/", 2)
      : ["", apiVersion];

    try {
      // Try a server-side apply. If the resource exists, it is patched; if
      // not, it is created. This is the canonical idempotent path.
      const existing = await this.tryRead(obj);
      if (!existing) {
        await this.objectApi.create(obj);
        return { group, version, kind, namespace, name, action: "created" };
      }
      // Server-side apply patch.
      await this.objectApi.patch(
        obj,
        undefined,
        undefined,
        fieldManager,
        true
      );
      return { group, version, kind, namespace, name, action: "updated" };
    } catch (e: unknown) {
      return {
        group,
        version,
        kind,
        namespace,
        name,
        action: "error",
        message: errMessage(e),
      };
    }
  }

  private async tryRead(obj: k8s.KubernetesObject): Promise<k8s.KubernetesObject | null> {
    if (!obj.metadata?.name) {
      throw new Error(`Cannot read object without metadata.name (kind=${obj.kind})`);
    }
    const header = {
      apiVersion: obj.apiVersion,
      kind: obj.kind,
      metadata: { name: obj.metadata.name, namespace: obj.metadata.namespace ?? "default" },
    };
    try {
      const r = await this.objectApi.read(header);
      return r as k8s.KubernetesObject;
    } catch (e: unknown) {
      const code = errStatusCode(e);
      if (code === 404) return null;
      throw e;
    }
  }

  // ---------------- Strimzi helpers ----------------

  async getKafka(name: string, namespace: string): Promise<KafkaResource | null> {
    return this.getStrimzi("kafkas", name, namespace);
  }

  async getKafkaTopic(name: string, namespace: string) {
    return this.getStrimzi("kafkatopics", name, namespace);
  }

  async getKafkaUser(name: string, namespace: string) {
    return this.getStrimzi("kafkausers", name, namespace);
  }

  async listKafkaTopics(namespace: string): Promise<KafkaTopicResource[]> {
    return this.listStrimzi("kafkatopics", namespace);
  }

  async listKafkaUsers(namespace: string): Promise<KafkaUserResource[]> {
    return this.listStrimzi("kafkausers", namespace);
  }

  async listKafkaNodePools(namespace: string): Promise<KafkaNodePoolResource[]> {
    return this.listStrimzi("kafkanodepools", namespace);
  }

  private async getStrimzi<T>(
    plural: string,
    name: string,
    namespace: string
  ): Promise<T | null> {
    try {
      const r = await this.custom.getNamespacedCustomObject(
        STRIMZI_GROUP, STRIMZI_VERSION, namespace, plural, name
      );
      return r as T;
    } catch (e: unknown) {
      if (errStatusCode(e) === 404) return null;
      throw e;
    }
  }

  private async listStrimzi<T>(plural: string, namespace: string): Promise<T[]> {
    const r = await this.custom.listNamespacedCustomObject(
      STRIMZI_GROUP, STRIMZI_VERSION, namespace, plural
    );
    const list = r as { items?: T[] };
    return list.items ?? [];
  }

  // ---------------- Pods / Deployments / Secrets ----------------

  async getDeployment(name: string, namespace: string) {
    try {
      return await this.apps.readNamespacedDeployment(name, namespace);
    } catch (e: unknown) {
      if (errStatusCode(e) === 404) return null;
      throw e;
    }
  }

  async scaleDeployment(name: string, namespace: string, replicas: number): Promise<void> {
    await this.apps.patchNamespacedDeploymentScale(
      name, namespace, { spec: { replicas } }
    );
  }

  async setDeploymentEnv(
    name: string,
    namespace: string,
    container: string,
    key: string,
    value: string
  ): Promise<void> {
    const depRaw = await this.getDeployment(name, namespace);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dep = (depRaw && typeof depRaw === 'object' && 'body' in depRaw ? (depRaw as {body: unknown}).body : depRaw) as any;
    if (!dep) throw new Error(`Deployment ${namespace}/${name} not found`);
    const containers = dep.spec?.template?.spec?.containers ?? [];
    const target = containers.find((c: {name: string}) => c.name === container) ?? containers[0];
    if (!target) throw new Error(`No container in ${namespace}/${name}`);
    const env = target.env ?? [];
    const existing = env.find((e: {name: string; value?: string}) => e.name === key);
    if (existing) existing.value = value;
    else env.push({ name: key, value });
    target.env = env;
    await this.apps.replaceNamespacedDeployment(name, namespace, dep);
  }

  async deletePod(name: string, namespace: string, gracePeriodSeconds = 10): Promise<void> {
    await this.core.deleteNamespacedPod(name, namespace, undefined, undefined, gracePeriodSeconds);
  }

  async listPods(namespace: string, labelSelector?: string) {
    return this.core.listNamespacedPod(namespace, undefined, undefined, undefined, labelSelector);
  }

  async getSecretData(name: string, namespace: string): Promise<Record<string, Buffer> | null> {
    try {
      const s = await this.core.readNamespacedSecret(name, namespace);
      const secret = (s && typeof s === 'object' && 'body' in s) ? (s as {body: {data?: Record<string, string>}}).body : s as {data?: Record<string, string>};
      const data = secret.data ?? {};
      const out: Record<string, Buffer> = {};
      for (const [k, v] of Object.entries(data)) {
        out[k] = Buffer.from(v, "base64");
      }
      return out;
    } catch (e) {
      if (errStatusCode(e) === 404) return null;
      throw e;
    }
  }

  async getNamespace(name: string) {
    try {
      return await this.core.readNamespace(name);
    } catch (e) {
      if (errStatusCode(e) === 404) return null;
      throw e;
    }
  }
}

// ---------------- Strimzi types (lightweight, only what we read) ----------------

export type KafkaCondition = ConditionStatus;

export type KafkaListenerStatus = {
  name: string;
  type?: string;
  bootstrapServers?: string;
  certificates?: string[];
};

export type KafkaResource = {
  apiVersion?: string;
  kind?: string;
  metadata?: { name?: string; namespace?: string; uid?: string; generation?: number };
  spec?: {
    kafka?: {
      version?: string;
      metadataVersion?: string;
      listeners?: { name: string; port: number; type: string; tls?: boolean }[];
      replicas?: number;
    };
  };
  status?: {
    conditions?: KafkaCondition[];
    listeners?: KafkaListenerStatus[];
    kafkaVersion?: string;
    kafkaMetadataVersion?: string;
    clusterId?: string;
    operatorLastSuccessfulVersion?: string;
    observedGeneration?: number;
  };
};

export type KafkaTopicResource = {
  apiVersion?: string;
  kind?: string;
  metadata?: { name?: string; namespace?: string };
  spec?: { partitions?: number; replicas?: number; config?: Record<string, unknown> };
  status?: { conditions?: KafkaCondition[]; observedGeneration?: number; topicName?: string };
};

export type KafkaUserResource = {
  apiVersion?: string;
  kind?: string;
  metadata?: { name?: string; namespace?: string };
  spec?: { authentication?: { type?: string }; authorization?: { type?: string } };
  status?: { conditions?: KafkaCondition[]; secret?: string; username?: string };
};

export type KafkaNodePoolResource = {
  apiVersion?: string;
  kind?: string;
  metadata?: { name?: string; namespace?: string };
  spec?: { replicas?: number; roles?: string[] };
  status?: {
    conditions?: KafkaCondition[];
    nodeIds?: number[];
    replicas?: number;
    roles?: string[];
  };
};

// ---------------- helpers ----------------

function errMessage(e: unknown): string {
  if (!e) return "unknown";
  if (typeof e === "string") return e;
  if (typeof e === "object" && e !== null) {
    const ee = e as { body?: { message?: string }; message?: string };
    return ee.body?.message ?? ee.message ?? JSON.stringify(e);
  }
  return String(e);
}

function errStatusCode(e: unknown): number | undefined {
  if (typeof e !== "object" || e === null) return undefined;
  const ee = e as {
    code?: number;
    statusCode?: number;
    response?: { statusCode?: number };
    body?: { code?: number };
  };
  return ee.code ?? ee.statusCode ?? ee.response?.statusCode ?? ee.body?.code;
}

export function readyCondition(conditions?: KafkaCondition[] | null): KafkaCondition | undefined {
  return conditions?.find((c) => c.type === "Ready");
}

export function isReady(conditions?: KafkaCondition[] | null): boolean {
  return readyCondition(conditions)?.status === "True";
}
