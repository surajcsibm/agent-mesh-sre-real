/**
 * Runtime mode singleton (server-side only).
 *
 * Drives whether the app produces synthetic events through the in-memory
 * `BrokerSim` (`mock`) or against a real Kafka cluster managed by Strimzi
 * on OpenShift (`real`). Defaults are derived from environment but can be
 * promoted to `real` at runtime once the Setup Wizard succeeds.
 */
import "server-only";

export type AppMode = "mock" | "real";

export type RuntimeConfig = {
  mode: AppMode;
  kubeAvailable: boolean;
  /** Most recent successful k8s/strimzi connection, ISO timestamp. */
  lastVerifiedAt?: string;
  cluster: {
    namespace: string;
    name: string;
  };
  kafka?: {
    bootstrapInternal?: string;
    bootstrapExternal?: string;
    username?: string;
    /** SCRAM password for the controller user. Server-only. */
    password?: string;
    /** PEM encoded CA cert for the cluster TLS listener. Server-only. */
    caCertPem?: string;
    /**
     * SASL mechanism to use when connecting.
     *   "scram-sha-512" — Strimzi on GKE/OpenShift (default)
     *   "scram-sha-256" — RedPanda Cloud Serverless
     *   "plain"         — Confluent Cloud
     */
    saslMechanism?: "scram-sha-512" | "scram-sha-256" | "plain";
  };
};

const DEFAULT_NS = process.env.KAFKA_NAMESPACE || "agent-mesh-sre";
const DEFAULT_CLUSTER = process.env.KAFKA_CLUSTER || "agent-mesh-kafka";
const DEFAULT_MODE: AppMode = (process.env.KAFKA_MODE === "real" ? "real" : "mock") as AppMode;

const g = globalThis as unknown as { __ams_runtime?: RuntimeConfig };

export function getRuntime(): RuntimeConfig {
  if (!g.__ams_runtime) {
    g.__ams_runtime = {
      mode: DEFAULT_MODE,
      kubeAvailable: false,
      cluster: { namespace: DEFAULT_NS, name: DEFAULT_CLUSTER },
      kafka:
        DEFAULT_MODE === "real"
          ? {
              bootstrapInternal: process.env.KAFKA_BOOTSTRAP,
              username: process.env.KAFKA_USERNAME,
              password: process.env.KAFKA_PASSWORD,
              caCertPem: process.env.KAFKA_CA_CERT_BASE64
                ? Buffer.from(process.env.KAFKA_CA_CERT_BASE64, "base64").toString("utf8")
                : undefined,
              saslMechanism: (process.env.KAFKA_SASL_MECHANISM ?? "scram-sha-512") as "scram-sha-512" | "scram-sha-256" | "plain",
            }
          : undefined,
    };
  }
  return g.__ams_runtime;
}

export function setMode(mode: AppMode): void {
  const r = getRuntime();
  r.mode = mode;
  r.lastVerifiedAt = new Date().toISOString();
}

export function setKubeAvailable(v: boolean): void {
  getRuntime().kubeAvailable = v;
}

export function setKafkaConnection(c: NonNullable<RuntimeConfig["kafka"]>): void {
  const r = getRuntime();
  r.kafka = c;
  r.lastVerifiedAt = new Date().toISOString();
}

/** Strip secrets before returning to the client. */
export function publicView(r: RuntimeConfig = getRuntime()) {
  return {
    mode: r.mode,
    kubeAvailable: r.kubeAvailable,
    lastVerifiedAt: r.lastVerifiedAt,
    cluster: r.cluster,
    kafka: r.kafka
      ? {
          bootstrapInternal: r.kafka.bootstrapInternal,
          bootstrapExternal: r.kafka.bootstrapExternal,
          username: r.kafka.username,
          hasPassword: !!r.kafka.password,
          hasCaCert: !!r.kafka.caCertPem,
        }
      : null,
  };
}
