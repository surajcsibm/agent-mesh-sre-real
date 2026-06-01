#!/usr/bin/env node
// test-kafka.js — Quick connectivity check against the cluster in .env.local
// Run from the project root: node deploy/scripts/test-kafka.js
//
// What it does:
//   1. Reads KAFKA_* vars from .env.local
//   2. Connects with the configured SASL mechanism + optional CA cert
//   3. Lists topic metadata (or creates a test topic) and disconnects
//   Exits 0 on success, 1 on failure.

const fs   = require("fs");
const path = require("path");

// ── Load .env.local ────────────────────────────────────────────────────────
function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    console.error(`ERROR: ${filePath} not found`);
    process.exit(1);
  }
  const lines = fs.readFileSync(filePath, "utf8").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const idx = trimmed.indexOf("=");
    if (idx === -1) continue;
    const key = trimmed.slice(0, idx).trim();
    const val = trimmed.slice(idx + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}

const projectRoot = path.resolve(__dirname, "../..");
loadEnvFile(path.join(projectRoot, ".env.local"));

// ── Validate required vars ─────────────────────────────────────────────────
const MODE      = process.env.KAFKA_MODE ?? "mock";
const BOOTSTRAP = process.env.KAFKA_BOOTSTRAP;
const USERNAME  = process.env.KAFKA_USERNAME;
const PASSWORD  = process.env.KAFKA_PASSWORD;
const MECHANISM = process.env.KAFKA_SASL_MECHANISM ?? "scram-sha-256";
const CA_B64    = process.env.KAFKA_CA_CERT_BASE64;

if (MODE !== "real") {
  console.log(`⚠  KAFKA_MODE=${MODE} — set it to "real" in .env.local to test a live cluster`);
  process.exit(0);
}
if (!BOOTSTRAP || !USERNAME || !PASSWORD) {
  console.error("ERROR: KAFKA_BOOTSTRAP, KAFKA_USERNAME, and KAFKA_PASSWORD must all be set");
  process.exit(1);
}

// ── Build KafkaJS config ───────────────────────────────────────────────────
let ssl;
if (CA_B64) {
  const caPem = Buffer.from(CA_B64, "base64").toString("utf8");
  ssl = { ca: [caPem], rejectUnauthorized: true };
} else {
  ssl = true; // trust public CA (Upstash / RedPanda / Confluent)
}

const sasl =
  MECHANISM === "plain"
    ? { mechanism: "plain",        username: USERNAME, password: PASSWORD }
    : MECHANISM === "scram-sha-256"
    ? { mechanism: "scram-sha-256", username: USERNAME, password: PASSWORD }
    : { mechanism: "scram-sha-512", username: USERNAME, password: PASSWORD };

// ── Run test ───────────────────────────────────────────────────────────────
const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: "kafka-connectivity-test",
  brokers:  [BOOTSTRAP],
  ssl,
  sasl,
  logLevel: logLevel.ERROR,
  connectionTimeout: 10_000,
  requestTimeout:    20_000,
});

(async () => {
  const admin = kafka.admin();
  console.log(`\n🔍  Testing Kafka connectivity…`);
  console.log(`    Bootstrap : ${BOOTSTRAP}`);
  console.log(`    Username  : ${USERNAME}`);
  console.log(`    Mechanism : ${MECHANISM}`);
  console.log(`    CA cert   : ${CA_B64 ? "custom (Aiven)" : "public CA"}\n`);

  try {
    await admin.connect();
    console.log("✅  Connected!\n");

    const topics = await admin.listTopics();
    if (topics.length === 0) {
      console.log("   No topics yet (auto-create is on — they'll appear on first produce)");
    } else {
      console.log(`   Topics (${topics.length}):`);
      topics.sort().forEach(t => console.log(`     • ${t}`));
    }

    await admin.disconnect();
    console.log("\n✅  Kafka cluster is reachable and accepting connections.\n");
    process.exit(0);
  } catch (err) {
    console.error("❌  Connection failed:", err.message);
    if (err.message?.includes("SASL")) {
      console.error("    → Check KAFKA_USERNAME / KAFKA_PASSWORD in .env.local");
    } else if (err.message?.includes("certificate") || err.message?.includes("SSL")) {
      console.error("    → CA cert issue — re-run configure-aiven.sh to re-encode ca.pem");
    } else if (err.message?.includes("ECONNREFUSED") || err.message?.includes("ETIMEDOUT")) {
      console.error("    → Bootstrap unreachable — check KAFKA_BOOTSTRAP and Aiven firewall");
    }
    process.exit(1);
  }
})();
