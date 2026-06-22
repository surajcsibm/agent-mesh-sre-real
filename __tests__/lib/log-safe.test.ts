import { describe, it, expect } from "vitest";
import { redact, safeErr } from "../../src/lib/log-safe";

describe("log-safe / redact()", () => {
  it("redacts top-level keys that look like secrets", () => {
    const input = {
      bootstrapInternal: "kafka.confluent.svc.cluster.local:9092",
      username: "ops-engineer",
      password: "SuperSecret123",
      saslMechanism: "plain",
    };
    const out = redact(input);
    expect(out.password).toBe("[redacted]");
    expect(out.bootstrapInternal).toBe(input.bootstrapInternal); // not a secret, untouched
    expect(out.username).toBe(input.username); // not a secret, untouched
  });

  it("redacts nested credential fields at any depth", () => {
    const input = {
      config: {
        sasl: { mechanism: "scram-sha-512", username: "u", password: "p" },
      },
    };
    const out = redact(input) as typeof input;
    expect(out.config.sasl.password).toBe("[redacted]");
    expect(out.config.sasl.username).toBe("u");
  });

  it("redacts long PEM-looking strings even on non-obvious key names", () => {
    const pem = "-----BEGIN CERTIFICATE-----\n" + "x".repeat(300) + "\n-----END CERTIFICATE-----";
    const out = redact({ ca: pem }) as { ca: string };
    expect(out.ca).toBe("[redacted]");
  });

  it("does not throw on circular references", () => {
    const obj: Record<string, unknown> = { password: "secret" };
    obj.self = obj;
    expect(() => redact(obj)).not.toThrow();
  });

  it("leaves primitives and null untouched", () => {
    expect(redact(42)).toBe(42);
    expect(redact("password")).toBe("password"); // a bare string is not a keyed object
    expect(redact(null)).toBe(null);
  });
});

describe("log-safe / safeErr()", () => {
  it("redacts credential-shaped substrings embedded in an Error message", () => {
    const err = new Error("SASL PLAIN authentication failed: password=hunter2 username=ops");
    const out = safeErr(err);
    expect(out.message).not.toContain("hunter2");
    expect(out.message).toContain("[redacted]");
  });

  it("redacts PEM blocks embedded in a stack/message", () => {
    const pem = "-----BEGIN PRIVATE KEY-----\nMIIB...\n-----END PRIVATE KEY-----";
    const err = new Error(`TLS handshake failed with cert ${pem}`);
    const out = safeErr(err);
    expect(out.message).not.toContain("MIIB");
  });

  it("handles non-Error thrown values without crashing", () => {
    expect(() => safeErr("plain string error")).not.toThrow();
    expect(() => safeErr({ statusCode: 500, password: "x" })).not.toThrow();
    expect(() => safeErr(undefined)).not.toThrow();
  });

  it("redacts object-shaped throws (e.g. kafkajs connection errors)", () => {
    const fakeKafkaError = {
      name: "KafkaJSConnectionError",
      message: "Connection failed",
      broker: "212.2.248.241:9092",
      sasl: { username: "ops-engineer", password: "do-not-leak" },
    };
    const out = safeErr(fakeKafkaError);
    expect(out.message).not.toContain("do-not-leak");
  });
});
