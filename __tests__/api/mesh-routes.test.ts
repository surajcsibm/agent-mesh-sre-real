import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/lib/emailer", () => ({
  sendAgentSummary: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("../../src/lib/kafka", async () => {
  const actual = await vi.importActual<typeof import("../../src/lib/kafka")>("../../src/lib/kafka");
  return { ...actual, kafkaProduce: vi.fn(), kafkaProduceAudit: vi.fn(), kafkaProduceLesson: vi.fn() };
});

import { POST as scenarioPOST } from "../../src/app/api/mesh/scenario/route";
import { POST as approvePOST } from "../../src/app/api/mesh/approve/route";
import { resetMesh } from "../../src/lib/mesh";

function jsonRequest(body: unknown): Request {
  return new Request("http://localhost/api/mesh/scenario", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function malformedRequest(): Request {
  return new Request("http://localhost/api/mesh/scenario", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{not valid json",
  });
}

describe("POST /api/mesh/scenario — input validation (SHOULD-2)", () => {
  beforeEach(() => resetMesh());

  it("accepts a valid, known scenario id", async () => {
    const res = await scenarioPOST(jsonRequest({ id: "lag-spike" }));
    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.ok).toBe(true);
  });

  it("rejects an unknown scenario id with 400, not a 500 crash", async () => {
    const res = await scenarioPOST(jsonRequest({ id: "totally-made-up-scenario" }));
    expect(res.status).toBe(400);
    const data = await res.json();
    expect(data.error).toBeDefined();
  });

  it("rejects a payload missing the id field", async () => {
    const res = await scenarioPOST(jsonRequest({}));
    expect(res.status).toBe(400);
  });

  it("rejects malformed JSON without throwing an unhandled exception", async () => {
    const res = await scenarioPOST(malformedRequest());
    expect(res.status).toBe(400);
  });

  it("rejects an id of the wrong type (number instead of string)", async () => {
    const res = await scenarioPOST(jsonRequest({ id: 12345 }));
    expect(res.status).toBe(400);
  });
});

describe("POST /api/mesh/approve — input validation (SHOULD-2)", () => {
  beforeEach(() => resetMesh());

  it("returns 400 for a missing decision field", async () => {
    const res = await approvePOST(jsonRequest({ id: "some-id" }));
    expect(res.status).toBe(400);
  });

  it("returns 400 for an invalid decision value", async () => {
    const res = await approvePOST(jsonRequest({ id: "some-id", decision: "maybe" }));
    expect(res.status).toBe(400);
  });

  it("returns 404 (not 500) for a well-formed request against a nonexistent approval", async () => {
    const res = await approvePOST(jsonRequest({ id: "nonexistent", decision: "approve" }));
    expect(res.status).toBe(404);
  });

  it("accepts an optional actor field within length bounds", async () => {
    const res = await approvePOST(
      jsonRequest({ id: "nonexistent", decision: "approve", actor: "test-operator" })
    );
    // Still 404 (approval doesn't exist) — the point is actor passed validation, not a 400.
    expect(res.status).toBe(404);
  });

  it("rejects malformed JSON without throwing an unhandled exception", async () => {
    const res = await approvePOST(malformedRequest());
    expect(res.status).toBe(400);
  });
});
