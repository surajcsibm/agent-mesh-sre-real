import { defineConfig } from "vitest/config";
import path from "path";

/**
 * Minimal Vitest setup — runs server-only lib code under Node, mapping the
 * "@/" alias the same way tsconfig.json does. Scope intentionally stays
 * small: this is a smoke-test suite (the 4 gated scenarios + the approval
 * decision flow + the redaction utility), not a full coverage harness.
 * See docs/Agent-Mesh-SRE-Remediation-Tracker.docx for the broader testing
 * roadmap (P2-6/7/8).
 */
export default defineConfig({
  test: {
    environment: "node",
    include: ["__tests__/**/*.test.ts"],
    globals: true,
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
