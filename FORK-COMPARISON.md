# PR Addendum: surajcsibm/agent-mesh-sre vs aswinayyolath PR #1

**Type:** Addendum / fork comparison  
**Reference PR:** https://github.com/aswinayyolath/agent-mesh-sre/pull/1  
**Fork:** https://github.com/surajcsibm/agent-mesh-sre  
**Date:** May 2026

---

## Summary

This document describes the architectural and feature differences between the upstream `aswinayyolath/agent-mesh-sre` Pull Request #1 and the current `surajcsibm/agent-mesh-sre` fork. The fork represents a **complete UI and simulation rewrite** on top of the same conceptual MRAL / multi-agent foundation, targeting a zero-infrastructure demo experience deployable on Vercel with no Kafka cluster required.

---

## Architectural Divergence

### Upstream PR #1 (aswinayyolath)

The upstream PR introduces a server-side event-streaming architecture:

- **`src/lib/mesh.ts`** — Server-side agent orchestration. Runs MRAL loop in a Node.js process. Connects to a real (or mocked) Kafka broker via `kafkajs`.
- **Zustand stores** — Client-side global state (`useAgentStore`, `useAuditStore`, `useTopicStore`). Components subscribe to slices.
- **SSE stream** — `/api/mesh/stream` endpoint pushes `text/event-stream` events to the client. Client connects via `EventSource`.
- **`src/components/ScenarioPanel.tsx`** — Dedicated scenario-picker component (separate file).
- **`src/components/SetupWizard.tsx`** — Infrastructure setup wizard for configuring Kafka, Redis, and SMTP connections.
- **`src/components/VisualBuilder.tsx`** — Drag-and-drop agent topology builder.
- **React Flow (rectangular nodes)** — Agent nodes rendered as rectangles with status badges.

### Fork (surajcsibm)

The fork replaces the server-side infrastructure with a pure client-side simulation:

- **`src/components/client-sim.ts`** — Deterministic async simulation of all 10 scenarios. No Kafka connection. Runs entirely in the browser.
- **`src/components/useMeshStream.ts`** — Custom React hook with `useReducer`. Owns all state. Calls `client-sim.ts` scenario functions directly.
- **No Zustand** — Eliminated entirely. All state flows through a single `useReducer`.
- **No SSE** — Eliminated. Events are dispatched directly to the reducer via `setTimeout`-based async chains.
- **`src/components/Dashboard.tsx`** — Single monolithic client component (~900 lines) that replaces `ScenarioPanel`, `SetupWizard`, `VisualBuilder`, and the previous dashboard layout.
- **`src/components/AgentCanvas.tsx`** — Complete rewrite using React Flow v12 (`@xyflow/react`). Circular nodes. Sub-agent ephemeral circles.

---

## New Features in Fork (not in upstream PR #1)

### 1. Client-side simulation engine

All 10 scenarios run without any server infrastructure. A deterministic `async` function chain in `client-sim.ts` dispatches events to the reducer on a realistic timeline. This makes the demo instantly runnable on Vercel.

### 2. Circular agent nodes with sub-agent circles

Agent nodes are rendered as styled circular divs (200 px diameter) using React Flow custom node types. During MRAL phases, three ephemeral sub-agent circles (90 px, dashed border) appear below the MONITOR node:
- **REASON** (violet) — during reasoning and human-approval wait
- **ACT** (orange) — during action execution
- **LEARN** (green) — during lesson recording

### 3. Distinct agent colour scheme

| Agent | Colour | Notes |
|-------|--------|-------|
| INTAKE | Emerald `#1D9E75` | Distinct from WRITER |
| MONITOR | Violet `#7c3aed` | Primary reasoning agent |
| WRITER | Cyan `#0891b2` | Different from INTAKE's green |
| NOTIFY | Orange `#f97316` | Escalation / alerting |

### 4. Arctic Clean theme

Full visual redesign: navy navigation (`#1e3a5f`), light blue-grey page background (`#f0f4f8`), emerald accent, white surface cards. No dark mode. Consistent across all panels.

### 5. Topics Management Panel

Right sidebar Topics tab with:
- Health badges (Healthy / Degraded / Critical)
- Heal Topic — triggers dedicated MRAL cycle for one topic
- Create Topic — adds new simulated topic (duplicate-name check)
- Copy Topic — clones with `-copy` suffix
- **Auto-create on scenario run** — topics relevant to a scenario are automatically added/updated in the list when that scenario starts
- Latest 20 shown; More button loads next 20 (paginated, scrollable)

### 6. Persistent Scenario History (localStorage)

Every completed run is saved to `localStorage`. The history bar at the bottom of the dashboard persists across page refreshes. Each entry is clickable and opens a Summary Modal with the full live events table. Hydration-safe pattern (init with `[]`, load in `useEffect`) prevents SSR mismatch.

### 7. Live Events in Summaries and Emails

The `EmailSummaryData` type includes a `liveEvents` array. This is hydrated from the audit log at scenario completion and is included in both the in-app Summary Modal and the HTML email sent by NOTIFY. Every run's event feed is fully captured and reviewable.

### 8. Canvas Resize Controls

`+` and `−` buttons on the canvas panel adjust canvas height in 80 px increments (380–960 px range). Useful on smaller screens or when more vertical space is needed.

### 9. Kill / Restart Agent Controls

Kill and Restart buttons are embedded in each agent circle. Fixed React Flow click-propagation issue (`e.stopPropagation()` + `pointerEvents: "all"`) so buttons are reliably clickable inside circular nodes.

### 10. Live Activity Banner

A slide-in banner below the navigation shows which agent is active, what it is doing, and a detail snippet. Disappears when all agents return to idle.

### 11. Google OAuth + Email/Password Auth

NextAuth.js v4 with both Google OAuth and Credentials providers. Forgot-password email flow included. Session protected by JWT. Middleware guards `/dashboard` route.

### 12. Vercel Deployment

`next.config.mjs` has `ignoreBuildErrors: true` to allow deployment despite third-party type errors. Environment variable template (`.env.local.example`) provided. Auto-deploys on `git push origin main`.

### 13. Ten Scenarios (vs fewer in upstream)

Four pinned scenarios always visible + six extra in a scrollable list:

**Pinned:** Consumer Lag Spike (KIP-848), KRaft Controller Failover, Share Group Rebalance (KIP-932), False-Positive Suppression (KIP-848)

**Extra:** Schema Registry Mismatch (Avro), Broker Disk Saturation (I/O), Under-Replicated Partitions (ISR), Producer Timeout Storm (Batch), Consumer Session Timeout (GC), Log Compaction Lag (Compact)

---

## Files Added in Fork (not in upstream PR #1)

| File | Description |
|------|-------------|
| `src/components/client-sim.ts` | Simulation engine (replaces mesh.ts) |
| `src/components/useMeshStream.ts` | useReducer hook (replaces Zustand) |
| `src/components/Dashboard.tsx` | Full dashboard rewrite |
| `src/components/AgentCanvas.tsx` | React Flow canvas rewrite |
| `src/app/api/send-email/route.ts` | SMTP email dispatch |
| `src/app/api/auth/[...nextauth]/route.ts` | NextAuth handler |
| `src/app/login/page.tsx` | Login / register UI |
| `src/middleware.ts` | Auth middleware |
| `.env.local.example` | Env var template |
| `asyncapi.yaml` | AsyncAPI spec (unchanged from upstream) |

## Files Removed vs Upstream PR #1

| File | Reason |
|------|--------|
| `src/lib/mesh.ts` | Replaced by `client-sim.ts` |
| `src/store/*.ts` | No Zustand |
| `src/lib/sse-client.ts` | No SSE |
| `src/components/ScenarioPanel.tsx` | Merged into Dashboard.tsx |
| `src/components/SetupWizard.tsx` | No infrastructure to configure |
| `src/components/VisualBuilder.tsx` | Out of scope |

---

## Backward Compatibility

The `/api/mesh/stream`, `/api/mesh/approve`, and `/api/mesh/reject` API routes are retained as no-ops (return 200 OK) for forward compatibility with a future real-Kafka integration. Swapping `client-sim.ts` events for SSE stream events would restore real-Kafka operation without changes to the reducer or UI.

---

## Suggested Merge Strategy

This fork is best treated as a **demo-first variant** of the upstream project rather than a direct merge candidate. It trades infrastructure fidelity for zero-setup deployability. A staged merge could:

1. Adopt the circular node design and sub-agent circles from this fork into the upstream canvas
2. Keep the upstream SSE/Zustand architecture for real-Kafka mode
3. Add `MOCK_MODE=true` env switch that routes events to `client-sim.ts` instead of the SSE stream
4. Port the Topics Management panel (auto-create, paginated, Heal Topic) into upstream
5. Port localStorage scenario history persistence into upstream
