"use client";

/**
 * Animated particle overlay rendered in screen coords on top of React Flow.
 * For each `particle` event from the SSE stream we draw a small dot that
 * travels along a straight line between the source agent's right handle and
 * the target agent's left handle. The line is curved by interpolating
 * a control point above the midpoint, matching the bezier edges visually.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { useReactFlow, useStore as useRFStore } from "@xyflow/react";
import { useMesh } from "@/lib/store";
import { AGENTS } from "@/lib/agents-config";
import type { AgentId, TopicName } from "@/lib/types";

interface PointXY {
  x: number;
  y: number;
}

const COLORS: Record<string, string> = {
  cyan: "#22d3ee",
  violet: "#a78bfa",
  emerald: "#34d399",
  amber: "#fbbf24",
  rose: "#fb7185",
  red: "#f87171",
};

/** Width-aware right-edge of the agent node and left-edge for entry. */
const NODE_W = 260;
const NODE_H = 168;

function nodeRightHandle(pos: { x: number; y: number }): PointXY {
  return { x: pos.x + NODE_W, y: pos.y + NODE_H / 2 };
}
function nodeLeftHandle(pos: { x: number; y: number }): PointXY {
  return { x: pos.x, y: pos.y + NODE_H / 2 };
}
function nodeTopHandleLeft(pos: { x: number; y: number }): PointXY {
  return { x: pos.x + NODE_W * 0.3, y: pos.y };
}
function nodeTopHandleRight(pos: { x: number; y: number }): PointXY {
  return { x: pos.x + NODE_W * 0.7, y: pos.y };
}

function quadBezier(t: number, a: PointXY, c: PointXY, b: PointXY): PointXY {
  const it = 1 - t;
  return {
    x: it * it * a.x + 2 * it * t * c.x + t * t * b.x,
    y: it * it * a.y + 2 * it * t * c.y + t * t * b.y,
  };
}

export function ParticleLayer() {
  const particles = useMesh((s) => s.particles);
  const rf = useReactFlow();
  const transform = useRFStore((s) => s.transform);
  const [tx, ty, zoom] = transform;

  // Force re-render at ~60fps while particles exist
  const [, force] = useState(0);
  const raf = useRef<number | null>(null);
  useEffect(() => {
    if (particles.length === 0) return;
    let stop = false;
    const loop = () => {
      if (stop) return;
      force((n) => (n + 1) & 0xffff);
      raf.current = requestAnimationFrame(loop);
    };
    raf.current = requestAnimationFrame(loop);
    return () => {
      stop = true;
      if (raf.current) cancelAnimationFrame(raf.current);
    };
  }, [particles.length]);

  const nodePositions = useMemo(() => {
    const out: Record<AgentId, { x: number; y: number }> = {} as never;
    for (const id of Object.keys(AGENTS) as AgentId[]) {
      const node = rf.getNode(id);
      out[id] = node?.position ?? AGENTS[id].position;
    }
    return out;
  }, [rf, particles.length]);

  const dots = particles
    .map((p) => {
      const startedAt = p.startedAt;
      const t = (Date.now() - startedAt) / p.durationMs;
      if (t < 0 || t > 1) return null;

      const isSelfLoop =
        p.source === "monitor-agent" && p.target === "monitor-agent" && p.topic === "ops.lessons.v1";

      let a: PointXY;
      let b: PointXY;
      let c: PointXY;
      if (isSelfLoop) {
        // top-left -> arc above -> top-right
        a = nodeTopHandleRight(nodePositions["monitor-agent"]);
        b = nodeTopHandleLeft(nodePositions["monitor-agent"]);
        c = { x: (a.x + b.x) / 2, y: a.y - 90 };
      } else {
        a = nodeRightHandle((nodePositions as Record<string, {x:number;y:number}>)[p.source]);
        b = nodeLeftHandle((nodePositions as Record<string, {x:number;y:number}>)[p.target]);
        const mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
        c = { x: mid.x, y: mid.y - 60 };
      }

      const q = quadBezier(t, a, c, b);
      const sx = q.x * zoom + tx;
      const sy = q.y * zoom + ty;
      return { p, x: sx, y: sy, t };
    })
    .filter(Boolean) as { p: (typeof particles)[number]; x: number; y: number; t: number }[];

  return (
    <div className="absolute inset-0 pointer-events-none overflow-hidden">
      <svg className="absolute inset-0 w-full h-full">
        <defs>
          {Object.entries(COLORS).map(([k, v]) => (
            <radialGradient key={k} id={`p-${k}`}>
              <stop offset="0%" stopColor={v} stopOpacity="1" />
              <stop offset="60%" stopColor={v} stopOpacity="0.6" />
              <stop offset="100%" stopColor={v} stopOpacity="0" />
            </radialGradient>
          ))}
        </defs>
        {dots.map(({ p, x, y, t }) => {
          const r = 7 * zoom * (0.8 + 0.4 * Math.sin(t * Math.PI));
          return (
            <g key={p.id}>
              <circle cx={x} cy={y} r={r * 2} fill={`url(#p-${p.color})`} opacity={0.7} />
              <circle cx={x} cy={y} r={r} fill={COLORS[p.color]} opacity={0.95} />
            </g>
          );
        })}
      </svg>
    </div>
  );
}

/* topic name not used in particle overlay; suppress unused import in some bundlers */
export type { TopicName };
