// SSE stream — clients connect here and receive real-time mesh events
import { eventBus } from "@/lib/event-bus";
import { getSnapshot } from "@/lib/mesh";
import { startMonitorPolling } from "@/lib/monitor-poll";

export const dynamic    = "force-dynamic";
export const maxDuration = 60; // Vercel: keep the serverless function alive for SSE

startMonitorPolling();

export async function GET() {
  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    start(controller) {
      // Send a full state snapshot immediately on connect
      const snapshot = getSnapshot();
      const initEvent = `data: ${JSON.stringify({ type: "state", ...snapshot })}\n\n`;
      controller.enqueue(encoder.encode(initEvent));

      // Subscribe to all future events
      const unsub = eventBus.subscribe((event) => {
        try {
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
        } catch {
          unsub();
        }
      });

      // Keep-alive ping every 25 seconds to prevent proxy timeouts
      const ping = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(`: ping\n\n`));
        } catch {
          clearInterval(ping);
          unsub();
        }
      }, 25_000);
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type":  "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      "Connection":    "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}
