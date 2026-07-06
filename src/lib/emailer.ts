// Nodemailer-based email sender for the Notification Agent.
// Gracefully skips sending when SMTP credentials are not configured.

import nodemailer from "nodemailer";
import type { AgentSummaryPayload, EmailResult } from "./types";

function isSmtpConfigured(): boolean {
  return !!(
    process.env.SMTP_HOST &&
    process.env.SMTP_USER &&
    process.env.SMTP_PASS &&
    process.env.SMTP_USER !== "your-gmail@gmail.com"
  );
}

function buildHtml(p: AgentSummaryPayload): string {
  const ts = new Date(p.ts).toISOString();
  const confidence = p.reasoning ? `${Math.round(p.reasoning.confidence * 100)}%` : "—";
  const kafkaFeature = p.reasoning?.kafkaFeatureCited ?? "—";
  const rootCause = p.reasoning?.rootCause ?? "—";
  const rationale = p.reasoning?.rationale ?? "—";
  const isRejected = p.action?.outcome === "rejected";
  const action  = p.action?.detail ?? "—";
  const outcome = p.action?.outcome ?? "—";
  const outcomeColor  = isRejected ? "#dc2626" : "#16a34a";
  const outcomeBg     = isRejected ? "#fef2f2" : "#dcfce7";
  const outcomeBorder = isRejected ? "#fca5a5" : "#86efac";
  const outcomeLabel  = isRejected ? "🚫 REJECTED" : "✅ SUCCESS";
  const lagRow = (p.action?.lagBefore != null && !isRejected)
    ? `<tr><td>Lag resolved</td><td><strong>${p.action.lagBefore.toLocaleString()} → ${(p.action.lagAfter ?? 0).toLocaleString()} messages</strong></td></tr>`
    : "";
  const mutation = (!isRejected && p.action?.clusterMutation)
    ? `<tr><td>Cluster mutation</td><td><code>${p.action.clusterMutation}</code></td></tr>`
    : "";
  const approvedByRow = isRejected
    ? `<tr><td>Rejected by</td><td>${p.approvedBy ?? "operator"}</td></tr>`
    : p.approvedBy
    ? `<tr><td>Approved by</td><td>${p.approvedBy}</td></tr>`
    : `<tr><td>Approval</td><td>auto (no gate required)</td></tr>`;
  const lessonRows = p.reasoning?.lessonsCited?.length
    ? p.reasoning.lessonsCited.map((l) => `<li style="margin:4px 0;font-size:12px;color:#64748b;">${l}</li>`).join("")
    : "<li style='color:#94a3b8;font-size:12px;'>No prior lessons cited</li>";
  const lessonNotes = isRejected
    ? "No lesson recorded — action was rejected by operator. Cluster was not modified."
    : (p.lesson?.notes ?? "—");

  const EVENT_COLORS: Record<string, { bg: string; color: string; border: string }> = {
    publish:      { bg: "#dbeafe", color: "#1d4ed8", border: "#93c5fd" },
    consume:      { bg: "#dcfce7", color: "#166534", border: "#86efac" },
    reasoning:    { bg: "#ede9fe", color: "#6d28d9", border: "#c4b5fd" },
    "tool-call":  { bg: "#ffedd5", color: "#9a3412", border: "#fdba74" },
    approval:     { bg: "#fef9c3", color: "#92400e", border: "#fde047" },
    lesson:       { bg: "#cffafe", color: "#155e75", border: "#67e8f9" },
    notification: { bg: "#fce7f3", color: "#9d174d", border: "#f9a8d4" },
  };
  const eventsTableHtml = p.liveEvents?.length
    ? `<table width="100%" cellpadding="6" cellspacing="0"
         style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:12px;color:#1e293b;">
        <tr style="background:#f1f5f9;border-bottom:1px solid #e2e8f0;">
          <th style="text-align:left;padding:8px 12px;color:#64748b;font-weight:600;width:80px;">Event</th>
          <th style="text-align:left;padding:8px 12px;color:#64748b;font-weight:600;width:90px;">Agent</th>
          <th style="text-align:left;padding:8px 12px;color:#64748b;font-weight:600;">Summary</th>
        </tr>
        ${p.liveEvents.map((ev, i) => {
          const c = EVENT_COLORS[ev.type] ?? { bg: "#f1f5f9", color: "#475569", border: "#cbd5e1" };
          const bg = i % 2 === 0 ? "#ffffff" : "#f8fafc";
          return `<tr style="background:${bg};border-bottom:1px solid #f1f5f9;">
            <td style="padding:7px 12px;">
              <span style="background:${c.bg};color:${c.color};border:1px solid ${c.border};border-radius:4px;padding:2px 7px;font-size:10px;font-weight:700;white-space:nowrap;">${ev.type}</span>
            </td>
            <td style="padding:7px 12px;color:#64748b;font-size:11px;">[${ev.agent}]</td>
            <td style="padding:7px 12px;color:#334155;font-size:12px;line-height:1.5;">${ev.summary}</td>
          </tr>`;
        }).join("")}
      </table>`
    : `<p style="color:#94a3b8;font-size:12px;margin:0;">No events captured for this scenario run.</p>`;
  const adjustedThreshold = (!isRejected && p.lesson?.adjustedThreshold)
    ? `Adjusted threshold → ${p.lesson.adjustedThreshold.toLocaleString()} msgs`
    : "";

  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Agent Mesh SRE — Incident Summary</title></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:'Segoe UI',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:32px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr><td style="background:linear-gradient(135deg,#1e3a8a 0%,#3b82f6 100%);padding:28px 32px;">
    <div style="font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-0.3px;">
      🤖 Agent Mesh SRE — Incident Summary
    </div>
    <div style="font-size:13px;color:#bfdbfe;margin-top:6px;">
      Scenario: <strong style="color:#ffffff;">${p.scenarioLabel}</strong> &nbsp;·&nbsp; ${ts}
    </div>
  </td></tr>

  <!-- MRAL badge row -->
  <tr><td style="padding:20px 32px 0;">
    <div style="display:flex;gap:8px;flex-wrap:wrap;">
      ${["Monitor","Reason","Act","Learn"].map((phase, i) => {
        const colors = ["#3b82f6","#8b5cf6","#f97316","#22c55e"];
        return `<span style="background:${colors[i]}18;color:${colors[i]};border:1px solid ${colors[i]}40;border-radius:20px;padding:4px 12px;font-size:11px;font-weight:700;letter-spacing:0.5px;">${phase}</span>`;
      }).join("")}
    </div>
  </td></tr>

  <!-- Reasoning section -->
  <tr><td style="padding:20px 32px 0;">
    <div style="font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px;">
      🧠 Monitor → Reason
    </div>
    <table width="100%" cellpadding="8" cellspacing="0"
      style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;color:#1e293b;">
      <tr style="border-bottom:1px solid #e2e8f0;">
        <td style="width:160px;color:#64748b;font-weight:600;">Root cause</td>
        <td>${rootCause}</td>
      </tr>
      <tr style="border-bottom:1px solid #e2e8f0;">
        <td style="color:#64748b;font-weight:600;">Kafka feature</td>
        <td><span style="background:#dbeafe;color:#1d4ed8;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:700;">${kafkaFeature}</span></td>
      </tr>
      <tr style="border-bottom:1px solid #e2e8f0;">
        <td style="color:#64748b;font-weight:600;">Confidence</td>
        <td><strong>${confidence}</strong></td>
      </tr>
      <tr>
        <td style="color:#64748b;font-weight:600;vertical-align:top;">Rationale</td>
        <td style="font-size:13px;line-height:1.6;color:#334155;">${rationale}</td>
      </tr>
    </table>
  </td></tr>

  <!-- Action section -->
  <tr><td style="padding:20px 32px 0;">
    <div style="font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px;">
      ⚡ Act
    </div>
    <table width="100%" cellpadding="8" cellspacing="0"
      style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;font-size:14px;color:#1e293b;">
      <tr style="border-bottom:1px solid #e2e8f0;">
        <td style="width:160px;color:#64748b;font-weight:600;">Action taken</td>
        <td>${action}</td>
      </tr>
      <tr style="border-bottom:1px solid #e2e8f0;">
        <td style="color:#64748b;font-weight:600;">Outcome</td>
        <td><span style="background:${outcomeBg};color:${outcomeColor};border:1px solid ${outcomeBorder};padding:2px 10px;border-radius:20px;font-size:12px;font-weight:700;">${outcomeLabel}</span></td>
      </tr>
      ${lagRow}
      ${mutation}
      ${approvedByRow}
    </table>
  </td></tr>

  <!-- Learn section -->
  <tr><td style="padding:20px 32px 0;">
    <div style="font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px;">
      📚 Learn
    </div>
    <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:14px 16px;">
      <div style="font-size:13px;color:#334155;line-height:1.6;">${lessonNotes}</div>
      ${adjustedThreshold ? `<div style="font-size:12px;color:#3b82f6;margin-top:6px;font-weight:600;">${adjustedThreshold}</div>` : ""}
      ${p.reasoning?.lessonsCited?.length ? `
      <div style="margin-top:10px;">
        <div style="font-size:11px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">Prior lessons cited by LLM:</div>
        <ul style="margin:0;padding-left:18px;">${lessonRows}</ul>
      </div>` : ""}
    </div>
  </td></tr>

  <!-- Notifications section (approved = Slack+ITSM, rejected = red notice) -->
  ${!isRejected ? `
  <tr><td style="padding:20px 32px 0;">
    <div style="font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px;">
      🔔 Notifications Sent
    </div>
    <div style="background:#fefce8;border:1px solid #fde68a;border-radius:8px;padding:12px 16px;font-size:13px;color:#92400e;">
      <div>💬 <strong>Slack</strong> #sre-alerts: ${p.slackMessage}</div>
      <div style="margin-top:6px;">🎫 <strong>ITSM</strong>: ${p.itsmTicket}</div>
    </div>
  </td></tr>` : `
  <tr><td style="padding:20px 32px 0;">
    <div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:12px 16px;font-size:13px;color:#991b1b;">
      🚫 <strong>No notifications sent</strong> — action was rejected by operator. No Slack message or ITSM ticket created.
    </div>
  </td></tr>`}

  <!-- Live Events Timeline -->
  <tr><td style="padding:20px 32px 0;">
    <div style="font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;letter-spacing:0.8px;margin-bottom:10px;">
      📡 Live Events Timeline
    </div>
    ${eventsTableHtml}
  </td></tr>

  <!-- Footer -->
  <tr><td style="padding:24px 32px;border-top:1px solid #e2e8f0;margin-top:24px;">
    <div style="font-size:12px;color:#94a3b8;line-height:1.6;">
      This email was sent automatically by the <strong>Agent Mesh SRE</strong> Notification Agent
      to <strong>Admin / Stakeholders</strong>.<br>
      All access is logged. Scenario: <code>${p.scenarioId}</code> · Status: <strong>${isRejected ? "REJECTED" : "RESOLVED"}</strong>
    </div>
  </td></tr>

</table>
</td></tr></table>
</body></html>`;
}

export async function sendAgentSummary(payload: AgentSummaryPayload): Promise<EmailResult> {
  if (!isSmtpConfigured()) {
    return { ok: false, error: "smtp_not_configured" };
  }

  try {
    const transporter = nodemailer.createTransport({
      host:   process.env.SMTP_HOST,
      port:   Number(process.env.SMTP_PORT ?? 587),
      secure: process.env.SMTP_SECURE === "true",
      auth: {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
      },
    });

    const to = process.env.NOTIFICATION_EMAIL;
    if (!to) {
      return { ok: false, error: "NOTIFICATION_EMAIL is not configured — refusing to send to an unconfigured recipient" };
    }
    const info = await transporter.sendMail({
      from:    `"Agent Mesh SRE" <${process.env.SMTP_USER}>`,
      to,
      subject: `[SRE Mesh] Incident resolved — ${payload.scenarioLabel}`,
      html:    buildHtml(payload),
    });

    return { ok: true, messageId: info.messageId };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, error: message };
  }
}
