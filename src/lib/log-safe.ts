/**
 * log-safe.ts — credential redaction for server-side logging.
 *
 * Error objects thrown by kafkajs and @kubernetes/client-node frequently
 * embed the full client configuration (including SASL password, bearer
 * tokens, and PEM-encoded keys) in `.message`, `.config`, or nested
 * `.cause` fields. Logging those objects directly — even via
 * `console.error("context:", err)` — can leak secrets into log
 * aggregators, CI output, or Vercel's function logs.
 *
 * Use `safeErr()` (or `redact()` for arbitrary objects) anywhere a
 * caught error, config object, or env-derived value is passed to
 * console.* or returned in an API response.
 */

const SECRET_KEY_PATTERN =
  /pass(word)?|secret|token|api[-_]?key|sasl|credential|authorization|bearer|private[-_]?key|ca[-_]?cert|pem/i;

const REDACTED = "[redacted]";

/**
 * Deep-redacts any object whose key name looks credential-shaped.
 * Safe to call on arbitrary thrown values, env snapshots, or client
 * config objects before logging or returning them.
 */
export function redact<T>(value: T, _seen: WeakSet<object> = new WeakSet()): T {
  if (value === null || typeof value !== "object") return value;
  if (_seen.has(value as object)) return value; // avoid circular refs
  _seen.add(value as object);

  if (Array.isArray(value)) {
    return value.map((v) => redact(v, _seen)) as unknown as T;
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (SECRET_KEY_PATTERN.test(k)) {
      out[k] = v == null ? v : REDACTED;
    } else if (typeof v === "string" && v.length > 200) {
      // Long opaque strings (PEM blocks, JWTs) — redact defensively
      // even if the key name didn't match (e.g. "ca", "cert").
      out[k] = /^-----BEGIN |^eyJ/.test(v) ? REDACTED : v;
    } else if (typeof v === "object" && v !== null) {
      out[k] = redact(v, _seen);
    } else {
      out[k] = v;
    }
  }
  return out as T;
}

/**
 * Produces a safe-to-log representation of a caught error: message and
 * stack only, with any embedded credential-shaped fields stripped from
 * the message text itself (covers brokers libs that interpolate config
 * into the error string, e.g. "SASL PLAIN authentication failed for
 * user 'x' with password 'y'").
 */
export function safeErr(err: unknown): { message: string; name?: string; stack?: string } {
  if (err instanceof Error) {
    return {
      name: err.name,
      message: redactString(err.message),
      stack: err.stack ? redactString(err.stack) : undefined,
    };
  }
  if (typeof err === "object" && err !== null) {
    const redacted = redact(err);
    return { message: redactString(JSON.stringify(redacted)) };
  }
  return { message: redactString(String(err)) };
}

function redactString(s: string): string {
  return s
    // key=value or key: value pairs where key looks like a secret
    .replace(
      new RegExp(`(${SECRET_KEY_PATTERN.source})\\s*[:=]\\s*['"]?[^\\s'",}]+`, "gi"),
      (_m, key) => `${key}=${REDACTED}`
    )
    // inline PEM blocks
    .replace(/-----BEGIN[\s\S]+?-----END [A-Z ]+-----/g, REDACTED);
}
