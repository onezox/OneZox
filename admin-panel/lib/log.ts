/**
 * Structured logging — Phase-05 Step U2.
 *
 * Same JSON-to-stdout shape every other OneZox service emits
 * (timestamp/level/service/message + fields), so Promtail ships panel
 * logs into Loki alongside the backend services with no separate
 * parsing rule. "It's a UI" is not a reason to log differently: this
 * process serves requests, calls gRPC, and holds sessions like any
 * other service here.
 *
 * NEVER log a session token, an admin credential, or a raw API key.
 * The panel handles all three; none of them belongs in a log line. Log
 * the user_id (an opaque UUID that is already in audit_log) instead —
 * it identifies the actor without carrying anything usable.
 */

type Level = 'INFO' | 'WARN' | 'ERROR';
type Fields = Record<string, unknown>;

const SERVICE = 'admin-panel';

function emit(level: Level, message: string, fields?: Fields): void {
  const line = {
    timestamp: new Date().toISOString(),
    level,
    service: SERVICE,
    message,
    ...fields,
  };
  // process.stdout, not console.log, keeps this a single write of one
  // JSON object per line even when fields contain newlines.
  process.stdout.write(JSON.stringify(line) + '\n');
}

export const log = {
  info: (message: string, fields?: Fields) => emit('INFO', message, fields),
  warn: (message: string, fields?: Fields) => emit('WARN', message, fields),
  error: (message: string, fields?: Fields) => emit('ERROR', message, fields),
};

/**
 * errorMessage narrows an unknown catch binding to something loggable
 * without ever stringifying an object that might carry credentials
 * (a gRPC error's metadata, for instance).
 */
export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}
