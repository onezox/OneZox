import 'server-only';

import { getSessionToken } from './session';
import { log, errorMessage } from './log';

/**
 * The READ path — Phase-05 Step U2.
 *
 * Every panel read goes through admin-api's GraphQL surface
 * (proto/admin/v1/admin.graphql), server-side only. Mutations go
 * through gRPC instead (lib/grpc.ts) — the commands-vs-queries split
 * settled at Step D and carried all the way down into admin-api's own
 * Go interfaces at Step U1b.
 *
 * `import 'server-only'` is the enforcement: this module reads the
 * httpOnly session cookie, so a Client Component importing it is a
 * build error rather than a token leak into the browser bundle.
 */

const ADMIN_API_URL =
  process.env.ADMIN_API_GRAPHQL_URL ??
  'http://admin-api.default.svc.cluster.local:8080/graphql';

export class NotAuthenticatedError extends Error {
  constructor() {
    super('no admin session');
    this.name = 'NotAuthenticatedError';
  }
}

interface GraphQLResponse<T> {
  data?: T;
  errors?: Array<{ message: string }>;
}

/**
 * query runs one GraphQL query as the current session's admin identity.
 *
 * Throws NotAuthenticatedError when there is no session, so callers can
 * redirect to /login rather than rendering a half-empty page. Any other
 * failure throws a plain Error carrying the GraphQL message — panel
 * pages surface that text to the operator, which is appropriate here:
 * admin-api's own resolvers return operational messages ("control-plane
 * unreachable"), never anything sensitive.
 */
export async function query<T>(
  gql: string,
  variables?: Record<string, unknown>,
): Promise<T> {
  const token = await getSessionToken();
  if (!token) throw new NotAuthenticatedError();

  const started = Date.now();
  let res: Response;
  try {
    res = await fetch(ADMIN_API_URL, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ query: gql, variables }),
      // Operator consoles must never show stale control-plane state —
      // a cached "no rollout in progress" during a real canary would be
      // actively dangerous. Every read is fresh.
      cache: 'no-store',
    });
  } catch (err) {
    log.error('graphql request failed', { error: errorMessage(err) });
    throw new Error('admin-api unreachable');
  }

  if (res.status === 401) {
    // The credential was rejected — a revoked admin_user row, or a
    // stale cookie from before a re-seed. Same handling as no session
    // at all: send them back to /login.
    throw new NotAuthenticatedError();
  }
  if (!res.ok) {
    log.error('graphql returned non-200', { status: res.status });
    throw new Error(`admin-api returned HTTP ${res.status}`);
  }

  const body = (await res.json()) as GraphQLResponse<T>;
  if (body.errors?.length) {
    const message = body.errors.map((e) => e.message).join('; ');
    log.error('graphql returned errors', { error: message });
    throw new Error(message);
  }
  if (!body.data) throw new Error('admin-api returned no data');

  // Success-path logging, not only failures — the same discipline every
  // backend service here follows (Phase-01's own lesson).
  log.info('graphql query served', { latency_ms: Date.now() - started });
  return body.data;
}
