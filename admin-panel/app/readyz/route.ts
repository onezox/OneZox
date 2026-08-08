import { log, errorMessage } from '@/lib/log';

/**
 * Readiness — Phase-05 Step U2.
 *
 * Unlike /healthz, this DOES check the one dependency the panel cannot
 * function without: admin-api. Every page in this console reads through
 * it, so a panel that cannot reach admin-api has nothing to serve and
 * should be taken out of the Service's endpoint list rather than
 * serving error pages.
 *
 * The probe is unauthenticated by design — it sends no session and
 * asks nothing of admin-api's data. A 401 back from the GraphQL
 * endpoint is a perfectly good READY signal: it means admin-api is
 * reachable, listening, and correctly refusing anonymous callers. Only
 * a transport failure means not-ready.
 */
export const dynamic = 'force-dynamic';

const ADMIN_API_URL =
  process.env.ADMIN_API_GRAPHQL_URL ??
  'http://admin-api.default.svc.cluster.local:8080/graphql';

export async function GET() {
  try {
    const res = await fetch(ADMIN_API_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: '{ __typename }' }),
      cache: 'no-store',
      signal: AbortSignal.timeout(3000),
    });
    // Any HTTP response at all proves reachability; 401 is expected.
    if (res.status >= 500) {
      log.error('readiness: admin-api returned a server error', { status: res.status });
      return new Response('admin-api unhealthy', { status: 503 });
    }
    return new Response('ready', { status: 200, headers: { 'content-type': 'text/plain' } });
  } catch (err) {
    log.error('readiness: admin-api unreachable', { error: errorMessage(err) });
    return new Response('admin-api unreachable', { status: 503 });
  }
}
