/**
 * Liveness — Phase-05 Step U2.
 *
 * Deliberately outside the (panel) route group, so it is NOT behind the
 * session gate: kubelet probes carry no admin session, and a liveness
 * check that required one would report a healthy process as dead.
 *
 * Answers only "is this Node process serving HTTP" — it makes no
 * downstream call, so a transient admin-api blip can never cause
 * Kubernetes to restart a perfectly healthy panel. Readiness is where
 * dependencies belong.
 */
export const dynamic = 'force-dynamic';

export function GET() {
  return new Response('ok', {
    status: 200,
    headers: { 'content-type': 'text/plain' },
  });
}
