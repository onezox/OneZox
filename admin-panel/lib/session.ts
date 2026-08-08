import 'server-only';

import { cookies } from 'next/headers';

/**
 * Session handling — Phase-05 Step U2.
 *
 * The panel's session IS an admin credential (admin_user.credential_hash,
 * migration 0015) — the same token scripts/seed-admin-user.sh mints and
 * admin-api's own authn middleware hashes and looks up. Two properties
 * follow, and both are load-bearing:
 *
 * 1. DISJOINT FROM TENANT KEYS. An admin token is looked up in
 *    admin_user, never api_keys; a tenant's api_keys value is looked up
 *    at edge-gateway, never here. Different tables, different services,
 *    no shared rows — proven at Step F (unit) and Step J (live, with a
 *    real tenant key rejected by admin-api). This module never touches a
 *    tenant credential, so the panel cannot blur that boundary.
 *
 * 2. NEVER REACHABLE FROM CLIENT-SIDE JS. The cookie is httpOnly, so
 *    document.cookie cannot read it — an XSS bug in the panel still
 *    cannot exfiltrate the admin token. `import 'server-only'` at the
 *    top of this file makes that structural rather than careful: a
 *    Client Component importing this module is a BUILD failure, not a
 *    runtime leak someone has to notice in review. The token is read
 *    server-side and attached to outbound admin-api calls; it is never
 *    serialized into a page, a prop, or a response body.
 */

const COOKIE_NAME = 'onezox_admin_session';

/**
 * Secure is on unless PANEL_INSECURE_COOKIES is explicitly set, which
 * exists solely for local `next dev` over plain http://localhost —
 * a Secure cookie is never stored on a non-TLS origin, so without this
 * escape hatch local development could not log in at all. It defaults
 * to OFF, so a deployment that simply doesn't set it gets the secure
 * behaviour; the insecure path has to be asked for by name.
 */
function secureCookies(): boolean {
  return process.env.PANEL_INSECURE_COOKIES !== 'true';
}

export async function setSession(token: string): Promise<void> {
  const jar = await cookies();
  jar.set(COOKIE_NAME, token, {
    httpOnly: true,
    secure: secureCookies(),
    // Strict, not Lax: nothing in this panel is meant to be reachable
    // by following a link from another site, so there is no legitimate
    // cross-site navigation whose session we need to preserve. Strict
    // removes the CSRF surface Lax leaves open on top-level GETs.
    sameSite: 'strict',
    path: '/',
    // Session-scoped rather than a fixed maxAge: admin_user has no
    // expiry column (migration 0015 — revocation is via revoked_at, not
    // a TTL), so inventing a client-side lifetime here would only
    // create a second, weaker source of truth. Closing the browser ends
    // the session; revoking the row ends it everywhere, immediately.
  });
}

export async function getSessionToken(): Promise<string | null> {
  const jar = await cookies();
  return jar.get(COOKIE_NAME)?.value ?? null;
}

export async function clearSession(): Promise<void> {
  const jar = await cookies();
  jar.delete(COOKIE_NAME);
}
