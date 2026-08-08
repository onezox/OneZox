'use server';

import { redirect } from 'next/navigation';

import { setSession, clearSession } from '@/lib/session';
import { log, errorMessage } from '@/lib/log';

/**
 * Login / logout Server Actions — Phase-05 Step U2.
 *
 * The credential is the raw admin token minted by
 * scripts/seed-admin-user.sh (Decision 1: admin_user rows are
 * provisioned by that script alone, never self-service through this
 * panel — there is deliberately no "create admin" RPC anywhere in
 * admin.proto).
 *
 * "Logging in" therefore means: prove the token is a live, non-revoked
 * admin_user row, then store it in an httpOnly cookie. Proof is
 * delegated to admin-api's own authn middleware — the panel does NOT
 * hash the token or query admin_user itself. That matters: if the panel
 * validated credentials locally it would become a second authentication
 * implementation that could drift from the real one, and it would need
 * a DB grant it should never have (the panel has NO database access at
 * all, by design).
 *
 * The token is read from the form, sent once to admin-api, and either
 * placed in the httpOnly cookie or discarded. It is never logged, never
 * echoed back into the rendered page, and never returned to the client.
 */

const ADMIN_API_URL =
  process.env.ADMIN_API_GRAPHQL_URL ??
  'http://admin-api.default.svc.cluster.local:8080/graphql';

export type LoginState = { error: string | null };

export async function login(_prev: LoginState, formData: FormData): Promise<LoginState> {
  const token = String(formData.get('token') ?? '').trim();
  if (!token) return { error: 'Enter an admin token.' };

  let res: Response;
  try {
    res = await fetch(ADMIN_API_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify({ query: '{ me { id role } }' }),
      cache: 'no-store',
    });
  } catch (err) {
    log.error('login: admin-api unreachable', { error: errorMessage(err) });
    return { error: 'admin-api is unreachable. Check the service and try again.' };
  }

  if (res.status === 401) {
    // Deliberately identical text for "no such token" and "revoked
    // token" — distinguishing them would confirm to an attacker that a
    // guessed value was once real. Same reasoning admin-api's own authn
    // layer uses for returning a single Unauthenticated for both.
    log.warn('login: rejected credential');
    return { error: 'That token was not accepted.' };
  }
  if (!res.ok) {
    log.error('login: admin-api returned non-200', { status: res.status });
    return { error: `admin-api returned HTTP ${res.status}.` };
  }

  const body = (await res.json()) as {
    data?: { me?: { id: string; role: string } };
    errors?: Array<{ message: string }>;
  };
  if (body.errors?.length || !body.data?.me) {
    log.warn('login: rejected credential (graphql error)');
    return { error: 'That token was not accepted.' };
  }

  await setSession(token);
  // user_id and role only — the token itself is never a log field.
  log.info('admin signed in', { user_id: body.data.me.id, role: body.data.me.role });

  redirect('/dashboard');
}

export async function logout(): Promise<void> {
  await clearSession();
  log.info('admin signed out');
  redirect('/login');
}
