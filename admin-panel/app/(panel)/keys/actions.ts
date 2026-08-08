'use server';

import { revalidatePath } from 'next/cache';

import * as adminApi from '@/lib/grpc';
import { NotAuthenticatedError } from '@/lib/graphql';
import { log, errorMessage } from '@/lib/log';

/**
 * API-key management mutations — Phase-05 Step U2.
 *
 * createKey returns the raw key ONCE, in this action's own return
 * value, to be rendered a single time and never again. Nothing here
 * persists it: it is not written to a log, not cached, not stored in a
 * cookie, and there is no query anywhere in this system that can
 * retrieve it later (listApiKeys returns metadata only, and even the
 * HASH is never exposed — Step S). That is the same shown-once
 * discipline seed-test-tenant.sh and seed-admin-user.sh already follow.
 */

export type KeyActionState = {
  error: string | null;
  createdKeyId: string | null;
  createdRawKey: string | null;
  ok: string | null;
};

export const emptyKeyState: KeyActionState = {
  error: null,
  createdKeyId: null,
  createdRawKey: null,
  ok: null,
};

function failure(err: unknown, what: string): KeyActionState {
  if (err instanceof NotAuthenticatedError) {
    return { ...emptyKeyState, error: 'Your session is no longer valid. Sign in again.' };
  }
  const message = errorMessage(err);
  log.error(`${what} failed`, { error: message });
  return { ...emptyKeyState, error: message };
}

export async function createKey(
  _prev: KeyActionState,
  formData: FormData,
): Promise<KeyActionState> {
  const orgId = String(formData.get('orgId') ?? '').trim();
  const scopesRaw = String(formData.get('scopes') ?? '').trim();
  if (!orgId) return { ...emptyKeyState, error: 'An org_id is required.' };

  const scopes = scopesRaw
    ? scopesRaw.split(',').map((s) => s.trim()).filter(Boolean)
    : [];

  try {
    const res = await adminApi.createApiKey({ orgId, scopes });
    // key_id and org_id only — the raw key is returned to the caller
    // below but must never appear in a log line.
    log.info('created api key from panel', { key_id: res.keyId, org_id: orgId });
    revalidatePath('/keys');
    return {
      error: null,
      createdKeyId: res.keyId,
      createdRawKey: res.rawKey,
      ok: 'Key created. Copy it now — it cannot be shown again.',
    };
  } catch (err) {
    return failure(err, 'createApiKey');
  }
}

export async function revokeKey(
  _prev: KeyActionState,
  formData: FormData,
): Promise<KeyActionState> {
  const keyId = String(formData.get('keyId') ?? '');
  try {
    await adminApi.revokeApiKey({ keyId });
    log.info('revoked api key from panel', { key_id: keyId });
    revalidatePath('/keys');
    return { ...emptyKeyState, ok: `Revoked ${keyId}.` };
  } catch (err) {
    return failure(err, 'revokeApiKey');
  }
}
