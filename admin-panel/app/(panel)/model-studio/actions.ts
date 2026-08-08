'use server';

import { revalidatePath } from 'next/cache';

import * as adminApi from '@/lib/grpc';
import { NotAuthenticatedError } from '@/lib/graphql';
import { log, errorMessage } from '@/lib/log';

/**
 * Model Studio mutations — Phase-05 Step U2.
 *
 * Each of these is a thin Server Action over one admin-api gRPC command.
 * The panel adds NO logic of its own: it does not choose a version_id,
 * does not compute a stage, and cannot set a canary percentage — those
 * live in control-plane's rollout state machine, and admin.proto has no
 * field through which a caller could influence them (the EC4 property
 * Step T proved by attempting exactly that).
 *
 * Every one of these is also audited server-side, on both success and
 * failure, by admin-api (Steps H/L/R) — the panel is not trusted to
 * report what it did.
 */

export type ActionState = { error: string | null; ok: string | null };

export const emptyState: ActionState = { error: null, ok: null };

/** Shared error mapping so every action reports failures identically. */
function toState(err: unknown, what: string): ActionState {
  if (err instanceof NotAuthenticatedError) {
    return { error: 'Your session is no longer valid. Sign in again.', ok: null };
  }
  const message = errorMessage(err);
  log.error(`${what} failed`, { error: message });
  return { error: message, ok: null };
}

export async function publishVersion(
  _prev: ActionState,
  formData: FormData,
): Promise<ActionState> {
  const modelRef = String(formData.get('modelRef') ?? '');
  const specJson = String(formData.get('specJson') ?? '');

  // Parse-check locally purely to give a fast, precise message — the
  // authoritative validation is control-plane's (it signs the manifest
  // and rejects malformed content regardless of what the panel thinks).
  try {
    JSON.parse(specJson);
  } catch {
    return { error: 'Spec is not valid JSON.', ok: null };
  }

  try {
    const res = await adminApi.publishModelVersion({ modelRef, specJson });
    log.info('published model version from panel', { model_ref: modelRef, version_id: res.versionId });
    revalidatePath(`/model-studio/${modelRef}`);
    return { error: null, ok: `Published version ${res.versionId}.` };
  } catch (err) {
    return toState(err, 'publishModelVersion');
  }
}

export async function startRollout(
  _prev: ActionState,
  formData: FormData,
): Promise<ActionState> {
  const modelRef = String(formData.get('modelRef') ?? '');
  const versionId = String(formData.get('versionId') ?? '');
  if (!versionId) return { error: 'Choose a version to roll out.', ok: null };

  try {
    const res = await adminApi.startRollout({ modelRef, versionId, strategyJson: '{}' });
    log.info('started rollout from panel', { model_ref: modelRef, rollout_id: res.rolloutId });
    revalidatePath(`/model-studio/${modelRef}`);
    revalidatePath('/rollouts');
    return { error: null, ok: `Started rollout ${res.rolloutId} at 1%.` };
  } catch (err) {
    return toState(err, 'startRollout');
  }
}

export async function promoteRollout(
  _prev: ActionState,
  formData: FormData,
): Promise<ActionState> {
  const rolloutId = String(formData.get('rolloutId') ?? '');
  const modelRef = String(formData.get('modelRef') ?? '');
  try {
    const res = await adminApi.promoteRollout({ rolloutId });
    log.info('promoted rollout from panel', { rollout_id: rolloutId, new_stage: res.newStage });
    revalidatePath(`/model-studio/${modelRef}`);
    revalidatePath('/rollouts');
    return { error: null, ok: `Advanced to ${res.newStage}.` };
  } catch (err) {
    return toState(err, 'promoteRollout');
  }
}

export async function abortRollout(
  _prev: ActionState,
  formData: FormData,
): Promise<ActionState> {
  const rolloutId = String(formData.get('rolloutId') ?? '');
  const modelRef = String(formData.get('modelRef') ?? '');
  try {
    await adminApi.abortRollout({ rolloutId });
    log.info('aborted rollout from panel', { rollout_id: rolloutId });
    revalidatePath(`/model-studio/${modelRef}`);
    revalidatePath('/rollouts');
    return { error: null, ok: 'Rollout aborted; traffic reverted to the previous stable version.' };
  } catch (err) {
    return toState(err, 'abortRollout');
  }
}
