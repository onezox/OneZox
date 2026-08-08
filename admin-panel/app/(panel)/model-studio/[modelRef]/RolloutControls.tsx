'use client';

import { useActionState } from 'react';

import { startRollout, promoteRollout, abortRollout, emptyState } from '../actions';
import styles from './controls.module.css';

/**
 * Live promote / rollback controls — Architecture Part R's own
 * "stages a canary rollout with live promote/rollback controls".
 *
 * What is NOT here is as deliberate as what is: there is no stage
 * selector, no percentage input, and no "jump to 100%" control. Promote
 * advances exactly one staged step and the server decides which — the
 * panel has no field with which to ask for anything else, because
 * admin.proto defines none (EC4, proven at Step T by attempting exactly
 * such a crafted request and having the contract itself refuse it).
 *
 * Disabled buttons for a viewer are a courtesy only. admin-api's RBAC
 * interceptor is what actually refuses, on every request, whatever this
 * component renders.
 */

function Pending({ children, pending }: { children: string; pending: boolean }) {
  return <>{pending ? '…' : children}</>;
}

export function StartRolloutForm({
  modelRef,
  versions,
  canMutate,
}: {
  modelRef: string;
  versions: Array<{ versionId: string; createdAt: string }>;
  canMutate: boolean;
}) {
  const [state, action, pending] = useActionState(startRollout, emptyState);

  return (
    <form action={action} className={styles.form}>
      <input type="hidden" name="modelRef" value={modelRef} />
      {state.error ? (
        <p className={styles.error} role="alert">
          {state.error}
        </p>
      ) : null}
      {state.ok ? (
        <p className={styles.ok} role="status">
          {state.ok}
        </p>
      ) : null}

      <label className={styles.label} htmlFor="versionId">
        Version to canary
      </label>
      <div className={styles.row}>
        <select id="versionId" name="versionId" className={styles.select} required>
          <option value="">Select a published version…</option>
          {versions.map((v) => (
            <option key={v.versionId} value={v.versionId}>
              {v.versionId} — {v.createdAt}
            </option>
          ))}
        </select>
        <button className={styles.primary} type="submit" disabled={pending || !canMutate}>
          <Pending pending={pending}>Start canary at 1%</Pending>
        </button>
      </div>
      <p className={styles.note}>
        A rollout always begins at the first staged step. Stages advance
        automatically as each SLO analysis passes.
      </p>
    </form>
  );
}

export function PromoteAbortForms({
  modelRef,
  rolloutId,
  canMutate,
}: {
  modelRef: string;
  rolloutId: string;
  canMutate: boolean;
}) {
  const [promoteState, promoteAction, promoting] = useActionState(promoteRollout, emptyState);
  const [abortState, abortAction, aborting] = useActionState(abortRollout, emptyState);

  const error = promoteState.error ?? abortState.error;
  const ok = promoteState.ok ?? abortState.ok;

  return (
    <div className={styles.form}>
      {error ? (
        <p className={styles.error} role="alert">
          {error}
        </p>
      ) : null}
      {ok ? (
        <p className={styles.ok} role="status">
          {ok}
        </p>
      ) : null}

      <div className={styles.row}>
        <form action={promoteAction}>
          <input type="hidden" name="rolloutId" value={rolloutId} />
          <input type="hidden" name="modelRef" value={modelRef} />
          <button className={styles.primary} type="submit" disabled={promoting || !canMutate}>
            <Pending pending={promoting}>Promote one stage</Pending>
          </button>
        </form>

        <form action={abortAction}>
          <input type="hidden" name="rolloutId" value={rolloutId} />
          <input type="hidden" name="modelRef" value={modelRef} />
          <button className={styles.danger} type="submit" disabled={aborting || !canMutate}>
            <Pending pending={aborting}>Abort &amp; roll back</Pending>
          </button>
        </form>
      </div>

      <p className={styles.note}>
        Promote skips the current wait and advances exactly one step — the same
        single-step transition the automatic driver performs. Abort reverts
        traffic to the previous stable version immediately.
      </p>
    </div>
  );
}
