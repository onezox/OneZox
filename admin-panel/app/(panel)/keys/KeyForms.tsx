'use client';

import { useActionState } from 'react';

import { createKey, revokeKey, emptyKeyState } from './actions';
import styles from './keys.module.css';

/**
 * Key management forms — Phase-05 Step U2.
 *
 * The raw key is rendered exactly once, immediately after creation,
 * from the Server Action's return value. It is never re-fetched, never
 * stored client-side beyond this render, and disappears on the next
 * navigation — matching the fact that no server-side path can produce
 * it again either.
 */
export function CreateKeyForm({ canMutate }: { canMutate: boolean }) {
  const [state, action, pending] = useActionState(createKey, emptyKeyState);

  return (
    <form action={action} className={styles.form}>
      {state.error ? (
        <p className={styles.error} role="alert">
          {state.error}
        </p>
      ) : null}

      {state.createdRawKey ? (
        <div className={styles.reveal} role="status">
          <p className={styles.revealTitle}>Copy this key now — it will not be shown again.</p>
          <code className={styles.rawKey}>{state.createdRawKey}</code>
          <p className={styles.revealMeta}>
            key_id <code>{state.createdKeyId}</code>. Only a SHA-256 hash of this
            value is stored; neither the key nor its hash can be retrieved later.
          </p>
        </div>
      ) : null}

      <div className={styles.row}>
        <div className={styles.field}>
          <label className={styles.label} htmlFor="orgId">
            Tenant org_id
          </label>
          <input
            id="orgId"
            name="orgId"
            className={styles.input}
            placeholder="00000000-0000-0000-0000-000000000000"
            required
          />
        </div>
        <div className={styles.field}>
          <label className={styles.label} htmlFor="scopes">
            Scopes (comma-separated)
          </label>
          <input
            id="scopes"
            name="scopes"
            className={styles.input}
            placeholder="chat.completions, responses, embeddings, models"
          />
        </div>
      </div>

      <button className={styles.primary} type="submit" disabled={pending || !canMutate}>
        {pending ? 'Creating…' : 'Create key'}
      </button>
      {!canMutate ? (
        <p className={styles.note}>Creating keys requires the admin role.</p>
      ) : null}
    </form>
  );
}

export function RevokeKeyButton({ keyId, canMutate }: { keyId: string; canMutate: boolean }) {
  const [state, action, pending] = useActionState(revokeKey, emptyKeyState);

  return (
    <form action={action}>
      <input type="hidden" name="keyId" value={keyId} />
      <button className={styles.danger} type="submit" disabled={pending || !canMutate}>
        {pending ? '…' : 'Revoke'}
      </button>
      {state.error ? (
        <span className={styles.inlineError} role="alert">
          {state.error}
        </span>
      ) : null}
    </form>
  );
}
