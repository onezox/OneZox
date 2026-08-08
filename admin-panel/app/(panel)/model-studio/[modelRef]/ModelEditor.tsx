'use client';

import { useActionState, useMemo, useState } from 'react';

import { publishVersion, emptyState } from '../actions';
import styles from './editor.module.css';

/**
 * Model Studio editor + diff view — Phase-05 Step U2.
 *
 * Phase-05.txt's own UX requirement: "Model Studio diff view matches the
 * actual manifest delta." So the diff here compares the EXACT bytes that
 * will be published against the EXACT bytes of the current active
 * manifest — no normalising, no pretty-printing, no key reordering
 * before comparison.
 *
 * That is deliberate and it matters. control-plane signs spec_json
 * byte-for-byte and every consumer re-verifies that signature
 * independently (Phase-04), so whitespace IS part of what gets signed —
 * a diff that hid formatting changes would be showing the operator
 * something other than what they are about to publish. Phase-04's own
 * migration 0013 exists because of exactly this class of
 * JSON-reformatting mismatch.
 *
 * The starting text is the current active manifest, which is what
 * Phase-05.txt calls createModelDraft — a read, persisting nothing until
 * the operator publishes.
 */

type Line = { text: string; kind: 'same' | 'added' | 'removed' };

/**
 * A minimal longest-common-subsequence line diff. Small on purpose: a
 * manifest is a handful of lines, and pulling a diff library in for
 * this would be a dependency out of proportion to the job.
 */
function diffLines(before: string, after: string): Line[] {
  const a = before.split('\n');
  const b = after.split('\n');

  const lcs: number[][] = Array.from({ length: a.length + 1 }, () =>
    new Array<number>(b.length + 1).fill(0),
  );
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
    }
  }

  const out: Line[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      out.push({ text: a[i], kind: 'same' });
      i++;
      j++;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) {
      out.push({ text: a[i], kind: 'removed' });
      i++;
    } else {
      out.push({ text: b[j], kind: 'added' });
      j++;
    }
  }
  while (i < a.length) out.push({ text: a[i++], kind: 'removed' });
  while (j < b.length) out.push({ text: b[j++], kind: 'added' });
  return out;
}

export function ModelEditor({
  modelRef,
  activeSpecJson,
  canPublish,
}: {
  modelRef: string;
  activeSpecJson: string;
  canPublish: boolean;
}) {
  const [draft, setDraft] = useState(activeSpecJson);
  const [state, formAction, pending] = useActionState(publishVersion, emptyState);

  const changed = draft !== activeSpecJson;
  const diff = useMemo(
    () => (changed ? diffLines(activeSpecJson, draft) : []),
    [activeSpecJson, draft, changed],
  );

  let jsonError: string | null = null;
  try {
    JSON.parse(draft);
  } catch (e) {
    jsonError = e instanceof Error ? e.message : 'Invalid JSON';
  }

  return (
    <form action={formAction} className={styles.wrap}>
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

      <label className={styles.label} htmlFor="specJson">
        Manifest spec (JSON)
      </label>
      <textarea
        id="specJson"
        name="specJson"
        className={styles.textarea}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        rows={8}
        spellCheck={false}
      />

      {jsonError ? (
        <p className={styles.warn}>Not valid JSON yet: {jsonError}</p>
      ) : null}

      <div className={styles.diffHeader}>
        <h3 className={styles.diffTitle}>Diff vs active version</h3>
        {changed ? (
          <span className={styles.diffMeta}>
            {diff.filter((l) => l.kind === 'added').length} added ·{' '}
            {diff.filter((l) => l.kind === 'removed').length} removed
          </span>
        ) : null}
      </div>

      {changed ? (
        <pre className={styles.diff}>
          {diff.map((l, idx) => (
            <div key={idx} className={styles[l.kind]}>
              <span className={styles.gutter}>
                {l.kind === 'added' ? '+' : l.kind === 'removed' ? '-' : ' '}
              </span>
              {l.text}
            </div>
          ))}
        </pre>
      ) : (
        <p className={styles.noDiff}>
          No changes. The draft is byte-identical to the active manifest.
        </p>
      )}

      <p className={styles.note}>
        Whitespace counts: control-plane signs these exact bytes and every
        consumer re-verifies that signature independently, so the diff above is
        the literal delta that will be published.
      </p>

      <button
        className={styles.button}
        type="submit"
        disabled={pending || !changed || jsonError !== null || !canPublish}
        title={canPublish ? undefined : 'Your role cannot publish manifest versions'}
      >
        {pending ? 'Publishing…' : 'Publish new version'}
      </button>
      {!canPublish ? (
        <p className={styles.note}>
          Publishing requires the admin role. Your session is read-only.
        </p>
      ) : null}
    </form>
  );
}
