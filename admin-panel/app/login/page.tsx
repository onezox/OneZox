'use client';

import { useActionState } from 'react';
import { login, type LoginState } from './actions';
import styles from './login.module.css';

const initial: LoginState = { error: null };

/**
 * Login page — Phase-05 Step U2.
 *
 * A Client Component only for useActionState's pending/error handling.
 * The token it collects goes straight into a Server Action and is never
 * stored in component state, localStorage, or any client-side store —
 * the browser's only lasting record of the session is the httpOnly
 * cookie the action sets, which this code (and any XSS in it) cannot
 * read back.
 *
 * autoComplete="off" + type="password": this is a bearer credential, so
 * it should neither render in plaintext over someone's shoulder nor be
 * offered up by a password manager as if it were a reusable website
 * login.
 */
export default function LoginPage() {
  const [state, formAction, pending] = useActionState(login, initial);

  return (
    <main className={styles.main}>
      <form className={styles.card} action={formAction}>
        <div className={styles.brand}>
          <span className={styles.brandMark}>OneZox</span>
          <span className={styles.brandSub}>admin</span>
        </div>

        <h1 className={styles.title}>Sign in</h1>
        <p className={styles.help}>
          Paste the admin token issued by <code>scripts/seed-admin-user.sh</code>.
          Admin accounts are provisioned by that script only — there is no
          self-service sign-up.
        </p>

        {state.error ? (
          <p className={styles.error} role="alert">
            {state.error}
          </p>
        ) : null}

        <label className={styles.label} htmlFor="token">
          Admin token
        </label>
        <input
          id="token"
          name="token"
          type="password"
          className={styles.input}
          autoComplete="off"
          spellCheck={false}
          required
          // Focus lands here on load: this form has exactly one field.
          autoFocus
        />

        <button className={styles.button} type="submit" disabled={pending}>
          {pending ? 'Verifying…' : 'Sign in'}
        </button>

        <p className={styles.note}>
          This credential is separate from tenant API keys. A tenant key will not
          authenticate here.
        </p>
      </form>
    </main>
  );
}
