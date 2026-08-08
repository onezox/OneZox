import { redirect } from 'next/navigation';

import { Nav } from '@/components/Nav';
import { query, NotAuthenticatedError } from '@/lib/graphql';
import { logout } from '@/app/login/actions';
import styles from './panel.module.css';

/**
 * The authenticated shell — Phase-05 Step U2.
 *
 * Every real panel route lives under this route group, so this layout
 * is the ONE place the panel checks for a session: it resolves `me`
 * server-side on every request and redirects to /login if the session
 * is missing, revoked, or rejected.
 *
 * Worth being precise about what this is and isn't. This is a UX gate,
 * not the security boundary. The actual enforcement is admin-api's own
 * authn (Step F) and RBAC (Step G) interceptors, which run on every
 * single request regardless of what the panel does — a caller who skips
 * the panel entirely and hits admin-api directly is refused there, which
 * is exactly what Step J proved live. If this layout were removed
 * tomorrow, nothing would become permitted; the panel would just render
 * error pages instead of redirecting politely.
 *
 * role is passed to Nav so the UI can hide mutating controls from a
 * viewer. Same framing: hiding a button is a courtesy, the PermissionDenied
 * from admin-api is the guarantee.
 */
export default async function PanelLayout({ children }: { children: React.ReactNode }) {
  let me: { me: { id: string; orgId: string; role: string } };
  try {
    me = await query<{ me: { id: string; orgId: string; role: string } }>(
      '{ me { id orgId role } }',
    );
  } catch (err) {
    if (err instanceof NotAuthenticatedError) redirect('/login');
    throw err;
  }

  return (
    <div className={styles.shell}>
      <aside className={styles.sidebar}>
        <Nav role={me.me.role} />
      </aside>
      <main className={styles.content}>
        <div className={styles.topbar}>
          <span className={styles.user} title={`user_id ${me.me.id}`}>
            {me.me.role}
          </span>
          <form action={logout}>
            <button className={styles.signout} type="submit">
              Sign out
            </button>
          </form>
        </div>
        {children}
      </main>
    </div>
  );
}
