'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import styles from './Nav.module.css';

/**
 * Panel navigation — Phase-05 Step U2 / Architecture Part R's own
 * section list.
 *
 * A Client Component solely because it highlights the active route via
 * usePathname(). It receives NOTHING sensitive: the role string is used
 * to gate which controls are shown, and the session token never reaches
 * this component or any other browser-side code (lib/session.ts is
 * server-only, enforced at build time).
 *
 * The `inert` flag is a first-class part of the nav, not a styling
 * afterthought: an operator should be able to tell which sections
 * carry real data BEFORE clicking into them (F11).
 */
const SECTIONS: Array<{ href: string; label: string; inert?: boolean }> = [
  { href: '/dashboard', label: 'Dashboard' },
  { href: '/model-studio', label: 'Model Studio' },
  { href: '/rollouts', label: 'Rollouts' },
  { href: '/keys', label: 'Keys' },
  { href: '/providers', label: 'Providers' },
  { href: '/audit', label: 'Audit' },
  { href: '/traces', label: 'Traces', inert: true },
  { href: '/cost', label: 'Cost', inert: true },
  { href: '/playground', label: 'Playground', inert: true },
];

export function Nav({ role }: { role: string }) {
  const pathname = usePathname();

  return (
    <nav className={styles.nav} aria-label="Panel sections">
      <div className={styles.brand}>
        <span className={styles.brandMark}>OneZox</span>
        <span className={styles.brandSub}>admin</span>
      </div>

      <ul className={styles.list}>
        {SECTIONS.map((s) => {
          const active = pathname === s.href || pathname.startsWith(s.href + '/');
          return (
            <li key={s.href}>
              <Link
                href={s.href}
                className={`${styles.link} ${active ? styles.active : ''}`}
                aria-current={active ? 'page' : undefined}
              >
                <span>{s.label}</span>
                {s.inert ? (
                  <span className={styles.inertTag} title="Populates in a later phase">
                    later
                  </span>
                ) : null}
              </Link>
            </li>
          );
        })}
      </ul>

      <div className={styles.footer}>
        <span className={styles.roleLabel}>signed in as</span>
        <span className={styles.role}>{role}</span>
      </div>
    </nav>
  );
}
