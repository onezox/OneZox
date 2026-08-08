import styles from './ui.module.css';

/**
 * The small shared vocabulary every real panel section is built from —
 * Phase-05 Step U2. Functional-first: enough structure to read dense
 * operational data clearly, no decoration beyond that.
 *
 * All Server Components (no 'use client'): none of these hold state or
 * handle events, so none of them needs to ship JavaScript to the
 * browser. That is the RSC default working as intended, and it keeps
 * the client bundle limited to the few genuinely interactive controls.
 */

export function PageHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description?: string;
  actions?: React.ReactNode;
}) {
  return (
    <header className={styles.pageHeader}>
      <div>
        <h1 className={styles.pageTitle}>{title}</h1>
        {description ? <p className={styles.pageDescription}>{description}</p> : null}
      </div>
      {actions ? <div className={styles.pageActions}>{actions}</div> : null}
    </header>
  );
}

export function Card({
  title,
  children,
  footer,
}: {
  title?: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
}) {
  return (
    <section className={styles.card}>
      {title ? <h2 className={styles.cardTitle}>{title}</h2> : null}
      {children}
      {footer ? <div className={styles.cardFooter}>{footer}</div> : null}
    </section>
  );
}

export function StatTile({
  label,
  value,
  hint,
}: {
  label: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className={styles.stat}>
      <div className={styles.statLabel}>{label}</div>
      <div className={styles.statValue}>{value}</div>
      {hint ? <div className={styles.statHint}>{hint}</div> : null}
    </div>
  );
}

export function StatGrid({ children }: { children: React.ReactNode }) {
  return <div className={styles.statGrid}>{children}</div>;
}

export type Tone = 'neutral' | 'accent' | 'success' | 'warn' | 'danger';

export function Badge({ tone = 'neutral', children }: { tone?: Tone; children: React.ReactNode }) {
  return <span className={`${styles.badge} ${styles[tone]}`}>{children}</span>;
}

/**
 * toneForRolloutStatus / toneForStage keep the rollout vocabulary
 * consistent everywhere it appears. Defined once so the dashboard,
 * Model Studio and the rollout history can never disagree about what
 * colour "rolled_back" is.
 */
export function toneForRolloutStatus(status: string): Tone {
  switch (status) {
    case 'running':
      return 'accent';
    case 'promoted':
      return 'success';
    case 'rolled_back':
      return 'danger';
    case 'aborted':
      return 'warn';
    default:
      return 'neutral';
  }
}

export function EmptyRow({ children }: { children: React.ReactNode }) {
  return <p className={styles.empty}>{children}</p>;
}

export function ErrorNote({ children }: { children: React.ReactNode }) {
  return (
    <p className={styles.error} role="alert">
      {children}
    </p>
  );
}

export function Mono({ children }: { children: React.ReactNode }) {
  return <span className={styles.mono}>{children}</span>;
}

export function Table({
  head,
  children,
  caption,
}: {
  head: string[];
  children: React.ReactNode;
  caption?: string;
}) {
  return (
    <div className={styles.tableWrap}>
      <table className={styles.table}>
        {caption ? <caption className="srOnly">{caption}</caption> : null}
        <thead>
          <tr>
            {head.map((h) => (
              <th key={h} scope="col">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}
