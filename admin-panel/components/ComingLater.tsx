import styles from './ComingLater.module.css';

/**
 * The honest inert state — Phase-05 Step U2 / F11.
 *
 * traces, cost and playground are SCAFFOLDED, NOT IMPLEMENTED this
 * phase: their data sources (ClickHouse traces + cost rollups, P13; the
 * eval platform, P12) genuinely do not exist yet. This component is what
 * those routes render instead.
 *
 * It deliberately shows NO chart, NO table, NO sample numbers, and no
 * "loading" affordance. A mock chart or lorem-ipsum row would make an
 * empty subsystem look like a working one, and an operator cannot tell a
 * convincing placeholder from real data — which in a console that also
 * shows REAL rollout and provider state is a genuine hazard, not a
 * cosmetic one. Naming the phase that fills it in is the useful thing to
 * show; fabricated completeness is not.
 */
export function ComingLater({
  title,
  phase,
  what,
}: {
  title: string;
  phase: string;
  what: string;
}) {
  return (
    <section className={styles.wrap} aria-labelledby="inert-heading">
      <p className={styles.badge}>Not implemented yet</p>
      <h2 id="inert-heading" className={styles.title}>
        {title}
      </h2>
      <p className={styles.body}>{what}</p>
      <p className={styles.meta}>
        This section is scaffolded so the route and navigation exist, but it is
        wired to no data source. It populates in <strong>{phase}</strong>.
      </p>
    </section>
  );
}
