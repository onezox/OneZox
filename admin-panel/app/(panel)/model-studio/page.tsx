import Link from 'next/link';

import { query } from '@/lib/graphql';
import { PageHeader, Card, Table, Mono, Badge, toneForRolloutStatus } from '@/components/ui';

export const dynamic = 'force-dynamic';

interface ModelsData {
  models: Array<{ modelRef: string; activeVersionId: string }>;
  rollouts: Array<{ modelRef: string; status: string; stage: string; canaryPercent: number }>;
}

const QUERY = `{
  models { modelRef activeVersionId }
  rollouts { modelRef status stage canaryPercent }
}`;

/**
 * Model Studio index — Phase-05 Step U2.
 *
 * Lists every registered virtual model with its live active version and
 * whether a canary is currently in flight for it. Authoring happens on
 * the per-model page.
 */
export default async function ModelStudioPage() {
  const data = await query<ModelsData>(QUERY);

  // Only a RUNNING rollout is "in flight" — a promoted or rolled-back
  // one is history and belongs on /rollouts, not as a live badge here.
  const running = new Map(
    data.rollouts.filter((r) => r.status === 'running').map((r) => [r.modelRef, r]),
  );

  return (
    <>
      <PageHeader
        title="Model Studio"
        description="Author and version virtual-model manifests, then roll them out by canary. Every published version is signed by control-plane and immutable."
      />

      <Card>
        <Table head={['Model', 'Active version', 'Rollout in flight', '']}>
          {data.models.map((m) => {
            const r = running.get(m.modelRef);
            return (
              <tr key={m.modelRef}>
                <td>
                  <strong>{m.modelRef}</strong>
                </td>
                <td>
                  <Mono>{m.activeVersionId}</Mono>
                </td>
                <td>
                  {r ? (
                    <Badge tone={toneForRolloutStatus(r.status)}>
                      {r.stage} · {r.canaryPercent}%
                    </Badge>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  )}
                </td>
                <td>
                  <Link href={`/model-studio/${m.modelRef}`}>Open</Link>
                </td>
              </tr>
            );
          })}
        </Table>
      </Card>
    </>
  );
}
