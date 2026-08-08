import Link from 'next/link';

import { query } from '@/lib/graphql';
import { PageHeader, Card, Table, Badge, Mono, EmptyRow, toneForRolloutStatus } from '@/components/ui';

export const dynamic = 'force-dynamic';

interface RolloutsData {
  rollouts: Array<{
    rolloutId: string;
    modelRef: string;
    versionId: string;
    stage: string;
    status: string;
    canaryPercent: number;
    startedAt: string;
    endedAt: string | null;
  }>;
}

const QUERY = `{
  rollouts { rolloutId modelRef versionId stage status canaryPercent startedAt endedAt }
}`;

/**
 * Rollout history — Phase-05 Step U2.
 *
 * Every canary this cluster has run, most recent first, including how
 * each one ended. A rolled_back row is a rollback that the automatic
 * SLO gate performed (Step P) or that an operator triggered; the two
 * are distinguished by status — "rolled_back" is automatic,
 * "aborted" is a human abort.
 *
 * Backed by control-plane's ListRollouts RPC, not a database read:
 * admin-api has no grant on the rollout table at all (migration 0020).
 */
export default async function RolloutsPage() {
  const data = await query<RolloutsData>(QUERY);

  return (
    <>
      <PageHeader
        title="Rollouts"
        description="Every canary, most recent first. Automatic rollbacks appear as rolled_back; operator-triggered ones as aborted."
      />

      <Card>
        {data.rollouts.length === 0 ? (
          <EmptyRow>No rollout has ever run in this cluster.</EmptyRow>
        ) : (
          <Table head={['Model', 'Version', 'Stage', 'Canary %', 'Status', 'Started', 'Ended']}>
            {data.rollouts.map((r) => (
              <tr key={r.rolloutId}>
                <td>
                  <Link href={`/model-studio/${r.modelRef}`}>{r.modelRef}</Link>
                </td>
                <td>
                  <Mono>{r.versionId.slice(0, 8)}…</Mono>
                </td>
                <td>
                  <Mono>{r.stage}</Mono>
                </td>
                <td>{r.canaryPercent}%</td>
                <td>
                  <Badge tone={toneForRolloutStatus(r.status)}>{r.status}</Badge>
                </td>
                <td>
                  <Mono>{r.startedAt}</Mono>
                </td>
                <td>{r.endedAt ? <Mono>{r.endedAt}</Mono> : '—'}</td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
    </>
  );
}
