import { query } from '@/lib/graphql';
import { PageHeader, Card, Table, Badge, Mono, EmptyRow } from '@/components/ui';

export const dynamic = 'force-dynamic';

interface ProvidersData {
  providerHealth: Array<{
    provider: string;
    healthy: boolean;
    quotaHeadroom: number;
    breakerState: string;
  }>;
}

const QUERY = `{ providerHealth { provider healthy quotaHeadroom breakerState } }`;

function breakerTone(state: string): 'success' | 'warn' | 'danger' | 'neutral' {
  switch (state) {
    case 'closed':
      return 'success';
    case 'half_open':
      return 'warn';
    case 'open':
      return 'danger';
    default:
      return 'neutral';
  }
}

/**
 * Provider Console — Phase-05 Step U2 / Architecture Part R.
 *
 * Live circuit-breaker state and quota headroom, read straight from
 * provider-gateway's own ProviderHealth RPC (Phase-02, unchanged).
 *
 * Provider CREDENTIALS are deliberately absent from this page. They
 * live in Vault and are fetched by provider-gateway alone via short-
 * lived scoped tokens (Phase-04 F9); the panel has no Vault client, no
 * credential RPC, and nothing to display. Part R lists "keys
 * (Vault-backed)" under this console — surfacing their existence and
 * rotation state belongs to a phase that builds that read path, not to
 * a page that would have to invent one.
 */
export default async function ProvidersPage() {
  const data = await query<ProvidersData>(QUERY);

  return (
    <>
      <PageHeader
        title="Providers"
        description="Circuit-breaker state and quota headroom per provider. Credentials are held in Vault and fetched only by provider-gateway — they are never exposed here."
      />

      <Card>
        {data.providerHealth.length === 0 ? (
          <EmptyRow>provider-gateway reported no registered providers.</EmptyRow>
        ) : (
          <Table head={['Provider', 'Breaker', 'Quota headroom', 'Status']}>
            {data.providerHealth.map((p) => (
              <tr key={p.provider}>
                <td>
                  <strong>{p.provider}</strong>
                </td>
                <td>
                  <Badge tone={breakerTone(p.breakerState)}>{p.breakerState}</Badge>
                </td>
                <td>
                  <Mono>{(p.quotaHeadroom * 100).toFixed(0)}%</Mono>
                </td>
                <td>
                  <Badge tone={p.healthy ? 'success' : 'danger'}>
                    {p.healthy ? 'healthy' : 'degraded'}
                  </Badge>
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>

      <Card title="How health is derived">
        <p style={{ margin: 0, color: 'var(--text-muted)' }}>
          A provider counts as healthy only when its circuit breaker is{' '}
          <Mono>closed</Mono>. <Mono>half_open</Mono> is a probationary state —
          the breaker is retrying but has not yet confirmed recovery — so it is
          reported as degraded rather than healthy.
        </p>
      </Card>
    </>
  );
}
