import { query } from '@/lib/graphql';
import {
  PageHeader,
  StatGrid,
  StatTile,
  Card,
  Table,
  Badge,
  EmptyRow,
  Mono,
  toneForRolloutStatus,
} from '@/components/ui';

export const dynamic = 'force-dynamic';

interface DashboardData {
  dashboardMetrics: {
    requestsPerSecond: number;
    p95LatencyMs: number;
    errorRate: number;
    activeModelsCount: number;
    activeRolloutsCount: number;
  };
  rollouts: Array<{
    rolloutId: string;
    modelRef: string;
    stage: string;
    status: string;
    canaryPercent: number;
    startedAt: string;
  }>;
  providerHealth: Array<{ provider: string; healthy: boolean; breakerState: string }>;
}

const QUERY = `{
  dashboardMetrics { requestsPerSecond p95LatencyMs errorRate activeModelsCount activeRolloutsCount }
  rollouts { rolloutId modelRef stage status canaryPercent startedAt }
  providerHealth { provider healthy breakerState }
}`;

/**
 * Dashboard — Phase-05 Step U2.
 *
 * REAL data: three Prometheus-backed SLO numbers, live model/rollout
 * counts, in-flight rollouts, and provider breaker state. This is
 * Architecture Part R's dashboard scoped to what genuinely exists —
 * its ClickHouse near-real-time panels and Redpanda WS live tail need
 * F6/P13 and are deliberately absent rather than mocked.
 */
export default async function DashboardPage() {
  const data = await query<DashboardData>(QUERY);
  const m = data.dashboardMetrics;

  const inFlight = data.rollouts.filter((r) => r.status === 'running');
  const unhealthy = data.providerHealth.filter((p) => !p.healthy);

  return (
    <>
      <PageHeader
        title="Dashboard"
        description="Live serving health and control-plane state. Traces, spend and eval panels arrive with their own phases."
      />

      <StatGrid>
        <StatTile
          label="Requests / sec"
          value={m.requestsPerSecond.toFixed(2)}
          hint="edge-gateway, 5m rate"
        />
        <StatTile
          label="p95 latency"
          value={`${m.p95LatencyMs.toFixed(0)} ms`}
          hint="edge-gateway, 5m"
        />
        <StatTile
          label="Error rate"
          value={`${(m.errorRate * 100).toFixed(2)}%`}
          hint="5xx share, 5m"
        />
        <StatTile label="Active models" value={String(m.activeModelsCount)} />
        <StatTile label="Rollouts in flight" value={String(m.activeRolloutsCount)} />
      </StatGrid>

      <Card title="Rollouts in flight">
        {inFlight.length === 0 ? (
          <EmptyRow>No canary is in progress.</EmptyRow>
        ) : (
          <Table head={['Model', 'Stage', 'Canary %', 'Status', 'Started']}>
            {inFlight.map((r) => (
              <tr key={r.rolloutId}>
                <td>{r.modelRef}</td>
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
              </tr>
            ))}
          </Table>
        )}
      </Card>

      <Card title="Provider health">
        {unhealthy.length > 0 ? (
          <p>
            {unhealthy.length} of {data.providerHealth.length} providers are not
            in a healthy state.
          </p>
        ) : (
          <p>All {data.providerHealth.length} providers report a closed circuit breaker.</p>
        )}
        <Table head={['Provider', 'Breaker', 'Healthy']}>
          {data.providerHealth.map((p) => (
            <tr key={p.provider}>
              <td>{p.provider}</td>
              <td>
                <Mono>{p.breakerState}</Mono>
              </td>
              <td>
                <Badge tone={p.healthy ? 'success' : 'danger'}>
                  {p.healthy ? 'healthy' : 'degraded'}
                </Badge>
              </td>
            </tr>
          ))}
        </Table>
      </Card>
    </>
  );
}
