import { query } from '@/lib/graphql';
import { PageHeader, Card, Table, Badge, Mono, EmptyRow } from '@/components/ui';

export const dynamic = 'force-dynamic';

interface AuditData {
  auditLog: Array<{
    auditId: string;
    actor: string;
    action: string;
    target: string;
    beforeJson: string | null;
    afterJson: string | null;
    ts: string;
  }>;
}

const QUERY = `query Audit($limit: Int) {
  auditLog(limit: $limit) { auditId actor action target beforeJson afterJson ts }
}`;

/**
 * Audit — Phase-05 Step U2.
 *
 * Read-only by construction, not by choice of what to render: audit_log
 * has no UPDATE or DELETE grant for admin_api at all (migration 0018),
 * which Step Q proved adversarially at the database itself. There is no
 * edit control here because there is no code path anywhere in this
 * system that could back one.
 *
 * Denied attempts appear alongside successes — a refused escalation
 * attempt is exactly the kind of thing this log exists to record.
 */
function actionTone(action: string): 'success' | 'danger' | 'warn' | 'neutral' {
  if (action.endsWith('_denied')) return 'danger';
  if (action.endsWith('_failed')) return 'warn';
  return 'success';
}

export default async function AuditPage() {
  const data = await query<AuditData>(QUERY, { limit: 100 });

  return (
    <>
      <PageHeader
        title="Audit"
        description="Every admin action, successful or refused. Append-only — no path exists to modify or delete a row, including from here."
      />

      <Card>
        {data.auditLog.length === 0 ? (
          <EmptyRow>No admin actions recorded yet.</EmptyRow>
        ) : (
          <Table
            head={['When', 'Actor', 'Action', 'Target', 'After']}
            caption="Audit log, most recent first"
          >
            {data.auditLog.map((e) => (
              <tr key={e.auditId}>
                <td>
                  <Mono>{e.ts}</Mono>
                </td>
                <td>
                  <Mono>{e.actor.slice(0, 8)}…</Mono>
                </td>
                <td>
                  <Badge tone={actionTone(e.action)}>{e.action}</Badge>
                </td>
                <td>
                  <Mono>{e.target}</Mono>
                </td>
                <td style={{ whiteSpace: 'normal', maxWidth: '38ch' }}>
                  {e.afterJson ? (
                    <Mono>{e.afterJson}</Mono>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  )}
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
    </>
  );
}
