import { query } from '@/lib/graphql';
import { PageHeader, Card, Table, Badge, Mono, EmptyRow } from '@/components/ui';
import { CreateKeyForm, RevokeKeyButton } from './KeyForms';

export const dynamic = 'force-dynamic';

interface KeysData {
  me: { role: string };
  apiKeys: Array<{
    keyId: string;
    orgId: string;
    scopes: string[];
    createdAt: string;
    revokedAt: string | null;
  }>;
}

const QUERY = `{
  me { role }
  apiKeys { keyId orgId scopes createdAt revokedAt }
}`;

/**
 * Tenant API keys — Phase-05 Step U2.
 *
 * These are TENANT credentials (api_keys, Phase-01), managed from the
 * admin surface — structurally disjoint from the admin_user credential
 * this panel session itself uses. An operator creating a key here is
 * not creating another admin.
 *
 * The list shows metadata only. No hash column is ever fetched from the
 * database (Step S), and ApiKeySummary has no field that could carry
 * one, so there is nothing here to leak.
 */
export default async function KeysPage() {
  const data = await query<KeysData>(QUERY);
  const canMutate = data.me.role === 'admin';

  const active = data.apiKeys.filter((k) => !k.revokedAt);
  const revoked = data.apiKeys.filter((k) => k.revokedAt);

  return (
    <>
      <PageHeader
        title="Tenant API keys"
        description="Keys tenants use at the edge. Only a SHA-256 hash is stored — the raw key is shown once at creation and is never recoverable."
      />

      <Card title="Create a key">
        <CreateKeyForm canMutate={canMutate} />
      </Card>

      <Card title={`Active keys (${active.length})`}>
        {active.length === 0 ? (
          <EmptyRow>No active keys.</EmptyRow>
        ) : (
          <Table head={['key_id', 'org_id', 'Scopes', 'Created', '']}>
            {active.map((k) => (
              <tr key={k.keyId}>
                <td>
                  <Mono>{k.keyId}</Mono>
                </td>
                <td>
                  <Mono>{k.orgId}</Mono>
                </td>
                <td style={{ whiteSpace: 'normal' }}>
                  {k.scopes.length ? k.scopes.join(', ') : <span style={{ color: 'var(--text-muted)' }}>none</span>}
                </td>
                <td>
                  <Mono>{k.createdAt}</Mono>
                </td>
                <td>
                  <RevokeKeyButton keyId={k.keyId} canMutate={canMutate} />
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>

      <Card title={`Revoked keys (${revoked.length})`}>
        {revoked.length === 0 ? (
          <EmptyRow>No revoked keys.</EmptyRow>
        ) : (
          <Table head={['key_id', 'org_id', 'Created', 'Revoked']}>
            {revoked.map((k) => (
              <tr key={k.keyId}>
                <td>
                  <Mono>{k.keyId}</Mono>
                </td>
                <td>
                  <Mono>{k.orgId}</Mono>
                </td>
                <td>
                  <Mono>{k.createdAt}</Mono>
                </td>
                <td>
                  <Badge tone="danger">{k.revokedAt}</Badge>
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
    </>
  );
}
