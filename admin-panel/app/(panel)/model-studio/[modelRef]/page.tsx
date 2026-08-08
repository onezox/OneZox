import Link from 'next/link';

import { query } from '@/lib/graphql';
import { PageHeader, Card, Badge, Mono, EmptyRow, toneForRolloutStatus } from '@/components/ui';
import { ModelEditor } from './ModelEditor';
import { StartRolloutForm, PromoteAbortForms } from './RolloutControls';

export const dynamic = 'force-dynamic';

interface ModelPageData {
  me: { role: string };
  modelDraft: {
    versionId: string;
    modelRef: string;
    specJson: string;
    signature: string;
    createdBy: string;
    createdAt: string;
    status: string;
  };
  rolloutByModel: {
    rolloutId: string;
    versionId: string;
    stage: string;
    status: string;
    canaryPercent: number;
    startedAt: string;
    endedAt: string | null;
  } | null;
}

const QUERY = `query ModelPage($modelRef: String!) {
  me { role }
  modelDraft(modelRef: $modelRef) {
    versionId modelRef specJson signature createdBy createdAt status
  }
  rolloutByModel(modelRef: $modelRef) {
    rolloutId versionId stage status canaryPercent startedAt endedAt
  }
}`;

/**
 * Model Studio, per model — Phase-05 Step U2.
 *
 * The full authoring loop: read the active manifest (createModelDraft),
 * edit it, see the byte-exact diff, publish a new signed version, then
 * canary it with live promote/abort controls.
 */
export default async function ModelPage({
  params,
}: {
  params: Promise<{ modelRef: string }>;
}) {
  const { modelRef } = await params;
  const data = await query<ModelPageData>(QUERY, { modelRef });

  const canMutate = data.me.role === 'admin';
  const active = data.modelDraft;
  const rollout = data.rolloutByModel;
  const running = rollout?.status === 'running';

  return (
    <>
      <PageHeader
        title={modelRef}
        description="Author a new manifest version, then stage it to live traffic by canary."
        actions={<Link href="/model-studio">← All models</Link>}
      />

      <Card title="Active version">
        <dl className="specList">
          <div>
            <dt>version_id</dt>
            <dd>
              <Mono>{active.versionId}</Mono>
            </dd>
          </div>
          <div>
            <dt>status</dt>
            <dd>
              <Badge tone="success">{active.status}</Badge>
            </dd>
          </div>
          <div>
            <dt>created_by</dt>
            <dd>
              <Mono>{active.createdBy}</Mono>
            </dd>
          </div>
          <div>
            <dt>created_at</dt>
            <dd>
              <Mono>{active.createdAt}</Mono>
            </dd>
          </div>
          <div>
            <dt>signature</dt>
            <dd>
              {/* Truncated: the full Vault Transit signature is long and
                  nothing here needs it verbatim — its presence is the
                  point, and every consumer re-verifies it independently. */}
              <Mono>{active.signature.slice(0, 28)}…</Mono>
            </dd>
          </div>
        </dl>
      </Card>

      <Card title="Edit &amp; publish">
        <ModelEditor
          modelRef={modelRef}
          activeSpecJson={active.specJson}
          canPublish={canMutate}
        />
      </Card>

      <Card title="Canary rollout">
        {rollout ? (
          <>
            <dl className="specList">
              <div>
                <dt>rollout_id</dt>
                <dd>
                  <Mono>{rollout.rolloutId}</Mono>
                </dd>
              </div>
              <div>
                <dt>version</dt>
                <dd>
                  <Mono>{rollout.versionId}</Mono>
                </dd>
              </div>
              <div>
                <dt>stage</dt>
                <dd>
                  <Mono>{rollout.stage}</Mono> · {rollout.canaryPercent}%
                </dd>
              </div>
              <div>
                <dt>status</dt>
                <dd>
                  <Badge tone={toneForRolloutStatus(rollout.status)}>{rollout.status}</Badge>
                </dd>
              </div>
              <div>
                <dt>started</dt>
                <dd>
                  <Mono>{rollout.startedAt}</Mono>
                </dd>
              </div>
              {rollout.endedAt ? (
                <div>
                  <dt>ended</dt>
                  <dd>
                    <Mono>{rollout.endedAt}</Mono>
                  </dd>
                </div>
              ) : null}
            </dl>

            {running ? (
              <PromoteAbortForms
                modelRef={modelRef}
                rolloutId={rollout.rolloutId}
                canMutate={canMutate}
              />
            ) : (
              <>
                <EmptyRow>
                  The most recent rollout is finished. Start a new one to stage
                  another version.
                </EmptyRow>
                <StartRolloutForm
                  modelRef={modelRef}
                  versions={[{ versionId: active.versionId, createdAt: active.createdAt }]}
                  canMutate={canMutate}
                />
              </>
            )}
          </>
        ) : (
          <>
            <EmptyRow>This model has never been canaried.</EmptyRow>
            <StartRolloutForm
              modelRef={modelRef}
              versions={[{ versionId: active.versionId, createdAt: active.createdAt }]}
              canMutate={canMutate}
            />
          </>
        )}
      </Card>
    </>
  );
}
