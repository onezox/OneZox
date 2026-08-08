import { PageHeader } from '@/components/ui';
import { ComingLater } from '@/components/ComingLater';

/**
 * Cost Center — SCAFFOLDED, NOT IMPLEMENTED (F11).
 *
 * Spend attribution needs usage_event.usd_cost populated, which is a
 * carried deferral to Phase-06, and the rollups Part R describes need
 * ClickHouse (Phase-13). Showing a plausible-looking spend chart backed
 * by nothing would be worse than showing nothing.
 */
export default function CostPage() {
  return (
    <>
      <PageHeader title="Cost" />
      <ComingLater
        title="Cost Center"
        phase="Phase-13 (with usd_cost wiring in Phase-06)"
        what="Spend by model, provider, tenant and role, with the orchestration-token breakdown and optimizer suggestions."
      />
    </>
  );
}
