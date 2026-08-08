import { PageHeader } from '@/components/ui';
import { ComingLater } from '@/components/ComingLater';

/**
 * Trace Explorer — SCAFFOLDED, NOT IMPLEMENTED (F11).
 *
 * Architecture Part R specifies a per-request agent waterfall with
 * per-node context, tool calls, tokens, cost and eval score, plus
 * one-click replay. That reads from ClickHouse, which does not exist
 * until P13. This route exists so navigation is complete; it is wired
 * to nothing, and says so.
 */
export default function TracesPage() {
  return (
    <>
      <PageHeader title="Traces" />
      <ComingLater
        title="Trace Explorer"
        phase="Phase-13"
        what="Per-request agent waterfall — every node, the context it saw, tool calls, tokens, cost and eval score, with one-click replay."
      />
    </>
  );
}
