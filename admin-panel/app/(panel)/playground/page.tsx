import { PageHeader } from '@/components/ui';
import { ComingLater } from '@/components/ComingLater';

/**
 * Playground — SCAFFOLDED, NOT IMPLEMENTED (F11).
 *
 * Part R's Eval Center (golden sets, shadow-traffic results, A/B
 * outcomes, regression-gate history) is the eval platform's own surface
 * and arrives with Phase-12. Nothing in this phase can populate it.
 */
export default function PlaygroundPage() {
  return (
    <>
      <PageHeader title="Playground" />
      <ComingLater
        title="Playground &amp; Eval Center"
        phase="Phase-12"
        what="Interactive request playground alongside golden sets, shadow-traffic results, quality-drift alerts and regression-gate history."
      />
    </>
  );
}
