package analysis

import (
	"strings"
	"testing"
)

// Post-M2 fix: the numerator MUST carry `or vector(0)`.
//
// Without it, a canary with zero errors matches no status="error" series
// at all, sum() returns an empty vector, and Argo's Prometheus provider
// fails evaluating result[0] ("reflect: slice index out of range"). That
// terminalizes the AnalysisRun as Error and the reconciler rolls back —
// so a PERFECTLY HEALTHY canary could never auto-promote, while one that
// had already errored at least once evaluated fine. Found live: a
// zero-error canary rolled back at canary_10 with measurements
// [None, None, None, None] while the denominator read a healthy
// 0.049/s.
//
// This is a shape assertion rather than a behavioural one, because the
// behaviour lives in Prometheus. It exists so the idiom cannot be
// "tidied up" out of the query without a test failing.
func TestQueryForDefaultsAnEmptyErrorNumeratorToZero(t *testing.T) {
	q := queryFor("openai")

	if !strings.Contains(q, "or vector(0)") {
		t.Fatalf("query lost its `or vector(0)` guard; a zero-error canary will fail to evaluate:\n%s", q)
	}

	// The guard must wrap the ERROR numerator, not the denominator.
	// On the denominator it would turn "no canary traffic at all" into a
	// division by a substituted zero, which would read as a real verdict
	// instead of staying NaN -> Inconclusive -> fail-safe.
	num, den, ok := strings.Cut(q, " / ")
	if !ok {
		t.Fatalf("query is not a single division; cannot verify guard placement:\n%s", q)
	}
	if !strings.Contains(num, `status="error"`) {
		t.Fatalf("numerator is not the error selector:\n%s", num)
	}
	if !strings.Contains(num, "or vector(0)") {
		t.Fatalf("`or vector(0)` is not on the error numerator:\n%s", num)
	}
	if strings.Contains(den, "or vector(0)") {
		t.Fatalf("`or vector(0)` must NOT be on the denominator; it would mask a no-data window:\n%s", den)
	}
	if strings.Count(q, "or vector(0)") != 1 {
		t.Fatalf("expected exactly one `or vector(0)`, on the numerator:\n%s", q)
	}
}

// The model_ref must be interpolated into BOTH selectors — a query that
// filtered only one side would compare one model's errors against every
// model's traffic.
func TestQueryForScopesBothSidesToTheModelRef(t *testing.T) {
	q := queryFor("cas-proof")
	if got := strings.Count(q, `model_ref="cas-proof"`); got != 2 {
		t.Fatalf("model_ref appears %d times, want 2 (numerator and denominator):\n%s", got, q)
	}
	if strings.Count(q, `canary="true"`) != 2 {
		t.Fatalf("both sides must be scoped to the canary population:\n%s", q)
	}
}
