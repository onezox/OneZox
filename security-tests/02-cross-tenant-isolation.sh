#!/usr/bin/env bash
# Phase-01 Step H2 (Security, EC2): a cross-tenant key cannot access
# another tenant's resources — specifically, rate-limit headroom.
#
# Behavioral, not structural, per explicit direction: this drives tenant A
# to its actual rate limit and confirms tenant B's own limit is completely
# unaffected, rather than just observing that ratelimit.rs uses
# {org_id}-scoped Redis keys (which proves the *intent* of the code, not
# that it behaves correctly under real load).
#
# Adversarial: before trusting "tenant B is unaffected," this first proves
# tenant A is genuinely, verifiably exhausted (one more request after the
# main loop still gets 429) — otherwise "B is fine" is an unsurprising,
# uninteresting result (of course two both non-exhausted tenants don't
# interfere). It also re-checks tenant A a second time, after B's request,
# to rule out any cross-contamination where B's success somehow affected
# A's state.
#
# Requires: kubectl port-forward to edge-gateway (8080) already running.
# Requires: KEY_A and KEY_B for two tenants sharing the same rpm (both
# default to 60 via data/seed/seed-test-tenant.sh).
set -euo pipefail

KEY_A="${1:?usage: $0 <key-a> <key-b> [rpm]}"
KEY_B="${2:?usage: $0 <key-a> <key-b> [rpm]}"
RPM="${3:-60}"

fail() { echo "FAIL: $1" >&2; exit 1; }

status_for() {
  curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $1" http://localhost:8080/v1/models
}

# Avoid starting near a 60s window boundary — a mid-test rollover would
# reset tenant A's counter and produce a false negative (A never actually
# gets exhausted within this run).
SECONDS_INTO_MINUTE=$(( $(date -u +%s) % 60 ))
if (( SECONDS_INTO_MINUTE > 40 )); then
  WAIT=$(( 60 - SECONDS_INTO_MINUTE + 1 ))
  echo "close to a rate-limit window boundary (${SECONDS_INTO_MINUTE}s in) — waiting ${WAIT}s for a fresh window"
  sleep "$WAIT"
fi

echo "=== driving tenant A to its rate limit (rpm=$RPM) ==="
for i in $(seq 1 "$RPM"); do
  code=$(status_for "$KEY_A")
  if [[ "$code" != "200" ]]; then
    fail "tenant A request $i/$RPM got $code, expected 200 — either rpm has changed, a previous run already consumed this window, or something else is wrong before we even reach the interesting part of this test"
  fi
done
echo "tenant A: all $RPM requests within its limit succeeded (200)"

echo
echo "=== positive control: confirm tenant A is now GENUINELY exhausted ==="
code=$(status_for "$KEY_A")
[[ "$code" == "429" ]] || fail "tenant A's $(($RPM + 1))th request got $code, expected 429 — A was never actually driven to its limit, so 'tenant B is unaffected' below would prove nothing"
echo "PASS: tenant A request $(($RPM + 1)) correctly rejected (429) — exhaustion is real"

echo
echo "=== the actual test: tenant B's headroom, immediately after A's exhaustion ==="
code=$(status_for "$KEY_B")
[[ "$code" == "200" ]] || fail "tenant B got $code instead of 200 right after tenant A was exhausted — cross-tenant isolation is broken"
echo "PASS: tenant B request succeeded (200) — completely unaffected by tenant A's exhaustion"

echo
echo "=== re-check: tenant A is still exhausted after B's request ==="
code=$(status_for "$KEY_A")
[[ "$code" == "429" ]] || fail "tenant A unexpectedly got $code (expected 429) after tenant B's request — B's request affected A's state, isolation is broken in the other direction"
echo "PASS: tenant A still rejected (429) — B's request did not touch A's counter"

echo
echo "=== H2 PASSED: cross-tenant rate-limit isolation confirmed behaviorally ==="
