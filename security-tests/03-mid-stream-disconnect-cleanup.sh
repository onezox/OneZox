#!/usr/bin/env bash
# Phase-01 Step H3 (Security, EC2): "client drop mid-stream frees
# resources cleanly" (Phase-01.txt testing requirement) — the mid-stream
# KILL case specifically, not clean completion. Step G already proved a
# stream that runs to natural completion drains the admission gauge to 0;
# that's a different, weaker claim than this one. This measures the real
# deployed edge-gateway's own Tokio task / Drop-based cleanup on an
# abrupt disconnect — not "goroutines" (that's the Go provider-stub's
# language, and the wrong process for this check).
#
# Adversarial by construction: checking the gauge only *after* the kill
# can't distinguish "cleanup ran correctly on disconnect" from "the guard
# was dropped too early in the first place" (e.g. a regression that drops
# AdmissionGuard right after admission instead of holding it for the
# stream's lifetime) — both would show 0 at that point. This script checks
# the gauge *during* the still-open stream first (must be 1, proving the
# guard is genuinely held), only THEN kills the connection and confirms
# the gauge returns to 0. It also confirms the received partial response
# never contains the final chunk's finish_reason, ruling out a race where
# the "kill" just happened to land after natural completion.
#
# Requires: kubectl port-forward to edge-gateway (8080) already running.
set -euo pipefail

RAW_KEY="${1:?usage: $0 <raw-api-key> [redis-pod]}"
REDIS_POD="${2:-redis-cluster-0}"
GAUGE_KEY="admission:cell-local:inflight"
OUT_FILE=$(mktemp)
trap 'rm -f "$OUT_FILE"' EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

# -c: cluster mode, follows MOVED redirects automatically — any node can
# be queried regardless of which shard actually owns this key's slot.
gauge() { kubectl exec -n default "$REDIS_POD" -- redis-cli -c GET "$GAUGE_KEY" 2>/dev/null | tr -d '\r'; }

BASELINE=$(gauge)
[[ "$BASELINE" == "0" || -z "$BASELINE" ]] || fail "baseline gauge is '$BASELINE', not 0 — a previous test/request is still in flight; re-run once it settles"

echo "=== starting a streaming request and killing it mid-stream ==="
curl -sN -H "Authorization: Bearer ${RAW_KEY}" \
  -H "content-type: application/json" \
  -X POST http://localhost:8080/v1/chat/completions \
  -d '{"model":"onezox-ultra","messages":[{"role":"user","content":"H3 disconnect test"}],"stream":true}' \
  > "$OUT_FILE" 2>&1 &
CURL_PID=$!

sleep 0.03
DURING=$(gauge)
kill -9 "$CURL_PID" 2>/dev/null || true
wait "$CURL_PID" 2>/dev/null || true

[[ "$DURING" == "1" ]] || fail "gauge was '$DURING' while the stream was still open, expected 1 — either the guard is being dropped before/without the stream actually starting, or something else is off; a 0 here would make the 'returns to 0 after disconnect' check below meaningless (indistinguishable from a guard dropped too early)"
echo "PASS (positive control): gauge was 1 while the stream was genuinely still open — the guard is held, not dropped early"

if grep -q '"finish_reason":"stop"' "$OUT_FILE"; then
  fail "the received response already contained the final chunk (finish_reason:stop) before we killed it — this was a race with natural completion, not a genuine mid-stream disconnect; re-run (the kill needs to land before dataplane-stub's ~300ms stream finishes)"
fi
echo "PASS: confirmed genuinely mid-stream (no finish_reason:stop in the partial response received before the kill)"

echo
echo "=== waiting for server-side cleanup after the abrupt disconnect ==="
FINAL=""
for i in $(seq 1 15); do
  FINAL=$(gauge)
  [[ "$FINAL" == "0" || -z "$FINAL" ]] && break
  sleep 0.3
done
[[ "$FINAL" == "0" || -z "$FINAL" ]] || fail "gauge is still '$FINAL' after waiting — resources were not freed after the client disconnected mid-stream"
echo "PASS: gauge returned to 0 after the mid-stream kill — edge-gateway's own task/Drop cleanup ran correctly on abrupt disconnect"

echo
echo "=== H3 PASSED: mid-stream client disconnect frees resources cleanly ==="
