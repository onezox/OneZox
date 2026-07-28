#!/usr/bin/env bash
# Phase-01 Step H1 (Security, EC2): raw API key never appears in
# logs/traces.
#
# Adversarial by design: an "absent" result for the raw key is only
# trustworthy if the SAME query path can find something known to be
# present first. Without that positive control, a silently-broken query
# (wrong label name, too-narrow time window) gives a false "pass"
# indistinguishable from a genuine "never logged" result — both actually
# happened while developing this script (an initial 10-minute Loki window
# missed the boot log line entirely; widening it to the default here is
# the fix, not anything about the raw key). Every negative assertion below
# is preceded by a positive one against the identical query.
#
# Requires: kubectl port-forward to edge-gateway (8080), tempo (3200), and
# loki (3100) already running (see Phase-01-Local-Restart-Guide.txt
# section 4).
set -euo pipefail

RAW_KEY="${1:?usage: $0 <raw-api-key> [lookback-seconds]}"
LOOKBACK_SECONDS="${2:-3600}"

fail() { echo "FAIL: $1" >&2; exit 1; }

echo "=== making one fresh authenticated request ==="
RESPONSE=$(curl -sN -H "Authorization: Bearer ${RAW_KEY}" \
  -H "content-type: application/json" \
  -X POST http://localhost:8080/v1/chat/completions \
  -d '{"model":"onezox-ultra","messages":[{"role":"user","content":"security test H1"}],"stream":true}')
REQUEST_ID=$(echo "$RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | sed 's/"id":"//;s/"//')
[[ -n "$REQUEST_ID" ]] || fail "could not extract request_id from response — request itself may have failed"
echo "request_id: $REQUEST_ID"

echo
echo "=== Tempo ==="
echo "(waiting for the OTel batch span processor to flush and our trace to become searchable — retrying rather than a fixed sleep, since export timing varies)"
ALL_TRACES=""
FOUND=""
for i in $(seq 1 15); do
  SEARCH=$(curl -s "http://localhost:3200/api/search?tags=service.name%3Dedge-gateway&limit=50")
  TRACE_IDS=$(echo "$SEARCH" | python3 -c "import json,sys; d=json.load(sys.stdin); print('\n'.join(t['traceID'] for t in d.get('traces', [])))" 2>/dev/null || true)
  ALL_TRACES=""
  for tid in $TRACE_IDS; do
    ALL_TRACES+=$(curl -s "http://localhost:3200/api/traces/${tid}")
  done
  if echo "$ALL_TRACES" | grep -q "$REQUEST_ID"; then
    FOUND=1
    break
  fi
  sleep 2
done
[[ -n "$TRACE_IDS" ]] || fail "Tempo search returned no traces at all for service.name=edge-gateway after waiting — query itself appears broken"
[[ -n "$FOUND" ]] \
  && echo "PASS (positive control): our request_id ($REQUEST_ID) found in Tempo — query path has teeth" \
  || fail "positive control failed: our own request_id never appeared in Tempo traces after waiting — query is broken, an 'absent' result for the raw key would be meaningless"

if echo "$ALL_TRACES" | grep -qF "$RAW_KEY"; then
  fail "raw key material found in Tempo traces!"
fi
echo "PASS (negative assertion): raw key not found in Tempo traces"

echo
echo "=== Loki ==="
LOKI_RESULT=$(curl -sG "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={app="edge-gateway"}' \
  --data-urlencode 'limit=1000' \
  --data-urlencode "start=$(( $(date -u +%s) - LOOKBACK_SECONDS ))000000000" \
  --data-urlencode "end=$(date -u +%s)000000000")

LINE_COUNT=$(echo "$LOKI_RESULT" | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(len(r['values']) for r in d['data']['result']))")
[[ "$LINE_COUNT" -gt 0 ]] || fail "Loki query returned zero log lines over the last ${LOOKBACK_SECONDS}s for {app=\"edge-gateway\"} — query is broken (wrong label or window too narrow), an 'absent' result for the raw key would be meaningless"
echo "PASS (positive control): Loki query found $LINE_COUNT log line(s) — query path has teeth"

if echo "$LOKI_RESULT" | grep -qF "$RAW_KEY"; then
  fail "raw key material found in Loki logs!"
fi
echo "PASS (negative assertion): raw key not found in Loki logs"

echo
echo "=== H1 PASSED: raw key never appears in logs/traces (query path independently verified as functional) ==="
