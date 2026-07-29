#!/usr/bin/env bash
# Recovers Redis Cluster from the WSL2-restart gossip-state drift that has
# now hit this local onezox-dev cluster three times (Phase-01 Step A, the
# promtail staleness noted during Step J, and again after this session's
# restart): a full host/WSL2 shutdown stops all three kind node containers
# at once, and Redis Cluster's gossip protocol comes back up believing its
# peers are at IPs that are no longer valid once the containers restart.
# Kubelet reports every pod "Running" throughout — this is a cluster-level
# protocol failure, not a pod failure, so `kubectl get pods` alone will not
# catch it. Symptom: `redis-cli cluster info` shows `cluster_state:fail`
# with slots stuck in `pfail`.
#
# This is EmptyDir-backed and intentionally ephemeral (Dependencies.txt
# section 6, "rebuild-on-demand, no DR needed") — resetting it loses no
# data that matters: rate-limit counters and the admission in-flight gauge
# are the only things Redis holds in Phase-01, both fine to zero out.
#
# Usage: scripts/recover-redis-cluster.sh
# Safe to re-run: if cluster_state is already "ok", this exits immediately
# without touching anything (see step 1).
#
# Not GitOps-bypassing: Redis lives under platform/operators/redis/, which
# the "onezox-stubs" Argo CD Application does not watch (it only watches
# platform/stubs/) — hand-applying here is the same, already-established
# mechanism Redis has used since Phase-00, not a new exception to the
# GitOps rule in CLAUDE.md.

set -euo pipefail

NAMESPACE="default"
REDIS_INIT_JOB_MANIFEST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/platform/operators/redis/redis-cluster-init-job.yaml"

cluster_state() {
  kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info 2>/dev/null \
    | grep -m1 '^cluster_state:' | cut -d: -f2 | tr -d '\r'
}

echo "=== 1. Checking current Redis Cluster state ==="
CURRENT_STATE="$(cluster_state || echo "unreachable")"
echo "cluster_state: ${CURRENT_STATE}"
if [[ "$CURRENT_STATE" == "ok" ]]; then
  echo "Already healthy — nothing to recover. Exiting without changes."
  exit 0
fi
echo "Not ok — proceeding with recovery (reset + reinit)."

echo
echo "=== 2. Resetting the 6 redis-cluster pods (drop stale gossip state) ==="
kubectl delete pod -n "$NAMESPACE" \
  redis-cluster-0 redis-cluster-1 redis-cluster-2 redis-cluster-3 redis-cluster-4 redis-cluster-5
kubectl rollout status statefulset/redis-cluster -n "$NAMESPACE" --timeout=180s

echo
echo "=== 3. Re-running redis-cluster-init to reform the ring ==="
kubectl delete job redis-cluster-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$REDIS_INIT_JOB_MANIFEST"
kubectl wait --for=condition=complete job/redis-cluster-init -n "$NAMESPACE" --timeout=120s

echo
echo "=== 4. Verifying cluster_state:ok, 16384/16384 slots ok ==="
FINAL_INFO="$(kubectl exec -n "$NAMESPACE" redis-cluster-0 -- redis-cli cluster info)"
echo "$FINAL_INFO" | grep -E '^cluster_state:|^cluster_slots_ok:|^cluster_slots_pfail:|^cluster_slots_fail:'
if ! echo "$FINAL_INFO" | grep -q '^cluster_state:ok'; then
  echo "FAILED: cluster_state is still not ok after recovery. Stopping — do not proceed to further steps." >&2
  exit 1
fi

echo
echo "=== 5. Nudging any Redis-dependent stub pods that are stuck CrashLoopBackOff ==="
# Pods that crashed while Redis was down sit in exponential backoff with a
# stale error cached in their last log — deleting them forces an immediate
# retry against the now-healthy cluster instead of waiting out the backoff.
for app in dataplane-stub provider-stub; do
  NOT_READY="$(kubectl get pods -n "$NAMESPACE" -l "app=${app}" \
    -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==false)]}{.metadata.name}{"\n"}{end}')"
  if [[ -n "$NOT_READY" ]]; then
    echo "Restarting not-ready ${app} pod(s): ${NOT_READY}"
    kubectl delete pod -n "$NAMESPACE" $NOT_READY
  else
    echo "${app}: already Ready, leaving alone."
  fi
done

echo
echo "=== 6. Waiting for dataplane-stub and provider-stub to reach 1/1 Ready ==="
kubectl wait --for=condition=ready pod -n "$NAMESPACE" -l app=dataplane-stub --timeout=90s
kubectl wait --for=condition=ready pod -n "$NAMESPACE" -l app=provider-stub --timeout=90s
kubectl get pods -n "$NAMESPACE" -l 'app in (dataplane-stub,provider-stub)'

echo
echo "=== 7. edge-gateway pod status (informational — restart it yourself if not 1/1) ==="
kubectl get pods -n "$NAMESPACE" -l app=edge-gateway
echo "Pod health here only proves the process is up, not that it can reach"
echo "dataplane-stub/Redis again. Confirm with a real request, e.g.:"
echo '  kubectl port-forward svc/edge-gateway 8080:8080 -n default &'
echo '  curl -N -X POST localhost:8080/v1/chat/completions \'
echo '    -H "Authorization: Bearer <raw-key-from-data/seed/seed-test-tenant.sh>" \'
echo '    -H "content-type: application/json" \'
echo '    -d '"'"'{"model":"onezox-ultra","messages":[{"role":"user","content":"hi"}],"stream":true}'"'"

echo
echo "=== 8. Argo CD onezox-stubs Application status ==="
kubectl get application onezox-stubs -n argocd -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}' 2>/dev/null \
  || echo "(argocd namespace not reachable — check separately)"

echo
echo "=== Recovery complete ==="
