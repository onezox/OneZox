#!/usr/bin/env bash
# Applies every migration in this directory, in filename order, against the
# running onezox-crdb cluster (Phase-00, Deployment Step 8). Each file is
# idempotent, so this is safe to re-run.
set -euo pipefail

POD="${1:-onezox-crdb-0}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for f in "$DIR"/*.sql; do
  echo "Applying: $(basename "$f")"
  kubectl exec -i "$POD" -- cockroach sql --insecure < "$f"
done

echo
echo "Tables:"
kubectl exec "$POD" -- cockroach sql --insecure --execute="SHOW TABLES;"
