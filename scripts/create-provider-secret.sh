#!/usr/bin/env bash
# Creates (or replaces) the provider-gateway-credentials K8s Secret from
# interactively-entered, hidden API key input — Phase-02 Step J3, run
# BEFORE any real adapter call (Step J4-J6) and only after the egress
# allow-list (Step J1-J2) is already verified in both directions. Extended
# for the Between-Phase provider task (Grok/GLM/Kimi) — now prompts for
# six fields, not three.
#
# Never echoes a key: `read -s` suppresses terminal echo, and nothing
# below ever `echo`/`printf`s a key value to stdout/stderr or passes one
# as a literal command-line argument (which would land in shell history
# and process listings) — every key reaches kubectl only via a
# --from-file path, per the explicit ask to avoid --from-literal. This is
# the ONLY sanctioned way real key material enters this Secret — never via
# `kubectl get secret ... -o jsonpath`/`-o yaml` in an agent session, which
# returns full base64-encoded (trivially decodable, not encrypted) values;
# an agent verifying this Secret's state must ONLY ever ask for
# `-o json | jq '.data | keys'`-style field NAMES, the same shape this
# script's own verification step below already uses.
#
# Usage: scripts/create-provider-secret.sh
# Leave any prompt blank to skip/deactivate that provider — proceeds with
# whichever keys are actually provided rather than requiring all six, same
# "proceed with what's available, don't block" pattern used earlier in
# Phase-02 for the real-provider verification steps. This blank-means-skip
# behavior IS how a provider gets deactivated after already being active
# (e.g. Gemini, Between-Phase provider task): `kubectl apply`'s own 3-way
# merge (comparing this run's manifest against the Secret's own
# last-applied-configuration) deletes a data key that was present in the
# previous apply but is absent from this one — confirmed against this
# exact Secret's own annotation before relying on it, not assumed from
# general kubectl behavior.

set -euo pipefail

NAMESPACE="default"
SECRET_NAME="provider-gateway-credentials"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found on PATH" >&2
  exit 1
fi

TMPDIR="$(mktemp -d)"
chmod 700 "$TMPDIR"

cleanup() {
  # shred -u overwrites before unlinking when available (GNU coreutils,
  # present on this WSL2 Ubuntu host); rm -f is the portable fallback if
  # it isn't. Either way this always runs, even on an early failure
  # above, via the EXIT trap below.
  if command -v shred >/dev/null 2>&1; then
    find "$TMPDIR" -type f -exec shred -u {} + 2>/dev/null || true
  else
    find "$TMPDIR" -type f -exec rm -f {} + 2>/dev/null || true
  fi
  rmdir "$TMPDIR" 2>/dev/null || true
}
trap cleanup EXIT

declare -a FROM_FILE_ARGS=()

prompt_for_key() {
  local field_name="$1"
  local display_name="$2"
  local key=""

  read -r -s -p "Enter ${display_name} (leave blank to skip): " key
  echo >&2 # move off the prompt line; read -s already suppressed echo of input

  if [[ -z "$key" ]]; then
    echo "Skipping ${display_name} (no value entered)." >&2
    unset key
    return
  fi

  local keyfile="${TMPDIR}/${field_name}"
  ( umask 077; printf '%s' "$key" > "$keyfile" )
  unset key

  FROM_FILE_ARGS+=("--from-file=${field_name}=${keyfile}")
  echo "${display_name} captured." >&2
}

prompt_for_key "OPENAI_API_KEY" "OpenAI API key"
prompt_for_key "ANTHROPIC_API_KEY" "Anthropic API key"
prompt_for_key "GOOGLE_API_KEY" "Google (Gemini) API key — leave blank to keep Gemini deactivated (Between-Phase provider task; main.go's registration for it is unaffected either way, only this key's presence decides it)"
prompt_for_key "GROK_API_KEY" "Grok (xAI) API key"
prompt_for_key "GLM_API_KEY" "GLM (Zhipu / z.ai international account) API key"
prompt_for_key "KIMI_API_KEY" "Kimi (Moonshot) API key"

if [[ "${#FROM_FILE_ARGS[@]}" -eq 0 ]]; then
  echo "No keys entered — nothing to do." >&2
  exit 1
fi

echo >&2
echo "Creating/replacing Secret ${SECRET_NAME} in namespace ${NAMESPACE} with ${#FROM_FILE_ARGS[@]} key(s)..." >&2

# create-or-replace via dry-run|apply, not delete-then-create: avoids a
# window where the Secret briefly doesn't exist (provider-gateway's own
# readiness doesn't depend on it yet, but a running pod with an already-
# mounted env var wouldn't see an update anyway without a restart — apply
# is just the correct idempotent primitive here regardless).
kubectl create secret generic "$SECRET_NAME" \
  -n "$NAMESPACE" \
  "${FROM_FILE_ARGS[@]}" \
  --dry-run=client -o yaml \
  | kubectl apply -f -

echo >&2
echo "=== Verification (field NAMES only, values never printed) ===" >&2
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE"
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o json \
  | python3 -c "import json,sys; print('keys present:', sorted(json.load(sys.stdin).get('data', {}).keys()))"
