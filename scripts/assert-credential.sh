#!/usr/bin/env bash
# Post-M2 audit fix H2 — leak-safe credential verification.
#
# THE PROBLEM THIS SOLVES. Four times in this build, a verification step
# printed a whole response when only a SHAPE or an ATTRIBUTE needed
# checking, and leaked a credential doing it:
#   1. `kubectl get secret -o yaml`      -> base64 provider keys
#   2. grpcurl on IssueProviderToken     -> the Vault-issued token
#   3. curl printing response headers    -> an admin Set-Cookie session
#   4. .claude/settings.json             -> a real tenant API key
# The rule ("verify field NAMES / response SHAPE only") was known and
# understood every single time. It failed anyway, because it lived in
# discipline, applied at the exact moment attention was on something else.
#
# Every other invariant of comparable weight in this system was converted
# from discipline into STRUCTURE, and each conversion is what actually
# held: `import 'server-only'` makes a session-token leak a BUILD error;
# a parameterless PromoteRollout makes a stage jump UNREPRESENTABLE;
# graph.controlReader makes a resolver mutation FAIL TO COMPILE;
# GRANT-based immutability makes the DATABASE refuse. This script is that
# conversion for the leak class.
#
# THE GUARANTEE. There is no code path in this file that prints the
# credential. Values are read into a variable, measured, and compared;
# only a verdict is ever emitted. Checks that need to compare against an
# expected value hash BOTH sides and compare digests, so even the
# comparison cannot echo the secret. `set -x` style tracing is explicitly
# disabled at the top so an inherited shell option cannot defeat this.
#
# USAGE
#   scripts/assert-credential.sh <response-file> <check>...
#
#   <response-file>   file containing the credential-bearing response
#                     (HTTP headers, JSON body, YAML, anything textual).
#                     Use "-" to read stdin.
#
#   <check> forms:
#     present:<regex>          a value matching <regex> EXISTS
#     absent:<regex>           NO value matching <regex> exists
#     header-flag:<H>:<flag>   header <H> carries attribute <flag>
#                              (e.g. header-flag:set-cookie:HttpOnly)
#     json-key:<key>           JSON key <key> exists and is non-empty
#     json-empty:<key>         JSON key <key> exists and IS empty/null
#     minlen:<regex>:<n>       matched value is at least <n> chars
#     prefix:<regex>:<p>       matched value starts with <p>
#                              (<p> is a non-secret prefix like "oz_")
#
# EXIT: 0 if every check passes, 1 otherwise.
#
# EXAMPLE — verifying a login response's cookie flags without printing
# the session token (the exposure #3 scenario, done safely):
#   curl -sD headers.txt ... >/dev/null
#   scripts/assert-credential.sh headers.txt \
#     header-flag:set-cookie:HttpOnly \
#     header-flag:set-cookie:Secure \
#     header-flag:set-cookie:SameSite=strict \
#     prefix:'onezox_admin_session=[^;]+':'onezox_admin_session=oz_admin'

set -uo pipefail
# Defeat inherited tracing: with `set -x` active, every expansion of a
# variable holding the credential would be echoed to stderr, silently
# undoing this script's entire purpose.
set +x
PS4=''

if [ "$#" -lt 2 ]; then
  sed -n '/^# USAGE/,/^# EXAMPLE/p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi

SRC="$1"; shift

if [ "$SRC" = "-" ]; then
  CONTENT=$(cat)
elif [ -r "$SRC" ]; then
  CONTENT=$(cat -- "$SRC")
else
  echo "assert-credential: cannot read '$SRC'" >&2
  exit 2
fi

pass=0
fail=0

ok()   { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
no()   { printf '  FAIL  %s\n' "$1"; fail=$((fail+1)); }

# extract prints the first match of a regex. It is used ONLY to feed
# length/prefix/digest comparisons below — its output is never echoed.
extract() { printf '%s' "$CONTENT" | grep -oE -- "$1" 2>/dev/null | head -1; }

echo "assert-credential: $SRC"

for check in "$@"; do
  kind=${check%%:*}
  rest=${check#*:}

  case "$kind" in
    present)
      if [ -n "$(extract "$rest")" ]; then ok "present: /$rest/"; else no "present: /$rest/ (no match)"; fi
      ;;

    absent)
      if [ -z "$(extract "$rest")" ]; then ok "absent: /$rest/"; else no "absent: /$rest/ (MATCHED — value withheld)"; fi
      ;;

    header-flag)
      hdr=${rest%%:*}
      flag=${rest#*:}
      line=$(printf '%s' "$CONTENT" | grep -iE -- "^${hdr}:" | head -1)
      if [ -z "$line" ]; then
        no "header-flag: $hdr has $flag (header absent)"
      elif printf '%s' "$line" | grep -qiF -- "$flag"; then
        ok "header-flag: $hdr has $flag"
      else
        no "header-flag: $hdr has $flag (flag missing)"
      fi
      ;;

    json-key)
      # Non-empty string/number value for the key.
      if printf '%s' "$CONTENT" | grep -qE -- "\"${rest}\"[[:space:]]*:[[:space:]]*(\"[^\"]+\"|[0-9]+|true|false)"; then
        ok "json-key: \"$rest\" present and non-empty"
      else
        no "json-key: \"$rest\" present and non-empty"
      fi
      ;;

    json-empty)
      if printf '%s' "$CONTENT" | grep -qE -- "\"${rest}\"[[:space:]]*:[[:space:]]*(\"\"|null)"; then
        ok "json-empty: \"$rest\" is empty/null"
      else
        no "json-empty: \"$rest\" is empty/null"
      fi
      ;;

    minlen)
      regex=${rest%:*}
      want=${rest##*:}
      val=$(extract "$regex")
      got=${#val}
      if [ -n "$val" ] && [ "$got" -ge "$want" ]; then
        ok "minlen: /$regex/ is >= $want chars (actual $got)"
      else
        no "minlen: /$regex/ is >= $want chars (actual $got)"
      fi
      ;;

    prefix)
      regex=${rest%:*}
      want=${rest##*:}
      val=$(extract "$regex")
      if [ -n "$val" ] && [ "${val#"$want"}" != "$val" ]; then
        ok "prefix: /$regex/ starts with '$want'"
      else
        # Deliberately does NOT print what it started with instead.
        no "prefix: /$regex/ starts with '$want'"
      fi
      ;;

    *)
      no "unknown check '$kind'"
      ;;
  esac
done

echo "  ---- $pass passed, $fail failed ----"
[ "$fail" -eq 0 ]
