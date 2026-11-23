#!/usr/bin/env bash
# migrate-check.sh — ab0t-events-go v0.2.0 -> v0.2.1. Read-only: it NEVER edits your code.
#
# WHAT CHANGED (bug fixes — your code compiles unchanged, no API removed, no signature changed)
#   - Durable path: a stale duplicate of a failed attempt is dropped; Delivery.Attempts reads N-1 until the
#     retry is enqueued; one extra DeliveryLog.Record with Attempt.N == 0 per durable retry >= 5 s out.
#   - In-process retries re-read the subscription (rotated secret used, deleted subscription not sent to).
#   - A cancelled attempt (Close / queue stop) is not recorded; Process / ProcessRef return context.Canceled.
#   - ratelimit.ByHost keys are canonical; webhooks.System.Close is safe concurrently.
#
# WHAT IT REPORTS (advisory — nothing here is required)
#   ○ E1 ATTEMPTS  a read of .Attempts in a file that uses the events package.
#   ○ E2 LOGIMPL   a custom DeliveryLog Record method: it must append nothing when Attempt.N == 0.
#   ○ E4 SHUTDOWN  a custom queue adapter calling Process / ProcessRef (treat context.Canceled as not done),
#                  and webhooks.New / inproc use in a repo that never calls Recover.
#   ○ E5 BYHOST    a use of ratelimit.ByHost: its keys for differently spelled hosts changed.
#
# MARKING A SITE REVIEWED:  // eventsgo:v0.2.1-reviewed  on the line or the line above it, or
#                           // eventsgo:v0.2.1-reviewed-file  anywhere in the file.
# Usage: ./migrate-check.sh [repo-dir]     Exit: 0 always (advisory) · 2 usage error
set -u
T="."
for a in "$@"; do
  case "$a" in
    --advisory) ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    -*) echo "unknown option: $a" >&2; exit 2 ;;
    *) T="$a" ;;
  esac
done
[ -d "$T" ] || { echo "not a directory: $T" >&2; exit 2; }

# reviewed FILE LINE — true when the line or the line above carries the reviewed marker.
reviewed() {
  local cur prev
  cur=$(sed -n "${2}p" "$1")
  prev=$(sed -n "$(($2 > 1 ? $2 - 1 : 1))p" "$1")
  case "$cur$prev" in *eventsgo:v0.2.1-reviewed*) return 0 ;; esac
  return 1
}

# collect VAR FILE PATTERN — append unreviewed matching lines of FILE to the named report variable.
collect() {
  local n rest
  while IFS=: read -r n rest; do
    reviewed "$2" "$n" || printf -v "$1" '%s%s:%s:%s\n' "${!1}" "$2" "$n" "$rest"
  done < <(grep -nE "$3" "$2" 2>/dev/null)
}

e1=""
e2=""
e4=""
e5=""
recover=0
facade=0
while IFS= read -r f; do
  [ -n "$f" ] || continue
  grep -qE 'ab0t-events-go' "$f" 2>/dev/null || continue
  grep -qE '\.Recover\(' "$f" && recover=1
  grep -qE 'webhooks\.New\(|inproc\.New\(' "$f" && facade=1
  grep -q 'eventsgo:v0.2.1-reviewed-file' "$f" && continue
  collect e1 "$f" '\.Attempts([^A-Za-z0-9_]|$)'
  collect e2 "$f" 'func \([^)]*\) Record\([^)]*events\.DeliveryRecord\)'
  collect e4 "$f" '\.(Process|ProcessRef)\('
  collect e5 "$f" 'ratelimit\.ByHost'
done < <(find "$T" -name '*.go' -not -path '*/vendor/*' -not -path '*/.git/*' 2>/dev/null)

if [ -n "$e1" ]; then
  echo "○ E1 ATTEMPTS — Delivery.Attempts on the durable path reads N-1 until a failed attempt's retry is"
  echo "  enqueued. Count the attempt list from GetDelivery if you need attempts made so far."
  printf '%s' "$e1" | sed 's/^/    /'
fi
if [ -n "$e2" ]; then
  echo "○ E2 LOGIMPL — a custom DeliveryLog: Record with Attempt.N == 0 must update the delivery and append nothing."
  printf '%s' "$e2" | sed 's/^/    /'
fi
if [ -n "$e4" ]; then
  echo "○ E4 SHUTDOWN — Process / ProcessRef return context.Canceled for an attempt cut off by shutdown;"
  echo "  nothing was recorded, so do not ack/delete the job."
  printf '%s' "$e4" | sed 's/^/    /'
fi
if [ "$facade" = 1 ] && [ "$recover" = 0 ]; then
  echo "○ E4 SHUTDOWN — this repo uses the in-process queue but never calls Recover. An attempt interrupted by"
  echo "  Close is not recorded; call Recover(ctx, orgID) at start to send it."
fi
if [ -n "$e5" ]; then
  echo "○ E5 BYHOST — ratelimit.ByHost keys are canonical now (lower-case, no trailing dot, no default port)."
  printf '%s' "$e5" | sed 's/^/    /'
fi
if [ -z "$e1$e2$e4$e5" ] && { [ "$facade" = 0 ] || [ "$recover" = 1 ]; }; then
  echo "No v0.2.1 notes: nothing to review."
else
  echo "Advisory only — see MIGRATION.md."
fi
exit 0
