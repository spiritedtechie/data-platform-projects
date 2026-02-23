#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --webhook-url URL --context NAME --step STEP --exit-code CODE" >&2
  exit 2
}

WEBHOOK_URL=""
CONTEXT="dbt_gold_ci"
STEP="unknown"
EXIT_CODE="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --webhook-url)
      WEBHOOK_URL="${2:-}"
      shift 2
      ;;
    --context)
      CONTEXT="${2:-}"
      shift 2
      ;;
    --step)
      STEP="${2:-}"
      shift 2
      ;;
    --exit-code)
      EXIT_CODE="${2:-}"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

if [[ -z "$WEBHOOK_URL" ]]; then
  usage
fi

TIMESTAMP_UTC="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
HOSTNAME_VALUE="${HOSTNAME:-unknown-host}"
MESSAGE="[${CONTEXT}] failed at step '${STEP}' (exit_code=${EXIT_CODE}) on ${HOSTNAME_VALUE} at ${TIMESTAMP_UTC}"

# Slack/Teams-compatible simple JSON payload.
PAYLOAD="{\"text\":\"${MESSAGE}\"}"

curl -sS -X POST "$WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" >/dev/null

echo "[notify] sent failure alert for step '${STEP}'"
