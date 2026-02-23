#!/usr/bin/env bash
set -euo pipefail

# CI entrypoint for dbt_gold.
# Defaults can be overridden by CI environment variables.
export DBT_TFL_SILVER_SCHEMA="${DBT_TFL_SILVER_SCHEMA:-silver}"
export DBT_TFL_SILVER_IDENTIFIER="${DBT_TFL_SILVER_IDENTIFIER:-tfl_line_status_events}"

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$HOME/.dbt}"
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_CONTEXT="${ALERT_CONTEXT:-dbt_gold_ci}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CURRENT_STEP="init"

# Change to the project root directory
cd "$(dirname "$0")/.."

notify_failure() {
  local exit_code="${1:-1}"
  if [[ -n "$ALERT_WEBHOOK_URL" ]]; then
    ./scripts/notify_failure.sh \
      --webhook-url "$ALERT_WEBHOOK_URL" \
      --context "$ALERT_CONTEXT" \
      --step "$CURRENT_STEP" \
      --exit-code "$exit_code"
  fi
}

# Set up a trap to catch any errors and send a notification
trap 'ec=$?; notify_failure "$ec"; exit "$ec"' ERR

# Run dbt commands
CURRENT_STEP="dbt parse"
echo "[ci] dbt parse"
dbt parse --profiles-dir "$DBT_PROFILES_DIR"

CURRENT_STEP="dbt source freshness"
echo "[ci] dbt source freshness"
dbt source freshness --profiles-dir "$DBT_PROFILES_DIR"

CURRENT_STEP="dbt build"
echo "[ci] dbt build"
dbt build --profiles-dir "$DBT_PROFILES_DIR"

CURRENT_STEP="sqlfluff lint"
echo "[ci] sqlfluff lint"
"$PYTHON_BIN" -m sqlfluff lint models --config .sqlfluff
