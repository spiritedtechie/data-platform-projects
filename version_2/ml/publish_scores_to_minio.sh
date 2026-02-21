#!/usr/bin/env bash
set -euo pipefail

LOCAL_SCORES_DIR="${1:-version_2/ml/scores}"
S3_BASE="${2:-s3://lake/ml/scores}"

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI not found. Install awscli to publish scores." >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin123}"
AWS_ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:9000}"

for f in ml_scores_line_hour.parquet ml_forecast_line_day.parquet ml_predicted_incident_duration.parquet ml_alerts_line_hour.parquet; do
  if [[ -f "${LOCAL_SCORES_DIR}/${f}" ]]; then
    aws --endpoint-url "${AWS_ENDPOINT}" s3 cp "${LOCAL_SCORES_DIR}/${f}" "${S3_BASE}/${f}"
  fi
done
