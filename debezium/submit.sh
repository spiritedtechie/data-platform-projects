#!/usr/bin/env bash
set -e

# Submits a Debezium connector configuration to the Debezium connector service running on Docker
# You can also do this via the Debezium UI

DEBEZIUM_HOST=localhost:8083/connectors

if [[ -e "$1" ]]; then
  echo "File exists at: $1"
else
  echo "File does not exist at: $1"
  exit 1
fi

json_contents=$(cat "$1")

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" "$DEBEZIUM_HOST" -d "$json_contents"
