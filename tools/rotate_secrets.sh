#!/usr/bin/env bash
set -euo pipefail

# Rotate secrets by adding a new version. Functions read :latest so no code change needed.
# Usage: PROJECT=asana-analytics-hub ./tools/rotate_secrets.sh

PROJECT=${PROJECT:-asana-analytics-hub}
SECRETS=(
  "asana-access-token:ASANA PAT"
  "slack-bot-token:Slack Bot Token"
)

echo "Project: ${PROJECT}"
for entry in "${SECRETS[@]}"; do
  secret_name="${entry%%:*}"
  prompt="${entry##*:}"
  echo "--- Rotating ${secret_name} (${prompt}) ---"
  read -r -p "Enter new ${prompt}: " secret_value
  # Allow empty? No.
  if [[ -z "${secret_value}" ]]; then
    echo "Skipped ${secret_name} (empty input)."
    continue
  fi
  printf "%s" "${secret_value}" | gcloud secrets versions add "${secret_name}" --data-file=- --project="${PROJECT}"
  echo "Added new version for ${secret_name}."
  unset secret_value
  echo

done

echo "Done. Cloud Functions will use :latest on next cold start or deployment."