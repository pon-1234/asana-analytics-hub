#!/usr/bin/env bash
set -euo pipefail
PROJECT=${PROJECT:-asana-analytics-hub}

# Lists principals with broad roles and key bindings for runtime

echo "=== IAM policy for ${PROJECT} ==="
gcloud projects get-iam-policy "$PROJECT" \
  --format='table(bindings.role, bindings.members)' | \
  grep -E 'roles/(owner|editor|viewer|admin|projectCreator|projectDeleter|resourcemanager)' || true

echo
echo "=== Run services invokers ==="
for svc in fetch-asana-tasks export-to-sheets snapshot-open-tasks; do
  echo "[service] $svc"
  gcloud run services get-iam-policy "$svc" --region=asia-northeast1 --project="$PROJECT" \
    --format='table(bindings.role, bindings.members)'
done

echo
echo "=== Secret accessors ==="
for sec in asana-access-token slack-bot-token; do
  echo "[secret] $sec"
  gcloud secrets get-iam-policy "$sec" --project="$PROJECT" \
    --format='table(bindings.role, bindings.members)'
done
