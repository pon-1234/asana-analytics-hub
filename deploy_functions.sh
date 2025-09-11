#!/bin/bash

# Deploy Cloud Functions Gen2 and update Cloud Scheduler

echo "=== Deploying Cloud Functions Gen2 ==="

# Deploy fetch-asana-tasks function
echo "1. Deploying fetch-asana-tasks function..."
gcloud functions deploy fetch-asana-tasks \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=fetch_asana_tasks_to_bq \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account=bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --min-instances=0 \
  --gen2

# Deploy export-to-sheets function
echo "2. Deploying export-to-sheets function..."
gcloud functions deploy export-to-sheets \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=export_reports_to_sheets \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account=bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --min-instances=0 \
  --gen2

# Deploy snapshot-open-tasks function
echo "3. Deploying snapshot-open-tasks function..."
gcloud functions deploy snapshot-open-tasks \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=snapshot_open_tasks \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account=bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --min-instances=0 \
  --gen2

# Deploy manual weekly digest function (optional manual trigger)
echo "4. Deploying send-weekly-digest-manual function..."
gcloud functions deploy send-weekly-digest-manual \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=send_weekly_digest_manual \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account=bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --min-instances=0 \
  --gen2

echo "=== Getting Function URLs ==="

# Get the URLs of the deployed functions
FETCH_URL=$(gcloud functions describe fetch-asana-tasks --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")
EXPORT_URL=$(gcloud functions describe export-to-sheets --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")
SNAPSHOT_URL=$(gcloud functions describe snapshot-open-tasks --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")
WEEKLY_MANUAL_URL=$(gcloud functions describe send-weekly-digest-manual --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")

echo "Fetch function URL: $FETCH_URL"
echo "Export function URL: $EXPORT_URL"

echo "=== Updating Cloud Scheduler Jobs (with OIDC) ==="

# Update Cloud Scheduler job for fetch-asana-tasks
echo "4. Updating Cloud Scheduler job for fetch-asana-tasks (weekly Mon 06:30 JST)..."
gcloud scheduler jobs update http fetch-asana-tasks-daily \
  --location=asia-northeast1 \
  --uri="$FETCH_URL" \
  --http-method=POST \
  --schedule="30 6 * * 1" \
  --time-zone="Asia/Tokyo" \
  --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
  --oidc-token-audience="$FETCH_URL" \
  --project=asana-analytics-hub

# Update Cloud Scheduler job for export-to-sheets
echo "5. Updating Cloud Scheduler job for export-to-sheets (weekly Mon 07:15 JST)..."
gcloud scheduler jobs update http export-to-sheets-daily \
  --location=asia-northeast1 \
  --uri="$EXPORT_URL" \
  --http-method=POST \
  --schedule="15 7 * * 1" \
  --time-zone="Asia/Tokyo" \
  --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
  --oidc-token-audience="$EXPORT_URL" \
  --project=asana-analytics-hub

# Create or update snapshot weekly job (Mon 07:00 JST)
echo "6. Creating/Updating Cloud Scheduler job for snapshot-open-tasks (weekly)..."
gcloud scheduler jobs describe snapshot-open-tasks-daily --location=asia-northeast1 --project=asana-analytics-hub >/dev/null 2>&1
if [ $? -eq 0 ]; then
  gcloud scheduler jobs update http snapshot-open-tasks-daily \
    --location=asia-northeast1 \
    --uri="$SNAPSHOT_URL" \
    --http-method=POST \
    --schedule="0 7 * * 1" \
    --time-zone="Asia/Tokyo" \
    --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --oidc-token-audience="$SNAPSHOT_URL" \
    --project=asana-analytics-hub
else
  gcloud scheduler jobs create http snapshot-open-tasks-daily \
    --location=asia-northeast1 \
    --uri="$SNAPSHOT_URL" \
    --http-method=POST \
    --schedule="0 7 * * 1" \
    --time-zone="Asia/Tokyo" \
    --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --oidc-token-audience="$SNAPSHOT_URL" \
    --project=asana-analytics-hub
fi

# Optionally create/update weekly digest manual scheduler on Mondays at 07:10 JST
if [ "${SCHEDULE_WEEKLY_DIGEST:-false}" = "true" ]; then
  echo "7. Creating/Updating Cloud Scheduler job for send-weekly-digest-manual (Mondays 07:10 JST)..."
  gcloud scheduler jobs describe send-weekly-digest-manual-monday --location=asia-northeast1 --project=asana-analytics-hub >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    gcloud scheduler jobs update http send-weekly-digest-manual-monday \
      --location=asia-northeast1 \
      --uri="$WEEKLY_MANUAL_URL" \
      --http-method=POST \
      --schedule="10 7 * * 1" \
      --time-zone="Asia/Tokyo" \
      --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
      --oidc-token-audience="$WEEKLY_MANUAL_URL" \
      --project=asana-analytics-hub
  else
    gcloud scheduler jobs create http send-weekly-digest-manual-monday \
      --location=asia-northeast1 \
      --uri="$WEEKLY_MANUAL_URL" \
      --http-method=POST \
      --schedule="10 7 * * 1" \
      --time-zone="Asia/Tokyo" \
      --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
      --oidc-token-audience="$WEEKLY_MANUAL_URL" \
      --project=asana-analytics-hub
  fi
fi

echo "=== Resuming Cloud Scheduler Jobs ==="

# Resume the Cloud Scheduler jobs
echo "7. Resuming fetch-asana-tasks-daily job..."
gcloud scheduler jobs resume fetch-asana-tasks-daily \
  --location=asia-northeast1 \
  --project=asana-analytics-hub

echo "8. Resuming export-to-sheets-daily job..."
gcloud scheduler jobs resume export-to-sheets-daily \
  --location=asia-northeast1 \
  --project=asana-analytics-hub

echo "9. Resuming snapshot-open-tasks-daily job..."
gcloud scheduler jobs resume snapshot-open-tasks-daily \
  --location=asia-northeast1 \
  --project=asana-analytics-hub

if [ "${SCHEDULE_WEEKLY_DIGEST:-false}" = "true" ]; then
  echo "10. Resuming send-weekly-digest-manual-monday job..."
  gcloud scheduler jobs resume send-weekly-digest-manual-monday \
    --location=asia-northeast1 \
    --project=asana-analytics-hub || true
fi

echo "=== Granting Cloud Run Invoker to Scheduler SA ==="
for SERVICE_NAME in fetch-asana-tasks export-to-sheets snapshot-open-tasks send-weekly-digest-manual; do
  gcloud run services add-iam-policy-binding "$SERVICE_NAME" \
    --region=asia-northeast1 \
    --project=asana-analytics-hub \
    --member="serviceAccount:bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --platform=managed
done

# Ensure least-privilege IAM for runtime SA and secrets (idempotent)
echo "=== Ensuring least-privilege IAM for runtime SA ==="
RUNTIME_SA="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding asana-analytics-hub \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/bigquery.dataEditor" --quiet || true
gcloud projects add-iam-policy-binding asana-analytics-hub \
  --member="serviceAccount:${RUNTIME_SA}" \
  --role="roles/bigquery.jobUser" --quiet || true
# Secret-level accessor (avoid project-wide)
for SECRET in asana-access-token slack-bot-token; do
  gcloud secrets add-iam-policy-binding "$SECRET" \
    --project=asana-analytics-hub \
    --member="serviceAccount:${RUNTIME_SA}" \
    --role="roles/secretmanager.secretAccessor" --quiet || true
done

echo "=== Deployment Complete ==="
echo "Both Cloud Functions have been deployed and Cloud Scheduler jobs have been updated and resumed."
