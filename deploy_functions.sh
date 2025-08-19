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

echo "Fetch function URL: $FETCH_URL"
echo "Export function URL: $EXPORT_URL"

echo "=== Updating Cloud Scheduler Jobs (with OIDC) ==="

# Update Cloud Scheduler job for fetch-asana-tasks
echo "4. Updating Cloud Scheduler job for fetch-asana-tasks..."
gcloud scheduler jobs update http fetch-asana-tasks-daily \
  --location=asia-northeast1 \
  --uri="$FETCH_URL" \
  --http-method=POST \
  --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
  --oidc-token-audience="$FETCH_URL" \
  --project=asana-analytics-hub

# Update Cloud Scheduler job for export-to-sheets
echo "5. Updating Cloud Scheduler job for export-to-sheets..."
gcloud scheduler jobs update http export-to-sheets-daily \
  --location=asia-northeast1 \
  --uri="$EXPORT_URL" \
  --http-method=POST \
  --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
  --oidc-token-audience="$EXPORT_URL" \
  --project=asana-analytics-hub

# Create or update snapshot daily job (07:00 JST)
echo "6. Creating/Updating Cloud Scheduler job for snapshot-open-tasks..."
gcloud scheduler jobs describe snapshot-open-tasks-daily --location=asia-northeast1 --project=asana-analytics-hub >/dev/null 2>&1
if [ $? -eq 0 ]; then
  gcloud scheduler jobs update http snapshot-open-tasks-daily \
    --location=asia-northeast1 \
    --uri="$SNAPSHOT_URL" \
    --http-method=POST \
    --schedule="0 22 * * *" \
    --time-zone="Asia/Tokyo" \
    --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --oidc-token-audience="$SNAPSHOT_URL" \
    --project=asana-analytics-hub
else
  gcloud scheduler jobs create http snapshot-open-tasks-daily \
    --location=asia-northeast1 \
    --uri="$SNAPSHOT_URL" \
    --http-method=POST \
    --schedule="0 22 * * *" \
    --time-zone="Asia/Tokyo" \
    --oidc-service-account-email="bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --oidc-token-audience="$SNAPSHOT_URL" \
    --project=asana-analytics-hub
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

echo "=== Granting Cloud Run Invoker to Scheduler SA ==="
for SVC in fetch-asana-tasks export-to-sheets snapshot-open-tasks; do
  URL=$(gcloud functions describe "$SVC" --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")
  SERVICE_NAME=$(echo "$URL" | sed -E 's#https://(.+)-(.+)-(.+)-a.run.app#\1#')
  gcloud run services add-iam-policy-binding "$SERVICE_NAME" \
    --region=asia-northeast1 \
    --project=asana-analytics-hub \
    --member="serviceAccount:bigquery-to-sheets@asana-analytics-hub.iam.gserviceaccount.com" \
    --role="roles/run.invoker"
done

echo "=== Deployment Complete ==="
echo "Both Cloud Functions have been deployed and Cloud Scheduler jobs have been updated and resumed."