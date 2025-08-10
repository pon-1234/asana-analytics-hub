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
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest \
  --timeout=540s \
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
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest \
  --timeout=540s \
  --gen2

echo "=== Getting Function URLs ==="

# Get the URLs of the deployed functions
FETCH_URL=$(gcloud functions describe fetch-asana-tasks --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")
EXPORT_URL=$(gcloud functions describe export-to-sheets --region=asia-northeast1 --project=asana-analytics-hub --gen2 --format="value(serviceConfig.uri)")

echo "Fetch function URL: $FETCH_URL"
echo "Export function URL: $EXPORT_URL"

echo "=== Updating Cloud Scheduler Jobs ==="

# Update Cloud Scheduler job for fetch-asana-tasks
echo "3. Updating Cloud Scheduler job for fetch-asana-tasks..."
gcloud scheduler jobs update http fetch-asana-tasks-daily \
  --location=asia-northeast1 \
  --uri="$FETCH_URL" \
  --http-method=POST \
  --project=asana-analytics-hub

# Update Cloud Scheduler job for export-to-sheets
echo "4. Updating Cloud Scheduler job for export-to-sheets..."
gcloud scheduler jobs update http export-to-sheets-daily \
  --location=asia-northeast1 \
  --uri="$EXPORT_URL" \
  --http-method=POST \
  --project=asana-analytics-hub

echo "=== Resuming Cloud Scheduler Jobs ==="

# Resume the Cloud Scheduler jobs
echo "5. Resuming fetch-asana-tasks-daily job..."
gcloud scheduler jobs resume fetch-asana-tasks-daily \
  --location=asia-northeast1 \
  --project=asana-analytics-hub

echo "6. Resuming export-to-sheets-daily job..."
gcloud scheduler jobs resume export-to-sheets-daily \
  --location=asia-northeast1 \
  --project=asana-analytics-hub

echo "=== Deployment Complete ==="
echo "Both Cloud Functions have been deployed and Cloud Scheduler jobs have been updated and resumed."