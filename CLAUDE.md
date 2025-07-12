# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Asana Analytics Hub that fetches completed task data from Asana, stores it in BigQuery, and exports aggregated reports to Google Sheets. The system runs on Google Cloud Functions with Cloud Scheduler for automation.

## Common Commands

### Local Development

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run Asana data fetch locally
PYTHONPATH=. python3 asana_reporter/main.py fetch

# Run Google Sheets export locally
PYTHONPATH=. python3 asana_reporter/main.py export
```

### Deployment Commands

```bash
# Deploy fetch-asana-tasks function
gcloud functions deploy fetch-asana-tasks \
  --project=<GCP_PROJECT_ID> \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=./asana_reporter \
  --entry-point=fetch_asana_tasks_to_bq \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest \
  --timeout=540s

# Deploy export-to-sheets function
gcloud functions deploy export-to-sheets \
  --project=<GCP_PROJECT_ID> \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=./asana_reporter \
  --entry-point=export_reports_to_sheets \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest \
  --timeout=540s
```

## Architecture

The codebase follows a modular architecture with clear separation of concerns:

### Core Modules (`asana_reporter/`)

- **config.py**: Central configuration management. Validates environment variables on import and provides constants for BigQuery dataset/table names. Uses `.env` for local development and runtime environment variables for Cloud Functions.

- **asana.py**: All Asana API interactions. Key functions:
  - `get_asana_client()`: Returns authenticated Asana client
  - `get_all_projects()`: Fetches all projects from workspace
  - `get_completed_tasks_for_project()`: Fetches and parses completed tasks with custom time fields
  - `_parse_custom_fields()`: Extracts time data from Asana custom fields (handles both `actual_time_raw` and `時間達成率`)

- **bigquery.py**: All BigQuery operations. Key functions:
  - `get_bigquery_client()`: Returns authenticated BigQuery client (uses service account locally, ADC in Cloud)
  - `ensure_table_exists()`: Creates dataset/table if missing
  - `insert_tasks()`: Uses MERGE statement to upsert tasks (prevents duplicates)
  - `get_report_data()`: Returns aggregated data for three report types using efficient CTE-based queries

- **sheets.py**: All Google Sheets operations. Key functions:
  - `get_sheets_service()`: Returns authenticated Sheets API service
  - `update_sheet_with_report()`: Updates specific sheet with formatted data
  - Handles API rate limiting with retry logic

- **main.py**: Entry points for Cloud Functions:
  - `fetch_asana_tasks_to_bq()`: Cloud Function entry point for data fetching
  - `export_reports_to_sheets()`: Cloud Function entry point for report generation
  - Command-line interface for local execution (`fetch` or `export` commands)

### Data Flow

1. **Data Collection**: Asana API → Python objects → BigQuery `asana_analytics.completed_tasks` table
2. **Data Aggregation**: BigQuery SQL queries aggregate by month/project/assignee
3. **Report Generation**: Query results → Google Sheets (3 tabs: project, assignee, project-assignee)

### Key Design Decisions

- **MERGE instead of INSERT**: Prevents duplicate task records, allows updates if task data changes
- **Custom field parsing**: Handles Japanese field names (`時間達成率`) and fallback logic for time calculations
- **Modular authentication**: Different credential strategies for local vs Cloud Functions execution
- **Rate limiting**: Built-in delays and retry logic for API calls

## Environment Configuration

Required environment variables (set in `.env` for local development):
- `ASANA_ACCESS_TOKEN`: Asana Personal Access Token
- `ASANA_WORKSPACE_ID`: Target Asana workspace
- `GCP_PROJECT_ID`: Google Cloud project ID
- `GCP_CREDENTIALS_PATH`: Path to service account JSON (local only)
- `SPREADSHEET_ID`: Target Google Sheets ID

For deployment, use `env.yaml` (excludes sensitive tokens) and Secret Manager for `ASANA_ACCESS_TOKEN`.

## BigQuery Schema

Table: `asana_analytics.completed_tasks`
- Primary key: `task_id` (STRING)
- Time tracking: `estimated_time`, `actual_time`, `actual_time_raw` (FLOAT)
- Metadata: `project_name`, `assignee_name`, `completed_at`, etc.
- Audit: `inserted_at` (auto-populated on insert/update)