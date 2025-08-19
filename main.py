import sys
import time
from typing import List, Dict, Any
import functions_framework
from flask import Request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# インポートパスを修正
from asana_reporter import asana, bigquery, sheets, config
from asana_reporter.asana import asana as asana_sdk # asana.rest.ApiException をキャッチするため
from asana_reporter.slack_notifier import (
    send_run_summary,
    send_monthly_digest,
    send_daily_digest,
)

def _get_last_modified_from_bq(client: bigquery.bigquery.Client) -> str | None:
    """BigQueryから最新のタスク更新時刻 (modified_at) を取得する"""
    table_fqn = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.{config.BQ_TABLE_ID}"
    try:
        # テーブルの存在確認
        client.get_table(table_fqn)

        # UTC形式 (ISO 8601) で最終更新日時を取得
        query = f"""
        SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', MAX(modified_at)) as last_modified
        FROM `{table_fqn}`
        WHERE modified_at IS NOT NULL
        """
        result = client.query(query).result()
        row = next(iter(result), None)

        if row and row.last_modified:
            print(f"Last modified time in BigQuery: {row.last_modified}")
            return row.last_modified

        print("No last modified time found in BigQuery. Will perform a full sync.")
        return None
    except Exception as e:
        print(f"Could not get last modified time from BigQuery (table might be empty): {e}. Will perform a full sync.")
        return None


@functions_framework.http
def fetch_asana_tasks_to_bq(request: Request):
    """
    Asanaからタスクを取得してBigQueryに保存するCloud Function。
    差分取得に対応し、タイムアウトを防ぎます。
    """
    print("--- Starting Asana to BigQuery sync (Incremental/Full selectable) ---")
    started_at_iso = datetime.now(timezone.utc).isoformat()
    try:
        config.validate_config()

        bq_client = bigquery.get_bigquery_client()
        api_client, _, _ = asana.get_asana_client()

        # リクエストパラメータ（任意）
        request_json = request.get_json(silent=True) if request is not None else None
        project_filter = None
        full_sync = False
        backfill_subtasks = False
        if request_json:
            project_filter = request_json.get('project_filter') or request_json.get('project_name')
            full_sync = bool(request_json.get('full_sync', False))
            backfill_subtasks = bool(request_json.get('backfill_subtasks', False))
            include_incomplete_subtasks = bool(request_json.get('include_incomplete_subtasks', False))
        else:
            include_incomplete_subtasks = False
        print(f"Request options: project_filter={project_filter}, full_sync={full_sync}, backfill_subtasks={backfill_subtasks}, include_incomplete_subtasks={include_incomplete_subtasks}")

        # BigQueryから最終更新日時を取得し、差分取得の起点にする（full_syncならNone）
        modified_since = None if full_sync else _get_last_modified_from_bq(bq_client)

        projects = asana.get_all_projects(api_client)
        if project_filter:
            projects = [p for p in projects if project_filter in p.get('name', '')]
        print(f"Found {len(projects)} projects to process.")

        all_tasks = []
        for i, project in enumerate(projects):
            print(f"[{i+1}/{len(projects)}] Processing project: {project['name']} ({project['gid']})")
            try:
                # `modified_since` を渡して、更新されたタスクのみを取得
                tasks = asana.get_completed_tasks_for_project(
                    api_client,
                    project,
                    modified_since=modified_since,
                    force_parent_sweep=backfill_subtasks,
                    completed_since_override='1970-01-01T00:00:00.000Z' if backfill_subtasks or full_sync else None,
                    include_incomplete_subtasks=include_incomplete_subtasks,
                )
                all_tasks.extend(tasks)
                # APIレート制限を避けるため、プロジェクトごとに短い待機を入れる
                time.sleep(0.5)
            except asana_sdk.rest.ApiException as e:
                print(f"  ERROR fetching tasks for project '{project['name']}': {e}")
                # 一つのプロジェクトでエラーが発生しても、他のプロジェクトの処理を続行
                continue

        print(f"\nTotal tasks collected/updated: {len(all_tasks)}")

        if all_tasks:
            bigquery.ensure_table_exists(bq_client)
            bigquery.insert_tasks(bq_client, all_tasks)
        else:
            print("No new or updated tasks to save.")

        print("--- Asana to BigQuery sync finished successfully ---")
        # Slack: health summary + daily digest (non-fatal)
        try:
            finished_at_iso = datetime.now(timezone.utc).isoformat()
            send_run_summary(
                tasks_processed=len(all_tasks),
                started_at_iso=started_at_iso,
                finished_at_iso=finished_at_iso,
                errors=0,
            )
            # 日次ダイジェスト（昨日, JST）
            send_daily_digest(bq_client)
        except Exception as e:
            print(f"Slack notifications skipped due to error: {e}")
        return {"status": "success", "tasks_processed": len(all_tasks)}, 200

    except Exception as e:
        import traceback
        print(f"FATAL Error in fetch_asana_tasks_to_bq: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}, 500


@functions_framework.http
def export_reports_to_sheets(request: Request):
    """
    BigQueryからデータを集計し、Google Sheetsに出力するCloud Function。
    """
    print("--- Starting BigQuery to Sheets export ---")
    try:
        config.validate_config()
        
        bq_client = bigquery.get_bigquery_client()
        report_data = bigquery.get_report_data(bq_client)

        sheets_service = sheets.get_sheets_service()
        
        for report_type, results in report_data.items():
            sheets.update_sheet_with_report(sheets_service, report_type, results)
            time.sleep(5)
            
        print("--- BigQuery to Sheets export finished successfully ---")
        # Slack: monthly digest (non-fatal)
        try:
            # optional request override
            request_json = request.get_json(silent=True) if request is not None else None
            force_monthly = bool(request_json.get('force_monthly_digest', False)) if request_json else False
            jst_now = datetime.now(ZoneInfo("Asia/Tokyo"))
            if force_monthly or jst_now.day == 1:
                send_monthly_digest(bq_client)
            else:
                print("Skipping monthly digest (not first day JST).")
        except Exception as e:
            print(f"Slack monthly digest skipped due to error: {e}")
        return "OK", 200

    except Exception as e:
        import traceback
        print(f"An error occurred in export_reports_to_sheets: {e}")
        traceback.print_exc()
        return "Error", 500


if __name__ == '__main__':
    """
    コマンドラインからローカルで実行するためのエントリポイント。
    例: python main.py fetch
    """
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'fetch':
            print("Running 'fetch_asana_tasks_to_bq' locally...")
            fetch_asana_tasks_to_bq(Request.from_values())
        elif command == 'export':
            print("Running 'export_reports_to_sheets' locally...")
            export_reports_to_sheets(Request.from_values())
        else:
            print(f"Unknown command: {command}. Use 'fetch' or 'export'.")
    else:
        print("Please provide a command: 'fetch' or 'export'.")
