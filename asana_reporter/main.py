import sys
import time
from typing import List, Dict, Any
from datetime import datetime, timezone
import functions_framework
from flask import Request

# ローカル実行時に `PYTHONPATH=.` を使わずに済むようにパスを追加
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import asana, bigquery, sheets, config

def _fetch_all_tasks() -> List[Dict[str, Any]]:
    """全てのプロジェクトから完了タスクを取得してリストにまとめる"""
    api_client, _, _ = asana.get_asana_client()
    projects = asana.get_all_projects(api_client)
    
    all_tasks = []
    for project in projects:
        tasks = asana.get_completed_tasks_for_project(api_client, project)
        all_tasks.extend(tasks)
        time.sleep(1) # APIレート制限を避けるための短い待機
    
    return all_tasks

# --- Cloud Function Entrypoints ---

@functions_framework.http
def fetch_asana_tasks_to_bq(request):
    """Asanaからタスクを取得してBigQueryに保存する（差分取得・バッチ処理対応）"""
    try:
        # リクエストパラメータを取得
        request_json = request.get_json(silent=True)
        if request_json:
            project_filter = request_json.get('project_filter')
            incremental = request_json.get('incremental', False)
            batch_size = request_json.get('batch_size', 10)  # デフォルト10プロジェクトずつ
            batch_number = request_json.get('batch_number', 0)  # バッチ番号
        else:
            project_filter = None
            incremental = False
            batch_size = 10
            batch_number = 0
        
        print(f"Starting fetch with project_filter: {project_filter}, incremental: {incremental}, batch_size: {batch_size}, batch_number: {batch_number}")
        
        # Asanaクライアントを取得
        api_client, projects_api, tasks_api = asana.get_asana_client()
        
        # プロジェクト一覧を取得
        if project_filter:
            # 特定のプロジェクトのみ処理
            all_projects = asana.get_all_projects(api_client)
            projects = [p for p in all_projects if project_filter in p['name']]
            print(f"Filtered to {len(projects)} projects matching '{project_filter}'")
        else:
            # 全プロジェクト処理
            projects = asana.get_all_projects(api_client)
            print(f"Processing all {len(projects)} projects")
        
        # バッチ処理
        if batch_size > 0:
            start_idx = batch_number * batch_size
            end_idx = start_idx + batch_size
            projects = projects[start_idx:end_idx]
            print(f"Processing batch {batch_number}: projects {start_idx+1}-{min(end_idx, len(projects))}")
        
        # 差分取得の場合、最後の更新時刻を取得
        last_update = None
        if incremental:
            try:
                from google.cloud import bigquery as gcp_bigquery
                client = gcp_bigquery.Client()
                query = """
                SELECT MAX(inserted_at) as last_update 
                FROM `asana-analytics-hub.asana_analytics.completed_tasks`
                """
                result = client.query(query).result()
                for row in result:
                    last_update = row.last_update
                    print(f"Last update: {last_update}")
            except Exception as e:
                print(f"Error getting last update: {e}")
                incremental = False
        
        # タスクを取得
        all_tasks = []
        for i, project in enumerate(projects):
            print(f"Processing project {i+1}/{len(projects)}: {project['name']}")
            
            # 差分取得の場合、プロジェクトの最終更新をチェック
            if incremental and last_update:
                # プロジェクトの最終更新時刻をチェック（簡易版）
                project_tasks = asana.get_completed_tasks_for_project(api_client, project)
                if project_tasks:
                    def _to_dt(s: str):
                        if not s:
                            return None
                        try:
                            # Asanaの日時はISO 8601 (例: 2024-06-01T12:34:56.789Z)
                            if s.endswith('Z'):
                                s = s[:-1] + '+00:00'
                            return datetime.fromisoformat(s)
                        except Exception:
                            return None
                    completed_times = [
                        _to_dt(task.get('completed_at')) for task in project_tasks
                    ]
                    completed_times = [dt for dt in completed_times if dt is not None]
                    if completed_times:
                        latest_task_dt = max(completed_times)
                        if latest_task_dt <= last_update:
                            print(f"Skipping {project['name']} - no updates since {last_update}")
                            continue
            
            tasks = asana.get_completed_tasks_for_project(api_client, project)
            all_tasks.extend(tasks)
            
            # 進捗表示
            if (i + 1) % 5 == 0:
                print(f"Progress: {i+1}/{len(projects)} projects processed")
        
        print(f"Total tasks collected: {len(all_tasks)}")
        
        # BigQueryに保存
        if all_tasks:
            bq_client = bigquery.get_bigquery_client()
            bigquery.ensure_table_exists(bq_client)
            bigquery.insert_tasks(bq_client, all_tasks)
            print(f"Successfully saved {len(all_tasks)} tasks to BigQuery")
        else:
            print("No tasks to save")
        
        return {
            'status': 'success', 
            'tasks_processed': len(all_tasks),
            'batch_number': batch_number,
            'batch_size': batch_size,
            'total_projects': len(projects)
        }
        
    except Exception as e:
        print(f"Error in fetch_asana_tasks_to_bq: {e}")
        return {'status': 'error', 'message': str(e)}

@functions_framework.http
def export_reports_to_sheets(request: Request):
    """
    BigQueryからデータを集計し、Google Sheetsに出力するCloud Function。
    """
    print("--- Starting BigQuery to Sheets export ---")
    try:
        config.validate_config()
        
        # 1. BigQueryからレポートデータを取得
        bq_client = bigquery.get_bigquery_client()
        report_data = bigquery.get_report_data(bq_client)

        # 2. Google Sheetsサービスを準備
        sheets_service = sheets.get_sheets_service()
        
        # 3. 各レポートをシートに書き込み
        for report_type, results in report_data.items():
            sheets.update_sheet_with_report(sheets_service, report_type, results)
            time.sleep(5) # APIレート制限を避けるための待機
            
        print("--- BigQuery to Sheets export finished successfully ---")
        return "OK", 200

    except Exception as e:
        print(f"An error occurred in export_reports_to_sheets: {e}")
        import traceback
        traceback.print_exc()
        return "Error", 500

# --- Local Execution ---
if __name__ == '__main__':
    """
    コマンドラインからローカルで実行するためのエントリポイント。
    'fetch' または 'export' を引数に指定します。
    例: python asana_reporter/main.py fetch
    """
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'fetch':
            print("Running 'fetch_asana_tasks_to_bq' locally...")
            # ローカル実行用のダミーリクエストオブジェクト
            class DummyRequest:
                pass
            fetch_asana_tasks_to_bq(DummyRequest())
        elif command == 'export':
            print("Running 'export_reports_to_sheets' locally...")
            export_reports_to_sheets(DummyRequest())
        else:
            print(f"Unknown command: {command}. Use 'fetch' or 'export'.")
    else:
        print("Please provide a command: 'fetch' or 'export'.")