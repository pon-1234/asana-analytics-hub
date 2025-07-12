import sys
import time
from typing import List, Dict, Any
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
def fetch_asana_tasks_to_bq(request: Request):
    """
    Asanaからデータを取得し、BigQueryに保存するCloud Function。
    HTTPトリガーまたは直接呼び出しで実行可能。
    """
    print("--- Starting Asana to BigQuery sync ---")
    try:
        config.validate_config()
        
        # 1. Asanaからタスクを取得
        tasks = _fetch_all_tasks()
        if not tasks:
            print("No completed tasks found to sync.")
            return "OK: No new tasks.", 200

        # 2. BigQueryクライアントを準備し、テーブルを確保
        bq_client = bigquery.get_bigquery_client()
        bigquery.ensure_table_exists(bq_client)

        # 3. BigQueryにデータを挿入
        bigquery.insert_tasks(bq_client, tasks)
        
        print("--- Asana to BigQuery sync finished successfully ---")
        return "OK", 200

    except Exception as e:
        print(f"An error occurred in fetch_asana_tasks_to_bq: {e}")
        # エラーをログに出力するために、スタックトレースも表示するとデバッグしやすい
        import traceback
        traceback.print_exc()
        return "Error", 500

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