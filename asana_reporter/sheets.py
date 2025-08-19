import time
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from typing import List, Any, Iterator, Dict

from . import config

# 各レポートシートの名前
SHEET_NAMES = {
    'project': 'プロジェクト別実績時間',
    'assignee': '担当者別実績時間',
    'project_assignee': 'プロジェクト担当者別実績時間'
}

def get_sheets_service() -> Resource:
    """Google Sheets APIサービスを初期化して返す"""
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    if config.GCP_CREDENTIALS_PATH:
        # ローカル実行時
        creds = service_account.Credentials.from_service_account_file(
            config.GCP_CREDENTIALS_PATH, scopes=scopes
        )
    else:
        # GCP環境
        from google.auth import default
        creds, _ = default(scopes=scopes)
        
    return build('sheets', 'v4', credentials=creds)

def _ensure_sheet_exists(service: Resource, sheet_name: str):
    """シートが存在しない場合に作成する"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=config.SPREADSHEET_ID).execute()
        if not any(s['properties']['title'] == sheet_name for s in spreadsheet.get('sheets', [])):
            print(f"Sheet '{sheet_name}' not found, creating it...")
            body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
            service.spreadsheets().batchUpdate(
                spreadsheetId=config.SPREADSHEET_ID, body=body
            ).execute()
    except HttpError as error:
        print(f"An error occurred while ensuring sheet exists: {error}")
        raise

def _format_data_for_sheet(report_type: str, results: Iterator[Dict[str, Any]]) -> List[List[Any]]:
    """BigQueryの結果をシートに書き込む形式に整形する"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = []
    
    if report_type == 'project':
        header = ["対象期間", "プロジェクト名", "完了タスク数", "合計実績時間", "合計見積時間", "最終更新日時"]
        data.append(header)
        for row in results:
            data.append([
                row.month, row.project_name, row.tasks_count,
                round(row.total_actual_hours or 0, 2),
                round(row.total_estimated_hours or 0, 2),
                now
            ])
    elif report_type == 'assignee':
        header = ["対象期間", "担当者名", "完了タスク数", "合計実績時間", "合計見積時間", "最終更新日時"]
        data.append(header)
        for row in results:
            data.append([
                row.month, row.assignee_name, row.tasks_count,
                round(row.total_actual_hours or 0, 2),
                round(row.total_estimated_hours or 0, 2),
                now
            ])
    elif report_type == 'project_assignee':
        header = ["対象期間", "プロジェクト名", "担当者名", "完了タスク数", "合計実績時間", "合計見積時間", "最終更新日時"]
        data.append(header)
        for row in results:
            data.append([
                row.month, row.project_name, row.assignee_name, row.tasks_count,
                round(row.total_actual_hours or 0, 2),
                round(row.total_estimated_hours or 0, 2),
                now
            ])
            
    return data

def update_sheet_with_report(service: Resource, report_type: str, results: Iterator[Dict[str, Any]]):
    """指定されたレポートタイプのシートをデータで更新する"""
    sheet_name = SHEET_NAMES[report_type]
    print(f"Updating sheet: '{sheet_name}'")
    
    _ensure_sheet_exists(service, sheet_name)
    
    data = _format_data_for_sheet(report_type, results)

    if len(data) <= 1:
        print(f"No data to update for '{sheet_name}'.")
        # ヘッダーだけでもクリア＆更新したい場合は以下を有効化
        # body = {'values': [data[0]] if data else []}
        return

    body = {'values': data}
    
    # APIのレート制限を考慮し、複数回のリクエストに分ける
    for i in range(3): # 最大3回リトライ
        try:
            # 1. シートをクリア
            service.spreadsheets().values().clear(
                spreadsheetId=config.SPREADSHEET_ID, range=sheet_name
            ).execute()
            
            # 2. データを更新
            result = service.spreadsheets().values().update(
                spreadsheetId=config.SPREADSHEET_ID,
                range=f'{sheet_name}!A1',
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            print(f"Successfully updated {result.get('updatedCells', 0)} cells in '{sheet_name}'.")
            return
        except HttpError as error:
            if "RESOURCE_EXHAUSTED" in str(error) or "Quota exceeded" in str(error):
                wait_time = (i + 1) * 10
                print(f"Quota exceeded. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"An error occurred while updating sheet '{sheet_name}': {error}")
                raise
    print(f"Failed to update sheet '{sheet_name}' after multiple retries.")