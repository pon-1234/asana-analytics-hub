import os
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# スプレッドシートのID
SPREADSHEET_ID = '1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ'
# 書き込むシート名
SHEET_NAME = 'プロジェクト別実績時間'

def get_data_from_bigquery():
    """BigQueryからデータを取得する"""
    project_id = os.getenv('GCP_PROJECT_ID')
    client = bigquery.Client(project=project_id)
    
    # 期間の制限を削除し、全期間のデータを取得
    print("全期間のデータを取得します")
    
    # プロジェクト別の実績時間を取得するクエリ（全期間）
    query = """
    SELECT 
        project_name, 
        COUNT(*) as tasks_count, 
        COUNT(actual_time) as tasks_with_actual, 
        SUM(actual_time) as total_actual_hours,
        AVG(actual_time) as avg_actual_hours,
        SUM(estimated_time) as total_estimated_hours,
        AVG(estimated_time) as avg_estimated_hours
    FROM 
        `asana-analytics-hub.asana_analytics.completed_tasks` 
    GROUP BY 
        project_name 
    ORDER BY 
        total_actual_hours DESC
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    # 結果をリストに変換
    data = []
    for row in results:
        data.append([
            row.project_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0
        ])
    
    # ヘッダー行を追加
    header = [
        "プロジェクト名", 
        "タスク数", 
        "実績時間あり", 
        "合計実績時間", 
        "平均実績時間", 
        "合計見積時間", 
        "平均見積時間",
        "対象期間",
        "最終更新日時"
    ]
    
    # 現在の日時と対象期間を追加
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    period = "全期間"
    for row in data:
        row.append(period)
        row.append(current_time)
    
    return [header] + data

def ensure_sheet_exists(service):
    """シートが存在することを確認し、存在しない場合は作成する"""
    try:
        # スプレッドシートの情報を取得
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        
        # シートの存在確認
        sheet_exists = False
        for sheet in spreadsheet.get('sheets', []):
            if sheet.get('properties', {}).get('title') == SHEET_NAME:
                sheet_exists = True
                break
        
        # シートが存在しない場合は作成
        if not sheet_exists:
            print(f"シート '{SHEET_NAME}' が存在しないため、作成します。")
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': SHEET_NAME
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request_body
            ).execute()
            print(f"シート '{SHEET_NAME}' を作成しました。")
        
        return True
    
    except HttpError as error:
        print(f"シートの確認中にエラーが発生しました: {error}")
        return False

def update_spreadsheet(data):
    """Google Sheetsにデータを書き込む"""
    # 認証情報の取得
    try:
        # サービスアカウントのJSONファイルパス
        creds_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # サービスアカウントの認証情報を取得
        creds = service_account.Credentials.from_service_account_file(
            creds_file, 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        
        # Google Sheets APIのクライアントを作成
        service = build('sheets', 'v4', credentials=creds)
        
        # シートの存在確認と作成
        if not ensure_sheet_exists(service):
            return False
        
        # シートをクリア
        clear_request = service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A1:Z1000'
        )
        clear_request.execute()
        
        # データを書き込む
        body = {
            'values': data
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"{result.get('updatedCells')} セルを更新しました。")
        return True
    
    except HttpError as error:
        print(f"エラーが発生しました: {error}")
        return False

def main():
    """メイン処理"""
    print("BigQueryからデータを取得しています...")
    data = get_data_from_bigquery()
    
    print(f"取得したデータ: {len(data) - 1}行")
    
    print("Google Sheetsにデータを書き込んでいます...")
    success = update_spreadsheet(data)
    
    if success:
        print("データの書き込みが完了しました。")
    else:
        print("データの書き込みに失敗しました。")

if __name__ == "__main__":
    main() 