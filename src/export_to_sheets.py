import os
import json
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# スプレッドシートのID
SPREADSHEET_ID = '1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ'
# 書き込むシート名
SHEET_NAME = 'プロジェクト別実績時間'

def get_monthly_data_from_bigquery():
    """BigQueryから月ごとのデータを取得する"""
    project_id = os.getenv('GCP_PROJECT_ID')
    client = bigquery.Client(project=project_id)
    
    print("月ごとのデータを取得します")
    
    # 月ごとのプロジェクト別実績時間を取得するクエリ
    query = """
    SELECT 
        FORMAT_TIMESTAMP("%Y-%m", completed_at) as month,
        project_name, 
        COUNT(*) as tasks_count, 
        COUNT(actual_time) as tasks_with_actual, 
        SUM(actual_time) as total_actual_hours,
        AVG(actual_time) as avg_actual_hours,
        SUM(estimated_time) as total_estimated_hours,
        AVG(estimated_time) as avg_estimated_hours
    FROM 
        `asana-analytics-hub.asana_analytics.completed_tasks` 
    WHERE
        completed_at IS NOT NULL
    GROUP BY 
        month, project_name 
    ORDER BY 
        month DESC, total_actual_hours DESC
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    # 全てのデータを単一のリストにまとめる
    all_data = []
    for row in results:
        month = row.month
        all_data.append([
            row.project_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0,
            month,  # 対象期間はYYYY-MM形式
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新日時
        ])
    
    # ヘッダー行を定義
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
    
    return [header] + all_data

def ensure_sheet_exists(service, sheet_name):
    """シートが存在することを確認し、存在しない場合は作成する"""
    try:
        # スプレッドシートの情報を取得
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        
        # シートの存在確認
        sheet_exists = False
        for sheet in spreadsheet.get('sheets', []):
            if sheet.get('properties', {}).get('title') == sheet_name:
                sheet_exists = True
                break
        
        # シートが存在しない場合は作成
        if not sheet_exists:
            print(f"シート '{sheet_name}' が存在しないため、作成します。")
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request_body
            ).execute()
            print(f"シート '{sheet_name}' を作成しました。")
        
        return True
    
    except HttpError as error:
        print(f"シート '{sheet_name}' の確認中にエラーが発生しました: {error}")
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
        if not ensure_sheet_exists(service, SHEET_NAME):
            return False
        
        print(f"シート '{SHEET_NAME}' にデータを書き込みます...")
        
        try:
            # シートをクリア
            clear_request = service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{SHEET_NAME}!A1:Z10000'  # より多くの行をカバー
            )
            clear_request.execute()
            print(f"シート '{SHEET_NAME}' をクリアしました。")
            
            # APIのレート制限を回避するために少し待機
            time.sleep(2)
        except HttpError as clear_error:
            print(f"シート '{SHEET_NAME}' のクリア中にエラーが発生: {clear_error}")
            if "Quota exceeded" in str(clear_error):
                print("APIレート制限を超過しました。60秒待機します...")
                time.sleep(60)
        
        try:
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
            
            updated_cells = result.get('updatedCells', 0)
            print(f"{SHEET_NAME}: {updated_cells} セルを更新しました。")
            
            return True
        except HttpError as update_error:
            print(f"シート '{SHEET_NAME}' の更新中にエラーが発生: {update_error}")
            if "Quota exceeded" in str(update_error):
                print("APIレート制限を超過しました。待機してから再度お試しください。")
            return False
    
    except HttpError as error:
        print(f"エラーが発生しました: {error}")
        return False

def main():
    """メイン処理"""
    print("BigQueryから月ごとのデータを取得しています...")
    data = get_monthly_data_from_bigquery()
    
    print(f"取得したデータ: {len(data) - 1}行")
    
    print("Google Sheetsにデータを書き込んでいます...")
    success = update_spreadsheet(data)
    
    if success:
        print("データの書き込みが完了しました。")
    else:
        print("データの書き込みに失敗しました。")

if __name__ == "__main__":
    main() 