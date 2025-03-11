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
    
    # 月ごとにデータを整理
    monthly_data = {}
    for row in results:
        month = row.month
        if month not in monthly_data:
            monthly_data[month] = []
        
        monthly_data[month].append([
            row.project_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0
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
    
    # 現在の日時
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 各月のデータにヘッダーと対象期間を追加
    formatted_data = {}
    for month, data in monthly_data.items():
        # 月の表示形式を整える（例: 2024-03 → 2024年3月）
        year, month_num = month.split('-')
        year_int = int(year)
        month_int = int(month_num)
        
        # シート名用の表示形式
        display_month = f"{year}年{month_int}月"
        
        # 対象期間はフィルター用に「YYYY-MM」形式のまま使用
        period = month
        
        # 各行に対象期間と更新日時を追加
        for row in data:
            row.append(period)
            row.append(current_time)
        
        # ヘッダー行を追加
        formatted_data[month] = [header] + data
    
    return formatted_data

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

def update_monthly_spreadsheets(monthly_data):
    """月ごとのデータをGoogle Sheetsの別々のシートに書き込む"""
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
        
        total_updated_cells = 0
        success_count = 0
        error_count = 0
        
        # 月ごとにシートを更新
        for month, data in monthly_data.items():
            try:
                # シート名を設定（例: 2024-03 → 2024年3月）
                year, month_num = month.split('-')
                sheet_name = f"{year}年{int(month_num)}月"
                
                print(f"シート '{sheet_name}' を処理中...")
                
                # シートの存在確認と作成
                if not ensure_sheet_exists(service, sheet_name):
                    error_count += 1
                    continue
                
                # APIのレート制限を回避するために少し待機
                time.sleep(2)
                
                # シートをクリア
                try:
                    clear_request = service.spreadsheets().values().clear(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f'{sheet_name}!A1:Z1000'
                    )
                    clear_request.execute()
                    
                    # APIのレート制限を回避するために少し待機
                    time.sleep(2)
                except HttpError as clear_error:
                    print(f"シート '{sheet_name}' のクリア中にエラーが発生: {clear_error}")
                    if "Quota exceeded" in str(clear_error):
                        print("APIレート制限を超過しました。60秒待機します...")
                        time.sleep(60)
                
                # データを書き込む
                try:
                    body = {
                        'values': data
                    }
                    result = service.spreadsheets().values().update(
                        spreadsheetId=SPREADSHEET_ID,
                        range=f'{sheet_name}!A1',
                        valueInputOption='RAW',
                        body=body
                    ).execute()
                    
                    updated_cells = result.get('updatedCells', 0)
                    total_updated_cells += updated_cells
                    print(f"{sheet_name}: {updated_cells} セルを更新しました。")
                    success_count += 1
                    
                    # APIのレート制限を回避するために少し待機
                    time.sleep(2)
                except HttpError as update_error:
                    print(f"シート '{sheet_name}' の更新中にエラーが発生: {update_error}")
                    error_count += 1
                    if "Quota exceeded" in str(update_error):
                        print("APIレート制限を超過しました。60秒待機します...")
                        time.sleep(60)
            
            except Exception as e:
                print(f"シート '{month}' の処理中に予期せぬエラーが発生: {e}")
                error_count += 1
        
        print(f"合計 {total_updated_cells} セルを更新しました。")
        print(f"成功: {success_count}シート、エラー: {error_count}シート")
        return error_count == 0
    
    except HttpError as error:
        print(f"エラーが発生しました: {error}")
        return False

def main():
    """メイン処理"""
    print("BigQueryから月ごとのデータを取得しています...")
    monthly_data = get_monthly_data_from_bigquery()
    
    print(f"取得した月数: {len(monthly_data)}ヶ月")
    for month, data in monthly_data.items():
        print(f"- {month}: {len(data) - 1}行のデータ")
    
    print("Google Sheetsに月ごとのデータを書き込んでいます...")
    success = update_monthly_spreadsheets(monthly_data)
    
    if success:
        print("データの書き込みが完了しました。")
    else:
        print("データの書き込みに失敗しました。")

if __name__ == "__main__":
    main() 