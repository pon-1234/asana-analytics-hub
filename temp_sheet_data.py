from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from datetime import datetime

try:
    # サービスアカウントの認証情報を取得
    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    print(f"認証ファイル: {creds_file}")
    
    creds = service_account.Credentials.from_service_account_file(
        creds_file, 
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    
    print(f"サービスアカウント: {creds.service_account_email}")
    
    # Google Sheets APIのクライアントを作成
    service = build('sheets', 'v4', credentials=creds)
    
    # スプレッドシートの情報を取得
    spreadsheet_id = '1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ'
    
    # プロジェクト別実績時間シートのデータを取得（最終更新日時を確認するため）
    sheet_name = 'プロジェクト別実績時間'
    range_name = f'{sheet_name}!A1:L20'  # ヘッダー行と最初の数行を取得
    
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    
    rows = result.get('values', [])
    print(f"取得した行数: {len(rows)}")
    
    if rows:
        # ヘッダー行を表示
        print(f"ヘッダー行: {rows[0]}")
        
        # 最終更新日時のカラムを探す
        header = rows[0]
        update_time_index = -1
        
        for i, col in enumerate(header):
            if '最終更新日時' in col:
                update_time_index = i
                break
        
        # 最新のデータを表示
        if update_time_index >= 0 and len(rows) > 1:
            latest_update_times = []
            
            for i in range(1, min(6, len(rows))):
                if len(rows[i]) > update_time_index:
                    row = rows[i]
                    project = row[0] if len(row) > 0 else "不明"
                    update_time = row[update_time_index] if len(row) > update_time_index else "不明"
                    latest_update_times.append((project, update_time))
                    print(f"プロジェクト: {project}, 最終更新日時: {update_time}")
        else:
            print("最終更新日時のカラムが見つからないか、データがありません。")
    else:
        print("データが取得できませんでした。")
    
except Exception as e:
    print(f"エラーが発生しました: {e}") 