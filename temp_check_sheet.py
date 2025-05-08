from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

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
    result = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    print(f"スプレッドシート名: {result.get('properties', {}).get('title')}")
    print(f"最終更新日時: {result.get('properties', {}).get('updatedTime')}")
    
    # スプレッドシートのシート一覧を取得
    sheets = result.get('sheets', [])
    print(f"シート数: {len(sheets)}")
    
    for sheet in sheets:
        sheet_name = sheet.get('properties', {}).get('title')
        sheet_id = sheet.get('properties', {}).get('sheetId')
        print(f"シート名: {sheet_name}, ID: {sheet_id}")
    
except Exception as e:
    print(f"エラーが発生しました: {e}") 