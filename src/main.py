import os
import json
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# スプレッドシートのID
SPREADSHEET_ID = '1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ'
# 書き込むシート名
PROJECT_SHEET_NAME = 'プロジェクト別実績時間'
ASSIGNEE_SHEET_NAME = '担当者別実績時間'
PROJECT_ASSIGNEE_SHEET_NAME = 'プロジェクト担当者別実績時間'

def get_data_from_bigquery():
    """BigQueryからデータを取得する"""
    project_id = os.getenv('GCP_PROJECT_ID')
    client = bigquery.Client(project=project_id)
    
    print("データを取得します")

    # 共通テーブル式（CTE）を定義
    # 各タスクについて、最も適切な代表レコードを1つだけ選ぶ
    # 担当者名がNULLでないものを優先し、次に完了日が新しいものを優先
    base_query = """
    WITH task_representatives AS (
        SELECT
            task_id,
            ARRAY_AGG(
                t
                ORDER BY (CASE WHEN assignee_name IS NOT NULL AND assignee_name != '' THEN 0 ELSE 1 END), completed_at DESC
                LIMIT 1
            )[OFFSET(0)] AS representative
        FROM `asana-analytics-hub.asana_analytics.completed_tasks` t
        WHERE completed_at IS NOT NULL
        GROUP BY task_id
    ),
    unique_tasks AS (
        SELECT
            representative.task_id,
            representative.task_name,
            representative.project_id,
            representative.project_name,
            representative.assignee_name,
            representative.completed_at,
            representative.estimated_time,
            representative.actual_time,
            representative.actual_time_raw,
            FORMAT_TIMESTAMP("%Y-%m", representative.completed_at) as month
        FROM task_representatives
    )
    """
    
    # プロジェクト別の実績時間を取得するクエリ
    project_query = base_query + """
    -- プロジェクト別に集計
    SELECT 
        month,
        project_name, 
        COUNT(*) as tasks_count, 
        COUNTIF(actual_time IS NOT NULL) as tasks_with_actual, 
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        AVG(actual_time) as avg_actual_hours,
        COUNTIF(actual_time_raw IS NOT NULL) as tasks_with_actual_raw,
        SUM(IFNULL(actual_time_raw, 0))/60 as total_actual_raw_hours,
        AVG(IFNULL(actual_time_raw, 0))/60 as avg_actual_raw_hours,
        SUM(IFNULL(estimated_time, 0)) as total_estimated_hours,
        AVG(estimated_time) as avg_estimated_hours
    FROM 
        unique_tasks
    GROUP BY 
        month, project_name 
    ORDER BY 
        month DESC, total_actual_hours DESC
    """
    
    # 担当者別の実績時間を取得するクエリ
    assignee_query = base_query + """
    -- 担当者別に集計
    SELECT 
        month,
        assignee_name, 
        COUNT(*) as tasks_count, 
        COUNTIF(actual_time IS NOT NULL) as tasks_with_actual, 
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        AVG(actual_time) as avg_actual_hours,
        COUNTIF(actual_time_raw IS NOT NULL) as tasks_with_actual_raw,
        SUM(IFNULL(actual_time_raw, 0))/60 as total_actual_raw_hours,
        AVG(IFNULL(actual_time_raw, 0))/60 as avg_actual_raw_hours,
        SUM(IFNULL(estimated_time, 0)) as total_estimated_hours,
        AVG(estimated_time) as avg_estimated_hours
    FROM 
        unique_tasks
    WHERE
        assignee_name IS NOT NULL AND assignee_name != ''
    GROUP BY 
        month, assignee_name 
    ORDER BY 
        month DESC, total_actual_hours DESC
    """
    
    # プロジェクト担当者別の実績時間を取得するクエリ
    project_assignee_query = base_query + """
    -- プロジェクトと担当者の組み合わせごとに集計
    SELECT 
        month,
        project_name,
        assignee_name, 
        COUNT(*) as tasks_count, 
        COUNTIF(actual_time IS NOT NULL) as tasks_with_actual, 
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        AVG(actual_time) as avg_actual_hours,
        COUNTIF(actual_time_raw IS NOT NULL) as tasks_with_actual_raw,
        SUM(IFNULL(actual_time_raw, 0))/60 as total_actual_raw_hours,
        AVG(IFNULL(actual_time_raw, 0))/60 as avg_actual_raw_hours,
        SUM(IFNULL(estimated_time, 0)) as total_estimated_hours,
        AVG(estimated_time) as avg_estimated_hours
    FROM 
        unique_tasks
    WHERE
        assignee_name IS NOT NULL AND assignee_name != ''
    GROUP BY 
        month, project_name, assignee_name 
    ORDER BY 
        month DESC, project_name, total_actual_hours DESC
    """
    
    # プロジェクト別データの取得
    print("プロジェクト別データを取得中...")
    project_job = client.query(project_query)
    project_results = project_job.result()
    
    # 担当者別データの取得
    print("担当者別データを取得中...")
    assignee_job = client.query(assignee_query)
    assignee_results = assignee_job.result()
    
    # プロジェクト担当者別データの取得
    print("プロジェクト担当者別データを取得中...")
    project_assignee_job = client.query(project_assignee_query)
    project_assignee_results = project_assignee_job.result()
    
    # プロジェクト別データを整形
    project_data = []
    for row in project_results:
        project_data.append([
            row.project_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.tasks_with_actual_raw,
            row.total_actual_raw_hours if row.total_actual_raw_hours else 0,
            row.avg_actual_raw_hours if row.avg_actual_raw_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0,
            row.month,  # 対象期間はYYYY-MM形式
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新日時
        ])
    
    # デバッグ: 最初の5件のプロジェクトデータを表示
    print("\n===== プロジェクトデータのサンプル（最初の5件）=====")
    for i, row in enumerate(project_data[:5]):
        print(f"行 {i+1}: {row}")
    
    # デバッグ: 月別のデータ集計を表示
    print("\n===== 月別のプロジェクトデータ集計 =====")
    month_stats = {}
    for row in project_data:
        month = row[7]  # 対象期間（YYYY-MM）
        if month not in month_stats:
            month_stats[month] = {
                'tasks_count': 0,
                'tasks_with_actual': 0,
                'total_actual_hours': 0
            }
        month_stats[month]['tasks_count'] += row[1]  # タスク数
        month_stats[month]['tasks_with_actual'] += row[2]  # 実績時間あり
        month_stats[month]['total_actual_hours'] += row[3]  # 合計実績時間
    
    # 月別に表示（降順）
    for month in sorted(month_stats.keys(), reverse=True):
        stats = month_stats[month]
        print(f"月: {month}, タスク数: {stats['tasks_count']}, 実績時間あり: {stats['tasks_with_actual']}, 合計実績時間: {stats['total_actual_hours']}")
    
    # 担当者別データを整形
    assignee_data = []
    for row in assignee_results:
        assignee_data.append([
            row.assignee_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.tasks_with_actual_raw,
            row.total_actual_raw_hours if row.total_actual_raw_hours else 0,
            row.avg_actual_raw_hours if row.avg_actual_raw_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0,
            row.month,  # 対象期間はYYYY-MM形式
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新日時
        ])
    
    # プロジェクト担当者別データを整形
    project_assignee_data = []
    for row in project_assignee_results:
        project_assignee_data.append([
            row.project_name,
            row.assignee_name,
            row.tasks_count,
            row.tasks_with_actual,
            row.total_actual_hours if row.total_actual_hours else 0,
            row.avg_actual_hours if row.avg_actual_hours else 0,
            row.tasks_with_actual_raw,
            row.total_actual_raw_hours if row.total_actual_raw_hours else 0,
            row.avg_actual_raw_hours if row.avg_actual_raw_hours else 0,
            row.total_estimated_hours if row.total_estimated_hours else 0,
            row.avg_estimated_hours if row.avg_estimated_hours else 0,
            row.month,  # 対象期間はYYYY-MM形式
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 更新日時
        ])
    
    # ヘッダー行を定義
    project_header = [
        "プロジェクト名", 
        "タスク数", 
        "実績時間あり（計算値）", 
        "合計実績時間（計算値）", 
        "平均実績時間（計算値）", 
        "実績時間あり（生値）", 
        "合計実績時間（生値）", 
        "平均実績時間（生値）", 
        "合計見積時間", 
        "平均見積時間",
        "対象期間",
        "最終更新日時"
    ]
    
    assignee_header = [
        "担当者名", 
        "タスク数", 
        "実績時間あり（計算値）", 
        "合計実績時間（計算値）", 
        "平均実績時間（計算値）", 
        "実績時間あり（生値）", 
        "合計実績時間（生値）", 
        "平均実績時間（生値）", 
        "合計見積時間", 
        "平均見積時間",
        "対象期間",
        "最終更新日時"
    ]
    
    project_assignee_header = [
        "プロジェクト名",
        "担当者名", 
        "タスク数", 
        "実績時間あり（計算値）", 
        "合計実績時間（計算値）", 
        "平均実績時間（計算値）", 
        "実績時間あり（生値）", 
        "合計実績時間（生値）", 
        "平均実績時間（生値）", 
        "合計見積時間", 
        "平均見積時間",
        "対象期間",
        "最終更新日時"
    ]
    
    return {
        'project': [project_header] + project_data,
        'assignee': [assignee_header] + assignee_data,
        'project_assignee': [project_assignee_header] + project_assignee_data
    }

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

def update_spreadsheet(service, sheet_name, data):
    """指定したシートにデータを書き込む"""
    try:
        # シートの存在確認と作成
        if not ensure_sheet_exists(service, sheet_name):
            return False
        
        print(f"シート '{sheet_name}' にデータを書き込みます...")
        
        try:
            # シートをクリア
            clear_request = service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{sheet_name}!A1:Z10000'  # より多くの行をカバー
            )
            clear_request.execute()
            print(f"シート '{sheet_name}' をクリアしました。")
            
            # APIのレート制限を回避するために少し待機
            time.sleep(2)
        except HttpError as clear_error:
            print(f"シート '{sheet_name}' のクリア中にエラーが発生: {clear_error}")
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
                range=f'{sheet_name}!A1',
                valueInputOption='RAW',
                body=body
            ).execute()
            
            updated_cells = result.get('updatedCells', 0)
            print(f"{sheet_name}: {updated_cells} セルを更新しました。")
            
            return True
        except HttpError as update_error:
            print(f"シート '{sheet_name}' の更新中にエラーが発生: {update_error}")
            if "Quota exceeded" in str(update_error):
                print("APIレート制限を超過しました。待機してから再度お試しください。")
            return False
    
    except Exception as e:
        print(f"シート '{sheet_name}' の更新中に予期せぬエラーが発生: {e}")
        return False

def main():
    """メイン処理"""
    print("BigQueryからデータを取得しています...")
    all_data = get_data_from_bigquery()
    
    print(f"取得したプロジェクトデータ: {len(all_data['project']) - 1}行")
    print(f"取得した担当者データ: {len(all_data['assignee']) - 1}行")
    print(f"取得したプロジェクト担当者別データ: {len(all_data['project_assignee']) - 1}行")
    
    print("Google Sheetsにデータを書き込んでいます...")
    
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
        
        # プロジェクト別データを書き込み
        project_success = update_spreadsheet(service, PROJECT_SHEET_NAME, all_data['project'])
        
        # APIのレート制限を回避するために少し待機
        time.sleep(5)
        
        # 担当者別データを書き込み
        assignee_success = update_spreadsheet(service, ASSIGNEE_SHEET_NAME, all_data['assignee'])
        
        # APIのレート制限を回避するために少し待機
        time.sleep(5)
        
        # プロジェクト担当者別データを書き込み
        project_assignee_success = update_spreadsheet(service, PROJECT_ASSIGNEE_SHEET_NAME, all_data['project_assignee'])
        
        if project_success and assignee_success and project_assignee_success:
            print("すべてのデータの書き込みが完了しました。")
        else:
            print("一部のデータの書き込みに失敗しました。")
            
    except HttpError as error:
        print(f"エラーが発生しました: {error}")
        
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")

def export_sheets_entrypoint(request):
    """Cloud Functionのエントリーポイント"""
    main()
    return "OK", 200

if __name__ == "__main__":
    load_dotenv()
    main() 