import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core import retry

# 環境変数の読み込み
load_dotenv()

def get_completed_tasks(project_id, project_name):
    """指定されたプロジェクトの完了タスクを取得"""
    headers = {
        'Authorization': f'Bearer {os.getenv("ASANA_ACCESS_TOKEN")}'
    }
    
    url = f'https://app.asana.com/api/1.0/projects/{project_id}/tasks'
    params = {
        'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee,custom_fields,custom_fields.name,custom_fields.number_value,custom_fields.display_value,custom_fields.type'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        tasks = response.json()['data']
        completed_tasks = []  # 完了タスクを格納するリスト
        # プロジェクト情報を各タスクに追加
        for task in tasks:
            task['project'] = {
                'gid': project_id,
                'name': project_name
            }
            # カスタムフィールドの処理
            task['estimated_time'] = None
            task['actual_time'] = None
            task['actual_time_raw'] = None  # actual_time_raw（分単位）フィールドを初期化
            has_actual_time_raw = False  # actual_time_rawフィールドの有無を追跡

            for field in task.get('custom_fields', []):
                print(f"Debug - Field: {field['name']}, Type: {field.get('type')}, Value: {field.get('number_value')}, Display Value: {field.get('display_value')}")
                if field['name'] == 'Estimated time' and field.get('number_value') is not None:
                    task['estimated_time'] = field['number_value']
                elif field['name'] == 'actual_time_raw' and field.get('number_value') is not None:
                    # 直接記録された実績時間を使用（分単位）
                    task['actual_time_raw'] = field['number_value']  # actual_time_rawフィールドを保存（分単位）
                    task['actual_time'] = field['number_value'] / 60  # 時間単位に変換
                    has_actual_time_raw = True
                    print(f"Debug - Using actual_time_raw: {task['actual_time_raw']}分 ({task['actual_time']}時間)")
                elif field['name'] == '時間達成率' and field.get('number_value') is not None and not has_actual_time_raw:
                    # actual_time_rawがない場合のみ、時間達成率から実績時間を計算
                    achievement_rate = field['number_value']
                    if task['estimated_time'] is not None and achievement_rate > 0:
                        task['actual_time'] = task['estimated_time'] * achievement_rate
                        print(f"Debug - Calculated actual_time: {task['actual_time']} (estimated: {task['estimated_time']} * rate: {achievement_rate})")

            # デバッグ出力を追加
            if task.get('gid') == '1209421344217855':
                print("Debug - Custom Fields for task:", task.get('name'))
                for cf in task.get('custom_fields', []):
                    print(f"Field Name: {cf.get('name')}, Value: {cf.get('display_value')}")

            # 完了タスクのみを保存
            if task.get('completed', False) and task.get('completed_at'):
                completed_tasks.append(task)
        return completed_tasks
    else:
        print(f"エラーが発生しました: {response.status_code}")
        print(response.text)
        return []

def create_bigquery_table():
    """BigQueryにテーブルを作成"""
    project_id = os.getenv('GCP_PROJECT_ID')
    client = bigquery.Client(project=project_id)
    dataset_id = "asana_analytics"
    table_id = "completed_tasks"
    
    # データセットの作成（存在しない場合）
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast1"
        client.create_dataset(dataset)
    
    # テーブルのスキーマ定義
    schema = [
        bigquery.SchemaField("task_id", "STRING"),
        bigquery.SchemaField("task_name", "STRING"),
        bigquery.SchemaField("project_id", "STRING"),
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("assignee_name", "STRING"),
        bigquery.SchemaField("completed_at", "TIMESTAMP"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("due_on", "DATE"),
        bigquery.SchemaField("modified_at", "TIMESTAMP"),
        bigquery.SchemaField("inserted_at", "TIMESTAMP"),
        bigquery.SchemaField("estimated_time", "FLOAT"),
        bigquery.SchemaField("actual_time", "FLOAT"),
        bigquery.SchemaField("actual_time_raw", "FLOAT")  # 分単位の実績時間
    ]
    
    # テーブルの作成（存在しない場合）
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

@retry.Retry(predicate=retry.if_transient_error)
def insert_tasks_to_bigquery(tasks):
    """タスクデータをBigQueryに挿入"""
    project_id = os.getenv('GCP_PROJECT_ID')
    client = bigquery.Client(project=project_id)
    dataset_id = "asana_analytics"
    table_id = "completed_tasks"
    
    # 既存のタスクIDを取得して重複を防止
    existing_task_ids = set()
    query = f"""
    SELECT DISTINCT task_id 
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    for row in query_job:
        existing_task_ids.add(row.task_id)
    
    print(f"既存のタスク数: {len(existing_task_ids)}")
    
    rows_to_insert = []
    skipped_count = 0
    for task in tasks:
        if task.get('completed', False):  # 完了タスクのみを処理
            task_id = task['gid']
            
            # 既に存在するタスクはスキップ
            if task_id in existing_task_ids:
                skipped_count += 1
                continue
                
            assignee = task.get('assignee')
            assignee_name = assignee.get('name') if assignee else ''
            
            # 日付データの処理
            completed_at = None
            if task.get('completed_at'):
                completed_at = task['completed_at'].replace('Z', '+00:00')
            
            created_at = task['created_at'].replace('Z', '+00:00')
            
            due_on = None
            if task.get('due_on'):
                due_on = task['due_on']
            
            modified_at = task['modified_at'].replace('Z', '+00:00')
            
            row = {
                'task_id': task_id,
                'task_name': task['name'],
                'project_id': task['project']['gid'],
                'project_name': task['project']['name'],
                'assignee_name': assignee_name,
                'completed_at': completed_at,
                'created_at': created_at,
                'due_on': due_on,
                'modified_at': modified_at,
                'inserted_at': datetime.utcnow().isoformat(),
                'estimated_time': task.get('estimated_time'),
                'actual_time': task.get('actual_time'),
                'actual_time_raw': task.get('actual_time_raw')  # 分単位の実績時間
            }
            rows_to_insert.append(row)
    
    if rows_to_insert:
        table_ref = client.dataset(dataset_id).table(table_id)
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            print(f"エラーが発生しました: {errors}")
        else:
            print(f"{len(rows_to_insert)}件のタスクデータを保存しました。{skipped_count}件の重複タスクをスキップしました。")
    else:
        print(f"新しいタスクはありません。{skipped_count}件の重複タスクをスキップしました。")

def main():
    """メイン処理"""
    # BigQueryテーブルの作成
    create_bigquery_table()
    
    # プロジェクト一覧の取得
    workspace_id = os.getenv('ASANA_WORKSPACE_ID')
    headers = {
        'Authorization': f'Bearer {os.getenv("ASANA_ACCESS_TOKEN")}'
    }
    
    url = f'https://app.asana.com/api/1.0/workspaces/{workspace_id}/projects'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        projects = response.json()['data']
        for project in projects:
            print(f"\nプロジェクト '{project['name']}' のタスクを取得中...")
            tasks = get_completed_tasks(project['gid'], project['name'])
            if tasks:
                insert_tasks_to_bigquery(tasks)
            else:
                print("完了タスクは見つかりませんでした。")
    else:
        print(f"プロジェクト一覧の取得に失敗しました: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    main() 