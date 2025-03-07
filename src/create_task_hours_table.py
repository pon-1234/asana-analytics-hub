import os
from google.cloud import bigquery
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

def create_task_hours_table():
    """タスクの時間情報を保存するテーブルを作成"""
    client = bigquery.Client()
    dataset_id = 'asana_data'
    table_id = f'{os.getenv("GCP_PROJECT_ID")}.{dataset_id}.task_hours'
    
    # テーブルのスキーマを定義
    schema = [
        bigquery.SchemaField("task_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("type", "STRING"),  # 'planned' or 'actual'
        bigquery.SchemaField("hours", "FLOAT"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP")
    ]
    
    # テーブルが存在しない場合は作成
    try:
        client.get_table(table_id)
        print(f"テーブル {table_id} は既に存在します。")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"テーブル {table_id} を作成しました。")

if __name__ == "__main__":
    create_task_hours_table() 