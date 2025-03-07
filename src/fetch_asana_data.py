import os
import json
from datetime import datetime
import asana
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# Asanaクライアントの初期化
client = asana.Client.access_token(os.getenv('ASANA_ACCESS_TOKEN'))

def get_all_projects(workspace_id):
    """ワークスペース内の全てのプロジェクトを取得"""
    projects = client.projects.get_projects({'workspace': workspace_id})
    return projects

def fetch_tasks(project_id):
    """プロジェクト内のタスクを取得"""
    tasks = client.tasks.get_tasks({'project': project_id})
    return tasks

def fetch_project_details(project_id):
    """プロジェクトの詳細情報を取得"""
    project = client.projects.get_project(project_id)
    return project

def fetch_users(workspace_id):
    """ワークスペースのユーザー情報を取得"""
    users = client.users.get_users({'workspace': workspace_id})
    return users

def fetch_tags(workspace_id):
    """ワークスペースのタグ情報を取得"""
    tags = client.tags.get_tags({'workspace': workspace_id})
    return tags

def save_to_bigquery(data, table_name):
    """データをBigQueryに保存"""
    client = bigquery.Client()
    dataset_id = 'asana_data'
    table_id = f'{os.getenv("GCP_PROJECT_ID")}.{dataset_id}.{table_name}'
    
    # データセットが存在しない場合は作成
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
    
    # データをDataFrameに変換
    df = pd.DataFrame(data)
    
    # BigQueryに保存
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def main():
    workspace_id = os.getenv('ASANA_WORKSPACE_ID')
    
    # 全てのプロジェクトを取得
    projects = get_all_projects(workspace_id)
    project_ids = [project['gid'] for project in projects]
    
    print(f"取得対象のプロジェクト数: {len(project_ids)}")
    
    # データ取得
    all_tasks = []
    for project_id in project_ids:
        print(f"プロジェクト {project_id} のタスクを取得中...")
        tasks = fetch_tasks(project_id)
        all_tasks.extend(tasks)
    
    print("プロジェクト情報を取得中...")
    projects_data = [fetch_project_details(pid) for pid in project_ids]
    
    print("ユーザー情報を取得中...")
    users = fetch_users(workspace_id)
    
    print("タグ情報を取得中...")
    tags = fetch_tags(workspace_id)
    
    # BigQueryに保存
    print("データをBigQueryに保存中...")
    save_to_bigquery(all_tasks, 'tasks')
    save_to_bigquery(projects_data, 'projects')
    save_to_bigquery(users, 'users')
    save_to_bigquery(tags, 'tags')
    
    print(f"データの取得と保存が完了しました: {datetime.now()}")

if __name__ == "__main__":
    main() 