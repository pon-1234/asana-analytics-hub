import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

def get_monthly_data(year, month):
    """指定された年月のデータを取得"""
    client = bigquery.Client()
    dataset_id = 'asana_data'
    
    # 月初と月末の日付を取得
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)
    
    # タスクと時間のデータを取得
    query = f"""
    WITH task_data AS (
        SELECT 
            t.gid as task_id,
            t.name as task_name,
            t.project.gid as project_id,
            p.name as project_name,
            t.assignee.gid as assignee_id,
            u.name as assignee_name,
            t.due_on,
            t.completed,
            t.completed_at,
            t.created_at,
            t.modified_at
        FROM `{os.getenv('GCP_PROJECT_ID')}.{dataset_id}.tasks` t
        LEFT JOIN `{os.getenv('GCP_PROJECT_ID')}.{dataset_id}.projects` p
        ON t.project.gid = p.gid
        LEFT JOIN `{os.getenv('GCP_PROJECT_ID')}.{dataset_id}.users` u
        ON t.assignee.gid = u.gid
        WHERE t.created_at <= TIMESTAMP('{end_date}')
        AND (t.completed_at IS NULL OR t.completed_at >= TIMESTAMP('{start_date}'))
    )
    SELECT 
        task_data.*,
        COALESCE(planned_hours, 0) as planned_hours,
        COALESCE(actual_hours, 0) as actual_hours
    FROM task_data
    LEFT JOIN (
        SELECT 
            task_id,
            SUM(CASE WHEN type = 'planned' THEN hours ELSE 0 END) as planned_hours,
            SUM(CASE WHEN type = 'actual' THEN hours ELSE 0 END) as actual_hours
        FROM `{os.getenv('GCP_PROJECT_ID')}.{dataset_id}.task_hours`
        WHERE date >= DATE('{start_date}')
        AND date < DATE('{end_date}')
        GROUP BY task_id
    ) hours_data
    ON task_data.task_id = hours_data.task_id
    """
    
    return client.query(query).to_dataframe()

def calculate_monthly_metrics(df):
    """月次のメトリクスを計算"""
    metrics = []
    
    # プロジェクトとユーザーごとに集計
    for (project_id, project_name, assignee_id, assignee_name), group in df.groupby(
        ['project_id', 'project_name', 'assignee_id', 'assignee_name']
    ):
        total_tasks = len(group)
        completed_tasks = group['completed'].sum()
        planned_hours = group['planned_hours'].sum()
        actual_hours = group['actual_hours'].sum()
        
        # 時間消化率の計算
        completion_rate = (actual_hours / planned_hours * 100) if planned_hours > 0 else 0
        
        # 実績時間記入率の計算
        actual_hours_rate = (len(group[group['actual_hours'] > 0]) / total_tasks * 100) if total_tasks > 0 else 0
        
        # 予定時間記入率の計算
        planned_hours_rate = (len(group[group['planned_hours'] > 0]) / total_tasks * 100) if total_tasks > 0 else 0
        
        metrics.append({
            'project_name': project_name,
            'assignee_name': assignee_name,
            'planned_hours': planned_hours,
            'actual_hours': actual_hours,
            'completion_rate': completion_rate,
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'planned_tasks': len(group[group['planned_hours'] > 0]),
            'actual_hours_rate': actual_hours_rate,
            'planned_hours_rate': planned_hours_rate
        })
    
    return pd.DataFrame(metrics)

def save_to_bigquery(df, year, month):
    """集計結果をBigQueryに保存"""
    client = bigquery.Client()
    dataset_id = 'asana_data'
    table_id = f'{os.getenv("GCP_PROJECT_ID")}.{dataset_id}.monthly_metrics'
    
    # 年月の列を追加
    df['year'] = year
    df['month'] = month
    
    # テーブルが存在しない場合は作成
    schema = [
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("assignee_name", "STRING"),
        bigquery.SchemaField("planned_hours", "FLOAT"),
        bigquery.SchemaField("actual_hours", "FLOAT"),
        bigquery.SchemaField("completion_rate", "FLOAT"),
        bigquery.SchemaField("total_tasks", "INTEGER"),
        bigquery.SchemaField("completed_tasks", "INTEGER"),
        bigquery.SchemaField("planned_tasks", "INTEGER"),
        bigquery.SchemaField("actual_hours_rate", "FLOAT"),
        bigquery.SchemaField("planned_hours_rate", "FLOAT")
    ]
    
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
    
    # データを保存
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def main():
    # 現在の年月を取得
    now = datetime.now()
    year = now.year
    month = now.month
    
    # データを取得して集計
    df = get_monthly_data(year, month)
    metrics_df = calculate_monthly_metrics(df)
    
    # 結果をBigQueryに保存
    save_to_bigquery(metrics_df, year, month)
    
    # 結果をCSVファイルとして保存
    output_file = f'data/monthly_report_{year}_{month:02d}.csv'
    metrics_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"レポートが生成されました: {output_file}")

if __name__ == "__main__":
    main() 