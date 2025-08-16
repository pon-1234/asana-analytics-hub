from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any, Iterator
from datetime import datetime, timezone

from . import config

def get_bigquery_client() -> bigquery.Client:
    """BigQueryクライアントを初期化して返す"""
    if config.GCP_CREDENTIALS_PATH:
        # ローカル実行時: サービスアカウントキーを使用
        credentials = service_account.Credentials.from_service_account_file(config.GCP_CREDENTIALS_PATH)
        return bigquery.Client(credentials=credentials, project=config.GCP_PROJECT_ID)
    else:
        # GCP環境: ADCを使用
        return bigquery.Client(project=config.GCP_PROJECT_ID)

def ensure_table_exists(client: bigquery.Client):
    """completed_tasksテーブルが存在しない場合に作成、または既存テーブルにカラムを追加"""
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Dataset '{config.BQ_DATASET_ID}' not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast1"
        client.create_dataset(dataset)
        print(f"Dataset '{config.BQ_DATASET_ID}' created.")

    table_ref = dataset_ref.table(config.BQ_TABLE_ID)
    try:
        existing_table = client.get_table(table_ref)
        print(f"Table '{config.BQ_TABLE_FQN}' already exists.")
        
        existing_columns = {field.name for field in existing_table.schema}
        
        # is_subtask と parent_task_id カラムがなければ追加 (既存のロジック)
        if 'is_subtask' not in existing_columns:
            alter_query = f"ALTER TABLE `{config.BQ_TABLE_FQN}` ADD COLUMN IF NOT EXISTS is_subtask BOOLEAN"
            client.query(alter_query).result()
            print("Added column: is_subtask")
        
        if 'parent_task_id' not in existing_columns:
            alter_query = f"ALTER TABLE `{config.BQ_TABLE_FQN}` ADD COLUMN IF NOT EXISTS parent_task_id STRING"
            client.query(alter_query).result()
            print("Added column: parent_task_id")
            
    except NotFound:
        print(f"Table '{config.BQ_TABLE_FQN}' not found. Creating...")
        schema = [
            bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("task_name", "STRING"),
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("project_name", "STRING"),
            bigquery.SchemaField("assignee_name", "STRING"),
            bigquery.SchemaField("completed_at", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("due_on", "DATE"),
            bigquery.SchemaField("modified_at", "TIMESTAMP"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("estimated_time", "FLOAT"),
            bigquery.SchemaField("actual_time", "FLOAT"),
            bigquery.SchemaField("actual_time_raw", "FLOAT"),
            bigquery.SchemaField("is_subtask", "BOOLEAN"),
            bigquery.SchemaField("parent_task_id", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        
        # ★改善点: パフォーマンス向上のためパーティショニングとクラスタリングを設定
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="completed_at",
        )
        table.clustering_fields = ["project_name", "assignee_name"]
        
        client.create_table(table)
        print(f"Table '{config.BQ_TABLE_FQN}' created with partitioning and clustering.")

def insert_tasks(client: bigquery.Client, tasks: List[Dict[str, Any]]):
    """タスクデータをBigQueryに挿入する。重複はtask_idでDELETE後にバルクINSERTする。"""
    if not tasks:
        print("No new tasks to insert.")
        return

    # 1) 事前に既存の同一 task_id を削除（UPSERT代替）
    task_ids = [str(t.get("task_id")) for t in tasks if t.get("task_id")]
    if task_ids:
        delete_query = f"""
        DELETE FROM `{config.BQ_TABLE_FQN}`
        WHERE task_id IN UNNEST(@ids)
        """
        try:
            delete_job = client.query(
                delete_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", task_ids)]
                ),
            )
            delete_job.result()
            print(f"Deleted existing rows for {len(task_ids)} task_ids.")
        except Exception as e:
            # Streaming buffer での削除禁止などはスキップして append-only で継続
            print(f"Skip delete due to error: {e}. Proceeding with insert-only (queries dedupe by task_id).")

    # 2) 行に inserted_at を付与し、JSONストリーミング挿入
    now_ts = datetime.now(timezone.utc).isoformat()
    rows_to_insert: List[Dict[str, Any]] = []
    for t in tasks:
        row = dict(t)
        row["inserted_at"] = now_ts
        rows_to_insert.append(row)

    table_ref = client.dataset(config.BQ_DATASET_ID).table(config.BQ_TABLE_ID)
    errors = client.insert_rows_json(table=table_ref, json_rows=rows_to_insert, ignore_unknown_values=True)
    if errors:
        print(f"Errors during insert_rows_json: {errors}")
        # 代表的なエラーだけ例外化
        raise RuntimeError(str(errors))
    print(f"Inserted {len(rows_to_insert)} rows into {config.BQ_TABLE_FQN}.")

def get_report_data(client: bigquery.Client) -> Dict[str, Iterator[Dict[str, Any]]]:
    """BigQueryからレポート用の集計データを取得する"""
    print("Querying BigQuery for report data...")

    # ★修正点: `SELECT *` をやめ、必要なカラムを明示的に指定してエラーを回避
    base_query = f"""
    WITH unique_tasks AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT
          task_id,
          TRIM(project_name) AS project_name,
          assignee_name,
          completed_at,
          actual_time,
          estimated_time,
          modified_at,
          inserted_at,
          ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY modified_at DESC, inserted_at DESC) as row_num
        FROM `{config.BQ_TABLE_FQN}`
        WHERE completed_at IS NOT NULL
      )
      WHERE row_num = 1
    )
    """

    # プロジェクト別集計
    project_query = base_query + """
    SELECT
        FORMAT_TIMESTAMP("%Y-%m", completed_at, "Asia/Tokyo") as month,
        project_name,
        COUNT(task_id) as tasks_count,
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        SUM(IFNULL(estimated_time, 0) / 60) as total_estimated_hours
    FROM unique_tasks
    GROUP BY month, project_name
    ORDER BY month DESC, total_actual_hours DESC
    """

    # 担当者別集計
    assignee_query = base_query + """
    SELECT
        FORMAT_TIMESTAMP("%Y-%m", completed_at, "Asia/Tokyo") as month,
        assignee_name,
        COUNT(task_id) as tasks_count,
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        SUM(IFNULL(estimated_time, 0) / 60) as total_estimated_hours
    FROM unique_tasks
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
    GROUP BY month, assignee_name
    ORDER BY month DESC, total_actual_hours DESC
    """
    
    # プロジェクト・担当者別集計
    project_assignee_query = base_query + """
    SELECT
        FORMAT_TIMESTAMP("%Y-%m", completed_at, "Asia/Tokyo") as month,
        project_name,
        assignee_name,
        COUNT(task_id) as tasks_count,
        SUM(IFNULL(actual_time, 0)) as total_actual_hours,
        SUM(IFNULL(estimated_time, 0) / 60) as total_estimated_hours
    FROM unique_tasks
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
    GROUP BY month, project_name, assignee_name
    ORDER BY month DESC, project_name, total_actual_hours DESC
    """
    
    return {
        'project': client.query(project_query).result(),
        'assignee': client.query(assignee_query).result(),
        'project_assignee': client.query(project_assignee_query).result()
    }