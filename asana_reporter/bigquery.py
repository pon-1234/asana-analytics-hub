from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any, Iterator

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
    """completed_tasksテーブルが存在しない場合に作成する、または既存テーブルにカラムを追加する"""
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{config.BQ_DATASET_ID}' already exists.")
    except NotFound:
        print(f"Dataset '{config.BQ_DATASET_ID}' not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-northeast1"
        client.create_dataset(dataset)
        print(f"Dataset '{config.BQ_DATASET_ID}' created.")

    table_ref = dataset_ref.table(config.BQ_TABLE_ID)
    table_exists = False
    try:
        existing_table = client.get_table(table_ref)
        print(f"Table '{config.BQ_TABLE_FQN}' already exists.")
        table_exists = True
        
        # 既存のカラムを確認
        existing_columns = {field.name for field in existing_table.schema}
        
        # 新しいカラムが存在しない場合は追加
        if 'is_subtask' not in existing_columns or 'parent_task_id' not in existing_columns:
            print("Adding missing columns to existing table...")
            
            # ALTER TABLE文でカラムを追加
            if 'is_subtask' not in existing_columns:
                alter_query = f"""
                ALTER TABLE `{config.BQ_TABLE_FQN}`
                ADD COLUMN IF NOT EXISTS is_subtask BOOLEAN
                """
                client.query(alter_query).result()
                print("Added column: is_subtask")
            
            if 'parent_task_id' not in existing_columns:
                alter_query = f"""
                ALTER TABLE `{config.BQ_TABLE_FQN}`
                ADD COLUMN IF NOT EXISTS parent_task_id STRING
                """
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
        client.create_table(table)
        print(f"Table '{config.BQ_TABLE_FQN}' created.")

def insert_tasks(client: bigquery.Client, tasks: List[Dict[str, Any]]):
    """タスクデータをBigQueryに挿入する。重複はtask_idに基づいてマージ（更新）する。"""
    if not tasks:
        print("No new tasks to insert.")
        return

    # MERGE文を使い、存在すればUPDATE、存在しなければINSERTする
    # これにより、重複を避けつつ、タスク情報が更新された場合に対応できる
    query = f"""
    MERGE `{config.BQ_TABLE_FQN}` AS target
    USING (
        SELECT
            CAST(task_id AS STRING) as task_id,
            CAST(task_name AS STRING) as task_name,
            CAST(project_id AS STRING) as project_id,
            CAST(project_name AS STRING) as project_name,
            CAST(assignee_name AS STRING) as assignee_name,
            CAST(completed_at AS TIMESTAMP) as completed_at,
            CAST(created_at AS TIMESTAMP) as created_at,
            CAST(due_on AS DATE) as due_on,
            CAST(modified_at AS TIMESTAMP) as modified_at,
            CAST(estimated_time AS FLOAT64) as estimated_time,
            CAST(actual_time AS FLOAT64) as actual_time,
            CAST(actual_time_raw AS FLOAT64) as actual_time_raw,
            CAST(is_subtask AS BOOLEAN) as is_subtask,
            CAST(parent_task_id AS STRING) as parent_task_id
        FROM UNNEST(@json_records)
    ) AS source
    ON target.task_id = source.task_id
    WHEN MATCHED THEN
        UPDATE SET
            task_name = source.task_name,
            project_id = source.project_id,
            project_name = source.project_name,
            assignee_name = source.assignee_name,
            completed_at = source.completed_at,
            due_on = source.due_on,
            modified_at = source.modified_at,
            estimated_time = source.estimated_time,
            actual_time = source.actual_time,
            actual_time_raw = source.actual_time_raw,
            is_subtask = source.is_subtask,
            parent_task_id = source.parent_task_id,
            inserted_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            task_id, task_name, project_id, project_name, assignee_name,
            completed_at, created_at, due_on, modified_at, inserted_at,
            estimated_time, actual_time, actual_time_raw,
            is_subtask, parent_task_id
        )
        VALUES (
            source.task_id, source.task_name, source.project_id, source.project_name, source.assignee_name,
            source.completed_at, source.created_at, source.due_on, source.modified_at, CURRENT_TIMESTAMP(),
            source.estimated_time, source.actual_time, source.actual_time_raw,
            source.is_subtask, source.parent_task_id
        )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("json_records", "JSON", tasks)
        ]
    )

    print(f"Merging {len(tasks)} tasks into BigQuery...")
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        print(f"Successfully merged {query_job.num_dml_affected_rows} rows.")
    except Exception as e:
        print(f"An error occurred during BigQuery merge: {e}")
        raise

def get_report_data(client: bigquery.Client) -> Dict[str, Iterator[Dict[str, Any]]]:
    """BigQueryからレポート用の集計データを取得する"""
    print("Querying BigQuery for report data...")

    # CTEを一度だけ定義し、3つの集計クエリで再利用する
    base_query = f"""
    WITH unique_tasks AS (
      -- 各タスクについて、最も新しいレコードを1つだけ選ぶ
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT
          *,
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
        FORMAT_TIMESTAMP("%Y-%m", completed_at) as month,
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
        FORMAT_TIMESTAMP("%Y-%m", completed_at) as month,
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
        FORMAT_TIMESTAMP("%Y-%m", completed_at) as month,
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