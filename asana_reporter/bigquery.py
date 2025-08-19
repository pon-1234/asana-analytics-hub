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

def ensure_views(client: bigquery.Client):
    """共通ビュー(v_unique_tasks)を作成・更新する"""
    view_sql = f"""
    CREATE OR REPLACE VIEW `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks` AS
    WITH ranked AS (
      SELECT
        task_id,
        task_name,
        parent_task_id,
        TRIM(project_name) AS project_name,
        assignee_name,
        completed_at,
        modified_at,
        inserted_at,
        estimated_time,
        actual_time,
        actual_time_raw,
        is_subtask,
        ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY modified_at DESC, inserted_at DESC) AS rn
      FROM `{config.BQ_TABLE_FQN}`
      WHERE completed_at IS NOT NULL
    )
    SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
    """
    client.query(view_sql).result()
    print("View v_unique_tasks ensured.")

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
        # 新規列（後方互換で追加）
        for col, typ in (
            ("project_gid", "STRING"),
            ("assignee_gid", "STRING"),
            ("estimated_minutes", "FLOAT"),
            ("actual_minutes", "FLOAT"),
        ):
            if col not in existing_columns:
                client.query(f"ALTER TABLE `{config.BQ_TABLE_FQN}` ADD COLUMN IF NOT EXISTS {col} {typ}").result()
                print(f"Added column: {col}")
            
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

def upsert_tasks_via_merge(client: bigquery.Client, tasks: List[Dict[str, Any]]):
    """STAGING→MERGEで原子的にUPSERTする。"""
    if not tasks:
        print("No new tasks to upsert.")
        return

    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    target_ref = dataset_ref.table(config.BQ_TABLE_ID)
    staging_table_id = "completed_tasks_staging"
    staging_ref = dataset_ref.table(staging_table_id)

    # 1) 一時テーブル作成（ターゲットのスキーマを流用）
    client.delete_table(staging_ref, not_found_ok=True)
    target_table = client.get_table(target_ref)
    staging_table = bigquery.Table(staging_ref, schema=target_table.schema)
    client.create_table(staging_table)

    # 2) inserted_at を付与して一時テーブルへロード
    now_ts = datetime.now(timezone.utc).isoformat()
    rows_to_insert: List[Dict[str, Any]] = []
    for t in tasks:
        row = dict(t)
        row["inserted_at"] = now_ts
        rows_to_insert.append(row)
    errors = client.insert_rows_json(table=staging_ref, json_rows=rows_to_insert, ignore_unknown_values=True)
    if errors:
        print(f"Errors during staging insert_rows_json: {errors}")
        raise RuntimeError(str(errors))

    # 3) MERGE 実行
    merge_sql = f"""
    MERGE `{config.BQ_TABLE_FQN}` T
    USING `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.{staging_table_id}` S
    ON T.task_id = S.task_id
    WHEN MATCHED THEN UPDATE SET
      task_name       = S.task_name,
      project_id      = S.project_id,
      project_name    = S.project_name,
      assignee_name   = S.assignee_name,
      project_gid     = COALESCE(S.project_gid, T.project_gid),
      assignee_gid    = COALESCE(S.assignee_gid, T.assignee_gid),
      completed_at    = S.completed_at,
      created_at      = S.created_at,
      due_on          = S.due_on,
      modified_at     = S.modified_at,
      estimated_time  = S.estimated_time,
      actual_time     = S.actual_time,
      actual_time_raw = S.actual_time_raw,
      estimated_minutes = COALESCE(S.estimated_minutes, T.estimated_minutes),
      actual_minutes    = COALESCE(S.actual_minutes, T.actual_minutes),
      is_subtask      = S.is_subtask,
      parent_task_id  = S.parent_task_id,
      inserted_at     = S.inserted_at
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    client.query(merge_sql).result()
    client.delete_table(staging_ref, not_found_ok=True)
    print(f"Upserted {len(rows_to_insert)} rows into {config.BQ_TABLE_FQN} via MERGE.")

def get_report_data(client: bigquery.Client) -> Dict[str, Iterator[Dict[str, Any]]]:
    """BigQueryからレポート用の集計データを取得する"""
    print("Querying BigQuery for report data...")

    # v_unique_tasks を参照
    base_query = f"""
    WITH unique_tasks AS (
      SELECT task_id, project_name, assignee_name, completed_at, actual_minutes, estimated_minutes, modified_at, inserted_at
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
    )
    """

    # プロジェクト別集計
    project_query = base_query + """
    SELECT
        FORMAT_TIMESTAMP("%Y-%m", completed_at, "Asia/Tokyo") as month,
        project_name,
        COUNT(task_id) as tasks_count,
        SUM(IFNULL(actual_minutes, 0) / 60.0) as total_actual_hours,
        SUM(IFNULL(estimated_minutes, 0) / 60.0) as total_estimated_hours
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
        SUM(IFNULL(actual_minutes, 0) / 60.0) as total_actual_hours,
        SUM(IFNULL(estimated_minutes, 0) / 60.0) as total_estimated_hours
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
        SUM(IFNULL(actual_minutes, 0) / 60.0) as total_actual_hours,
        SUM(IFNULL(estimated_minutes, 0) / 60.0) as total_estimated_hours
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