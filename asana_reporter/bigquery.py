from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any, Iterator
from datetime import datetime, timezone
import time

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
        project_gid,
        assignee_name,
        assignee_gid,
        completed_at,
        modified_at,
        inserted_at,
        estimated_time,
        actual_time,
        actual_time_raw,
        estimated_minutes,
        actual_minutes,
        is_subtask,
        ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY modified_at DESC, inserted_at DESC) AS rn
      FROM `{config.BQ_TABLE_FQN}`
      WHERE completed_at IS NOT NULL
    )
    SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;
    """
    client.query(view_sql).result()
    print("View v_unique_tasks ensured.")

    # Optional: date-granularity helper view for daily aggregations
    daily_view_sql = f"""
    CREATE OR REPLACE VIEW `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks_daily` AS
    WITH base AS (
      SELECT
        task_id,
        project_name,
        project_gid,
        assignee_name,
        assignee_gid,
        DATE(completed_at, 'Asia/Tokyo') AS completed_date_jst,
        estimated_minutes,
        actual_minutes
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
    )
    SELECT * FROM base;
    """
    client.query(daily_view_sql).result()
    print("View v_unique_tasks_daily ensured.")

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
            ("estimated_minutes", "FLOAT64"),
            ("actual_minutes", "FLOAT64"),
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
        # Cluster by stable IDs for better pruning on joins/filters
        table.clustering_fields = ["project_gid", "assignee_gid"]
        
        client.create_table(table)
        print(f"Table '{config.BQ_TABLE_FQN}' created with partitioning and clustering.")

def backfill_minutes_columns(client: bigquery.Client):
    """Backfill estimated_minutes/actual_minutes from legacy columns.

    - estimated_minutes <- estimated_time (minutes)
    - actual_minutes <- actual_time_raw (minutes) else actual_time (hours) * 60
    """
    print("Backfilling minutes columns if needed...")
    backfill_sql = f"""
    -- estimated_minutes from estimated_time
    UPDATE `{config.BQ_TABLE_FQN}`
    SET estimated_minutes = estimated_time
    WHERE estimated_minutes IS NULL AND estimated_time IS NOT NULL;

    -- actual_minutes from actual_time_raw first
    UPDATE `{config.BQ_TABLE_FQN}`
    SET actual_minutes = actual_time_raw
    WHERE actual_minutes IS NULL AND actual_time_raw IS NOT NULL;

    -- otherwise from actual_time (hours)
    UPDATE `{config.BQ_TABLE_FQN}`
    SET actual_minutes = actual_time * 60.0
    WHERE actual_minutes IS NULL AND actual_time IS NOT NULL;
    """
    # BigQuery supports running multiple statements when enabled via job config; issue sequentially for reliability
    for stmt in [s.strip() for s in backfill_sql.split(";\n") if s.strip()]:
        client.query(stmt).result()
    print("Backfill completed.")

def update_completed_tasks_clustering_to_ids(client: bigquery.Client):
    """Switch clustering to project_gid/assignee_gid for the completed_tasks table."""
    print("Updating clustering fields to project_gid, assignee_gid (if supported)...")
    alter = f"""
    ALTER TABLE `{config.BQ_TABLE_FQN}`
    SET OPTIONS (
      clustering_fields = ['project_gid','assignee_gid']
    )
    """
    try:
        client.query(alter).result()
        print("Clustering updated.")
    except Exception as e:
        # Non-fatal in case of permissions or unsupported state
        print(f"Clustering update skipped or failed: {e}")

def ensure_open_tasks_snapshot_table(client: bigquery.Client):
    """Ensure the open_tasks_snapshot table exists for daily snapshots of incomplete tasks."""
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    table_ref = dataset_ref.table("open_tasks_snapshot")
    try:
        client.get_table(table_ref)
        print("Table open_tasks_snapshot already exists.")
        return
    except NotFound:
        pass

    schema = [
        bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("snapshot_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("task_name", "STRING"),
        bigquery.SchemaField("project_gid", "STRING"),
        bigquery.SchemaField("project_name", "STRING"),
        bigquery.SchemaField("assignee_gid", "STRING"),
        bigquery.SchemaField("assignee_name", "STRING"),
        bigquery.SchemaField("due_on", "DATE"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("modified_at", "TIMESTAMP"),
        bigquery.SchemaField("is_overdue", "BOOLEAN"),
        bigquery.SchemaField("has_time_fields", "BOOLEAN"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="snapshot_date",
    )
    table.clustering_fields = ["project_gid", "assignee_gid"]
    client.create_table(table)
    print("Table open_tasks_snapshot created.")

def insert_open_tasks_snapshot(client: bigquery.Client, rows: List[Dict[str, Any]]):
    """Append rows into open_tasks_snapshot."""
    if not rows:
        print("No open tasks to snapshot.")
        return
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    table_ref = dataset_ref.table("open_tasks_snapshot")
    errors = client.insert_rows_json(table=table_ref, json_rows=rows, ignore_unknown_values=True)
    if errors:
        print(f"Errors during open_tasks_snapshot insert: {errors}")
        raise RuntimeError(str(errors))
    print(f"Inserted {len(rows)} rows into open_tasks_snapshot.")

def ensure_time_entries_table(client: bigquery.Client):
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    table_ref = dataset_ref.table("time_entries")
    try:
        client.get_table(table_ref)
        print("Table time_entries already exists.")
        return
    except NotFound:
        schema = [
            bigquery.SchemaField("entry_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("task_id", "STRING"),
            bigquery.SchemaField("task_name", "STRING"),
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("project_name", "STRING"),
            bigquery.SchemaField("user_name", "STRING"),
            bigquery.SchemaField("entered_on", "DATE"),
            bigquery.SchemaField("duration_minutes", "FLOAT"),
            bigquery.SchemaField("duration_hours", "FLOAT"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("modified_at", "TIMESTAMP"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["entered_on", "project_name", "user_name"]
        client.create_table(table)
        print("Created table time_entries.")

def insert_time_entries(client: bigquery.Client, rows: List[Dict[str, Any]]):
    if not rows:
        print("No time entries to insert.")
        return
    # Upsert by deleting existing entry_ids, then insert
    ids = [r["entry_id"] for r in rows if r.get("entry_id")]
    if ids:
        delete_query = f"DELETE FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.time_entries` WHERE entry_id IN UNNEST(@ids)"
        try:
            client.query(
                delete_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
                ),
            ).result()
        except Exception as e:
            print(f"Skip delete time_entries: {e}")

    now_ts = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["inserted_at"] = now_ts
        try:
            r["duration_hours"] = (float(r.get("duration_minutes") or 0.0)) / 60.0
        except Exception:
            r["duration_hours"] = 0.0

    table_ref = client.dataset(config.BQ_DATASET_ID).table("time_entries")
    errors = client.insert_rows_json(table_ref, rows, ignore_unknown_values=True)
    if errors:
        raise RuntimeError(str(errors))
    print(f"Inserted {len(rows)} rows into time_entries.")

def upsert_tasks_via_merge(client: bigquery.Client, tasks: List[Dict[str, Any]]):
    """STAGING→MERGEで原子的にUPSERTする。"""
    if not tasks:
        print("No new tasks to upsert.")
        return

    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    target_ref = dataset_ref.table(config.BQ_TABLE_ID)
    staging_table_id = f"completed_tasks_staging_{int(time.time())}"
    staging_ref = dataset_ref.table(staging_table_id)

    # 1) 一時テーブル作成（ターゲットのスキーマを流用）: 固有名で作成し、前段の削除APIを回避
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
    USING (
      SELECT * FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.{staging_table_id}`
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY task_id
        ORDER BY COALESCE(modified_at, inserted_at) DESC, inserted_at DESC
      ) = 1
    ) S
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

def ensure_dim_tables(client: bigquery.Client):
    """Ensure dim_projects and dim_users tables exist for normalization and Slack DM.
    Minimal schema to start with.
    """
    dataset_ref = client.dataset(config.BQ_DATASET_ID)
    # dim_projects
    proj_ref = dataset_ref.table("dim_projects")
    try:
        client.get_table(proj_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("project_gid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("project_name", "STRING"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        client.create_table(bigquery.Table(proj_ref, schema=schema))
        print("Table dim_projects created.")
    # dim_users
    user_ref = dataset_ref.table("dim_users")
    try:
        client.get_table(user_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("assignee_gid", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("assignee_name", "STRING"),
            bigquery.SchemaField("slack_user_id", "STRING"),
            bigquery.SchemaField("work_hours_per_day", "FLOAT"),
            bigquery.SchemaField("active", "BOOL"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]
        client.create_table(bigquery.Table(user_ref, schema=schema))
        print("Table dim_users created.")