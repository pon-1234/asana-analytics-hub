import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from asana_reporter import config

def get_client() -> bigquery.Client:
    if config.GCP_CREDENTIALS_PATH:
        creds = service_account.Credentials.from_service_account_file(config.GCP_CREDENTIALS_PATH)
        return bigquery.Client(credentials=creds, project=config.GCP_PROJECT_ID)
    return bigquery.Client(project=config.GCP_PROJECT_ID)


def upsert_dim_projects(bq: bigquery.Client) -> int:
    sql = f"""
    MERGE `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.dim_projects` T
    USING (
      SELECT project_gid, ANY_VALUE(TRIM(project_name)) AS project_name FROM (
        SELECT project_gid, project_name FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
        WHERE project_gid IS NOT NULL AND project_gid != ''
      )
      GROUP BY project_gid
    ) S
    ON T.project_gid = S.project_gid
    WHEN MATCHED THEN UPDATE SET
      project_name = S.project_name,
      active = TRUE,
      updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (project_gid, project_name, active, created_at, updated_at)
      VALUES (S.project_gid, S.project_name, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    job = bq.query(sql)
    job.result()
    return job.num_dml_affected_rows or 0


def upsert_dim_users(bq: bigquery.Client) -> int:
    sql = f"""
    MERGE `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.dim_users` T
    USING (
      SELECT assignee_gid, ANY_VALUE(assignee_name) AS assignee_name FROM (
        SELECT assignee_gid, assignee_name FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
        WHERE assignee_gid IS NOT NULL AND assignee_gid != ''
      )
      GROUP BY assignee_gid
    ) S
    ON T.assignee_gid = S.assignee_gid
    WHEN MATCHED THEN UPDATE SET
      assignee_name = S.assignee_name,
      active = TRUE,
      updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (assignee_gid, assignee_name, active, created_at, updated_at)
      VALUES (S.assignee_gid, S.assignee_name, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    job = bq.query(sql)
    job.result()
    return job.num_dml_affected_rows or 0


def main():
    bq = get_client()
    proj = upsert_dim_projects(bq)
    users = upsert_dim_users(bq)
    print(f"dim_projects upserted: {proj}, dim_users upserted: {users}")

if __name__ == "__main__":
    sys.exit(main())
