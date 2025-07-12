from google.cloud import bigquery
import os
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# BigQueryクライアントの初期化
project_id = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project=project_id)

table_id = "asana-analytics-hub.asana_analytics.completed_tasks"

# assignee_nameカラムの状態を確認するクエリ
check_query = f"""
SELECT
    COUNT(*) AS total_rows,
    COUNTIF(assignee_name IS NULL) AS null_assignees,
    COUNTIF(assignee_name = '') AS empty_string_assignees,
    COUNTIF(assignee_name IS NOT NULL AND assignee_name != '') AS valid_assignees
FROM `{table_id}`
"""

print(f"テーブル `{table_id}` の `assignee_name` カラムの状態を確認します...")
query_job = client.query(check_query)
results = query_job.result()

for row in results:
    print(f"  - 総行数: {row.total_rows}")
    print(f"  - assignee_nameがNULLの行数: {row.null_assignees}")
    print(f"  - assignee_nameが空文字列('')の行数: {row.empty_string_assignees}")
    print(f"  - assignee_nameに有効な名前が入っている行数: {row.valid_assignees}")

# 有効な担当者名のサンプルを取得
sample_query = f"""
SELECT DISTINCT assignee_name
FROM `{table_id}`
WHERE assignee_name IS NOT NULL AND assignee_name != ''
LIMIT 10
"""
print("\n有効な担当者名のサンプル:")
query_job = client.query(sample_query)
results = query_job.result()
sample_count = 0
for row in results:
    print(f"  - {row.assignee_name}")
    sample_count += 1

if sample_count == 0:
    print("  (有効な担当者名が見つかりませんでした)") 