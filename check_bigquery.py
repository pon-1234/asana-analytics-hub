from google.cloud import bigquery
import os
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# BigQueryクライアントの初期化
project_id = os.getenv('GCP_PROJECT_ID')
client = bigquery.Client(project=project_id)

# サンプルデータを取得するクエリ
query = """
SELECT * 
FROM `asana-analytics-hub.asana_analytics.completed_tasks` 
LIMIT 5
"""

# クエリの実行
query_job = client.query(query)
results = query_job.result()

# カラム名の表示
print('カラム名:')
for field in results.schema:
    print(field.name)

# サンプルデータの表示
print('\nサンプルデータ:')
for row in results:
    print(row)

# actual_timeとestimated_timeのNULL値の数を確認
null_check_query = """
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(actual_time IS NULL) as null_actual_time,
    COUNTIF(estimated_time IS NULL) as null_estimated_time
FROM `asana-analytics-hub.asana_analytics.completed_tasks`
"""

null_check_job = client.query(null_check_query)
null_check_results = null_check_job.result()

print('\nNULL値の確認:')
for row in null_check_results:
    print(f"総行数: {row.total_rows}")
    print(f"actual_timeがNULLの行数: {row.null_actual_time} ({row.null_actual_time/row.total_rows*100:.2f}%)")
    print(f"estimated_timeがNULLの行数: {row.null_estimated_time} ({row.null_estimated_time/row.total_rows*100:.2f}%)")

# 0値の数を確認
zero_check_query = """
SELECT 
    COUNT(*) as total_rows,
    COUNTIF(actual_time = 0) as zero_actual_time,
    COUNTIF(estimated_time = 0) as zero_estimated_time
FROM `asana-analytics-hub.asana_analytics.completed_tasks`
WHERE actual_time IS NOT NULL AND estimated_time IS NOT NULL
"""

zero_check_job = client.query(zero_check_query)
zero_check_results = zero_check_job.result()

print('\n0値の確認:')
for row in zero_check_results:
    print(f"NULL以外の行数: {row.total_rows}")
    print(f"actual_timeが0の行数: {row.zero_actual_time} ({row.zero_actual_time/row.total_rows*100:.2f}%)")
    print(f"estimated_timeが0の行数: {row.zero_estimated_time} ({row.zero_estimated_time/row.total_rows*100:.2f}%)") 