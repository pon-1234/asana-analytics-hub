from google.cloud import bigquery
import os
import argparse


def hm_from_hours(hours_float: float) -> str:
    if hours_float is None:
        return "0時間0分"
    total_minutes = int(round(hours_float * 60))
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{h}時間{m}分"


def hm_from_minutes(minutes_float: float) -> str:
    if minutes_float is None:
        return "0時間0分"
    total_minutes = int(round(minutes_float))
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{h}時間{m}分"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--assignee_name", required=True)
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument(
        "--basis",
        choices=["completed", "modified"],
        default="completed",
        help="当月の判定基準: completed(完了日) or modified(更新日)",
    )
    parser.add_argument(
        "--show_details",
        action="store_true",
        help="該当レコードのタスク別詳細（予定/実績/内月判定）を出力する",
    )
    parser.add_argument("--gcp_project_id", default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument(
        "--task_ids",
        help="カンマ区切りの task_id リスト（親/サブタスクをこのID群に限定する）。指定時は project/assignee 条件にANDで追加",
    )
    args = parser.parse_args()

    if not args.gcp_project_id:
        raise SystemExit("GCP_PROJECT_ID が環境変数か --gcp_project_id で必要です")

    client = bigquery.Client(project=args.gcp_project_id)
    table_fqn = f"{args.gcp_project_id}.asana_analytics.completed_tasks"

    # 最新レコードのみ採用（task_id単位で重複排除）
    base_cte = f"""
    WITH unique_tasks AS (
      SELECT * EXCEPT(row_num) FROM (
        SELECT
          task_id,
          task_name,
          parent_task_id,
          TRIM(project_name) AS project_name,
          assignee_name,
          completed_at,
          estimated_time,   -- minutes
          actual_time,      -- hours
          actual_time_raw,  -- minutes
          modified_at,
          inserted_at,
          ROW_NUMBER() OVER(
            PARTITION BY task_id
            ORDER BY modified_at DESC, inserted_at DESC
          ) AS row_num
        FROM `{table_fqn}`
        WHERE completed_at IS NOT NULL
      )
      WHERE row_num = 1
    )
    """

    # 追加フィルタ（task_ids）
    extra_filter = ""
    ids_param = None
    if args.task_ids:
        ids = [x.strip() for x in args.task_ids.split(",") if x.strip()]
        if ids:
            extra_filter = " AND (task_id IN UNNEST(@ids) OR parent_task_id IN UNNEST(@ids))"
            ids_param = bigquery.ArrayQueryParameter("ids", "STRING", ids)

    # プロジェクト/担当者で全期間合計（実績=時間, 予定=分→時間換算も併記）
    overall_sql = base_cte + """
    SELECT
      COUNT(*) AS tasks_count,
      SUM(IFNULL(actual_time, 0.0)) AS total_actual_hours,
      SUM(IFNULL(estimated_time, 0.0)) AS total_estimated_minutes
    FROM unique_tasks
    WHERE TRIM(project_name) = @project_name AND assignee_name = @assignee_name""" + extra_filter
    

    # 対象月(YYYY-MM)に完了した分のみ（=「内 月」相当）
    # basisに応じて当月の定義を切替
    if args.basis == "completed":
        month_filter = "FORMAT_TIMESTAMP('%Y-%m', completed_at, 'Asia/Tokyo') = @month"
    else:
        month_filter = "FORMAT_TIMESTAMP('%Y-%m', modified_at, 'Asia/Tokyo') = @month"

    monthly_sql = base_cte + f"""
    SELECT
      COUNT(*) AS tasks_count,
      SUM(IFNULL(actual_time, 0.0)) AS total_actual_hours,
      SUM(IFNULL(estimated_time, 0.0)) AS total_estimated_minutes
    FROM unique_tasks
    WHERE TRIM(project_name) = @project_name
      AND assignee_name = @assignee_name
      AND {month_filter}""" + extra_filter
    

    params = [
        bigquery.ScalarQueryParameter("project_name", "STRING", args.project_name),
        bigquery.ScalarQueryParameter("assignee_name", "STRING", args.assignee_name),
        bigquery.ScalarQueryParameter("month", "STRING", args.month),
    ]
    if ids_param is not None:
        params.append(ids_param)
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    overall = list(client.query(overall_sql, job_config=job_config).result())
    monthly = list(client.query(monthly_sql, job_config=job_config).result())

    o = overall[0]
    m = monthly[0]

    print("==== 検証条件 ====")
    print(f"GCP Project: {args.gcp_project_id}")
    print(f"Table: {table_fqn}")
    print(f"Project Name: {args.project_name}")
    print(f"Assignee Name: {args.assignee_name}")
    print(f"Target Month: {args.month} (basis={args.basis}, tz=Asia/Tokyo)")
    if args.task_ids:
        print(f"Task IDs filter: {args.task_ids}")

    print("\n==== 全期間（プロジェクト×担当者）====")
    print(f"タスク数: {o.tasks_count}")
    print(f"実績時間(合計): {hm_from_hours(o.total_actual_hours)}")
    print(f"予定時間(合計): {hm_from_minutes(o.total_estimated_minutes)}")

    print("\n==== 当月のみ（completed_at の月 = 対象月）====")
    print(f"タスク数: {m.tasks_count}")
    print(f"実績時間(内{args.month}): {hm_from_hours(m.total_actual_hours)}")
    print(f"予定時間(内{args.month}): {hm_from_minutes(m.total_estimated_minutes)}")

    if args.show_details:
        detail_sql = base_cte + f"""
        SELECT
          task_id,
          task_name,
          estimated_time AS estimated_minutes,
          actual_time_raw AS actual_minutes,
          completed_at,
          FORMAT_TIMESTAMP('%Y-%m', completed_at, 'Asia/Tokyo') AS completed_month,
          modified_at,
          FORMAT_TIMESTAMP('%Y-%m', modified_at, 'Asia/Tokyo') AS modified_month
        FROM unique_tasks
        WHERE TRIM(project_name) = @project_name
          AND assignee_name = @assignee_name
          {" AND (task_id IN UNNEST(@ids) OR parent_task_id IN UNNEST(@ids))" if ids_param else ""}
        ORDER BY completed_at NULLS LAST, modified_at NULLS LAST
        """
        detail_rows = list(client.query(detail_sql, job_config=job_config).result())
        print("\n==== タスク別詳細 ====")
        for r in detail_rows:
            within = (r.completed_month == args.month) if args.basis == "completed" else (r.modified_month == args.month)
            est = hm_from_minutes(r.estimated_minutes)
            act = hm_from_minutes(r.actual_minutes)
            print(f"- {r.task_name} (task_id={r.task_id})")
            print(f"  予定: {est} / 実績: {act} / 内{args.month}: {'Yes' if within else 'No'}")


if __name__ == "__main__":
    main()


