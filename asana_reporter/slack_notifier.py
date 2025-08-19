import os
from typing import Optional, List, Dict, Any

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from google.cloud import bigquery

from . import config
from datetime import datetime, timezone

# Environment-driven configuration (optional; safe to skip when unset)
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
SLACK_ALERT_CHANNEL_ID = os.getenv("SLACK_ALERT_CHANNEL_ID")  # 下振れ強アラート用（任意）
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

_slack_client: Optional[WebClient] = WebClient(token=SLACK_BOT_TOKEN) if SLACK_BOT_TOKEN else None


def _post_message_to(channel_id: Optional[str], blocks: List[Dict[str, Any]], text_fallback: str, thread_ts: Optional[str] = None) -> Optional[str]:
    """Post a message to Slack if configured. Returns ts or None.

    This function is intentionally non-fatal: if Slack is not configured or the
    API call fails, it prints a message and returns None without raising.
    """
    if not _slack_client or not channel_id:
        print("Slack token/channel not configured. Skipping Slack post.")
        return None
    try:
        resp = _slack_client.chat_postMessage(
            channel=channel_id,
            text=text_fallback,
            blocks=blocks,
            thread_ts=thread_ts,
        )
        return resp.get("ts")
    except SlackApiError as e:
        try:
            err = e.response.get("error") if e.response else str(e)
        except Exception:
            err = str(e)
        print(f"Slack post failed: {err}")
        return None
    except Exception as e:
        print(f"Slack post failed: {e}")
        return None


def _post_message(blocks: List[Dict[str, Any]], text_fallback: str, thread_ts: Optional[str] = None) -> Optional[str]:
    return _post_message_to(SLACK_CHANNEL_ID, blocks, text_fallback, thread_ts)


def _as_mrkdwn_table(rows: List[Dict[str, Any]], cols: List[str], headers: List[str]) -> str:
    """Render a tiny monospace table using code block for alignment."""
    if not rows:
        return "_(データなし)_"
    widths = []
    for header, col in zip(headers, cols):
        cell_width = max(len(header), *(len(str(r.get(col, ""))) for r in rows))
        widths.append(cell_width)

    def fmt_row(row: Dict[str, Any]) -> str:
        return " | ".join(str(row.get(col, "")).ljust(width) for col, width in zip(cols, widths))

    header_line = " | ".join(header.ljust(width) for header, width in zip(headers, widths))
    sep_line = "-+-".join("-" * width for width in widths)
    body_lines = "\n".join(fmt_row(r) for r in rows)
    return f"```\n{header_line}\n{sep_line}\n{body_lines}\n```"


def _hm(hours: Optional[float]) -> str:
    return f"{round(float(hours or 0.0), 2)}h"


def _quick_links_elements() -> List[Dict[str, Any]]:
    """Return a single-element context with quick links to Sheets and BigQuery."""
    links: List[str] = []
    if config.SPREADSHEET_ID:
        sheets_url = f"https://docs.google.com/spreadsheets/d/{config.SPREADSHEET_ID}"
        links.append(f"<{sheets_url}|Sheets>")
    if config.GCP_PROJECT_ID:
        bq_url = f"https://console.cloud.google.com/bigquery?project={config.GCP_PROJECT_ID}"
        links.append(f"<{bq_url}|BigQuery>")
    if not links:
        return []
    return [{"type": "mrkdwn", "text": " | ".join(links)}]


def send_run_summary(tasks_processed: int, started_at_iso: str, finished_at_iso: str, errors: int = 0) -> None:
    """Send a concise health summary after fetch completes."""
    title = "*Asana → BigQuery 同期結果*"
    blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*処理タスク数:*\n{tasks_processed}"},
                {"type": "mrkdwn", "text": f"*エラー件数:*\n{errors}"},
                {"type": "mrkdwn", "text": f"*開始:*\n{started_at_iso}"},
                {"type": "mrkdwn", "text": f"*終了:*\n{finished_at_iso}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"`{config.BQ_TABLE_FQN}`"},
            ],
        },
    ]
    ql = _quick_links_elements()
    if ql:
        blocks.append({"type": "context", "elements": ql})
    _post_message(blocks, text_fallback="Asana→BQ 同期結果")


def send_monthly_digest(bq: bigquery.Client, month: Optional[str] = None, top_n: int = 5) -> None:
    """Post a monthly digest for a given YYYY-MM (default latest month in data)."""
    base = f"""
    WITH unique_tasks AS (
      SELECT task_id, TRIM(project_name) AS project_name, assignee_name,
             completed_at, actual_minutes, estimated_minutes, modified_at, inserted_at
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
    )
    """

    # Resolve month expression
    month_selector = "SELECT FORMAT_TIMESTAMP('%Y-%m', MAX(completed_at), 'Asia/Tokyo') AS m FROM unique_tasks"
    month_expr = f"'{month}'" if month else f"({month_selector})"

    kpi_sql = base + f"""
    SELECT
      {month_expr} AS month,
      COUNT(task_id) AS tasks_count,
      SUM(IFNULL(actual_time,0.0)) AS total_actual_hours,
      SUM(IFNULL(estimated_time,0.0))/60.0 AS total_estimated_hours
    FROM unique_tasks
    WHERE FORMAT_TIMESTAMP('%Y-%m', completed_at, 'Asia/Tokyo') = {month_expr}
    """

    top_project_sql = base + f"""
    SELECT project_name,
           COUNT(task_id) AS tasks,
           SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    WHERE FORMAT_TIMESTAMP('%Y-%m', completed_at, 'Asia/Tokyo') = {month_expr}
    GROUP BY project_name
    ORDER BY hours DESC
    LIMIT {top_n}
    """

    top_assignee_sql = base + f"""
    SELECT assignee_name,
           COUNT(task_id) AS tasks,
           SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
      AND FORMAT_TIMESTAMP('%Y-%m', completed_at, 'Asia/Tokyo') = {month_expr}
    GROUP BY assignee_name
    ORDER BY hours DESC
    LIMIT {top_n}
    """

    kpi_row = next(iter(bq.query(kpi_sql).result()), None)
    projects = list(bq.query(top_project_sql).result())
    assignees = list(bq.query(top_assignee_sql).result())

    # Fall back month string
    if kpi_row and getattr(kpi_row, "month", None):
        chosen_month = kpi_row.month
    else:
        chosen_month = next(iter(bq.query(base + month_selector).result()), None)
        chosen_month = getattr(chosen_month, "m", "(no data)")

    title = f"*📊 {chosen_month} 月次ダイジェスト*"
    projects_tbl = _as_mrkdwn_table(
        [{"project": r.project_name, "hours": round((r.hours or 0.0), 2), "tasks": r.tasks} for r in projects],
        ["project", "hours", "tasks"],
        ["プロジェクト", "実績h", "件数"],
    )
    assignees_tbl = _as_mrkdwn_table(
        [{"assignee": r.assignee_name, "hours": round((r.hours or 0.0), 2), "tasks": r.tasks} for r in assignees],
        ["assignee", "hours", "tasks"],
        ["担当者", "実績h", "件数"],
    )

    total_actual_hours = getattr(kpi_row, "total_actual_hours", 0.0)
    tasks_count = getattr(kpi_row, "tasks_count", 0)
    total_estimated_hours = getattr(kpi_row, "total_estimated_hours", 0.0)

    blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*合計実績時間:*\n{_hm(total_actual_hours)}"},
                {"type": "mrkdwn", "text": f"*完了タスク数:*\n{tasks_count}"},
                {"type": "mrkdwn", "text": f"*合計見積時間:*\n{_hm(total_estimated_hours)}"},
            ],
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Projects*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": projects_tbl}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Assignees*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": assignees_tbl}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"データソース: `{config.BQ_TABLE_FQN}` / TZ: Asia/Tokyo"}]},
    ]
    ql = _quick_links_elements()
    if ql:
        blocks.append({"type": "context", "elements": ql})
    _post_message(blocks, text_fallback=f"{chosen_month} 月次ダイジェスト")


def send_daily_digest(bq: bigquery.Client, target_date: Optional[str] = None, top_n: int = 5) -> None:
    """Post a daily digest for yesterday (JST) or specified YYYY-MM-DD."""
    tz = "Asia/Tokyo"
    y_expr = f"DATE '{target_date}'" if target_date else f"DATE_SUB(CURRENT_DATE('{tz}'), INTERVAL 1 DAY)"

    base = f"""
    WITH unique_tasks AS (
      SELECT task_id, TRIM(project_name) AS project_name, assignee_name,
             completed_at, actual_minutes, estimated_minutes, modified_at, inserted_at
      FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.v_unique_tasks`
    )
    """

    y_sql = base + f"""
    SELECT
      COUNT(task_id) AS tasks_count,
      SUM(IFNULL(actual_minutes,0.0))/60.0 AS total_hours,
      SUM(IFNULL(estimated_minutes,0.0))/60.0 AS estimated_hours
    FROM unique_tasks
    WHERE DATE(completed_at, '{tz}') = {y_expr}
    """

    # 直近7営業日の平均（週末除外）
    # Append CTEs to the existing WITH unique_tasks ... using comma, not a second WITH
    w_sql = base + f"""
    , daily AS (
      SELECT
        DATE(completed_at, '{tz}') AS d,
        COUNT(task_id) AS tasks,
        SUM(IFNULL(actual_time,0.0)) AS hours
      FROM unique_tasks
      WHERE DATE(completed_at, '{tz}') BETWEEN DATE_SUB({y_expr}, INTERVAL 30 DAY) AND DATE_SUB({y_expr}, INTERVAL 1 DAY)
      GROUP BY d
    ), business7 AS (
      SELECT d, tasks, hours
      FROM daily
      WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7)
      ORDER BY d DESC
      LIMIT 7
    )
    SELECT AVG(tasks) AS avg_tasks, AVG(hours) AS avg_hours FROM business7
    """

    top_projects_sql = base + f"""
    SELECT project_name, COUNT(task_id) AS tasks, SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    WHERE DATE(completed_at, '{tz}') = {y_expr}
    GROUP BY project_name
    ORDER BY hours DESC
    LIMIT {top_n}
    """

    top_assignees_sql = base + f"""
    SELECT assignee_name, COUNT(task_id) AS tasks, SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
      AND DATE(completed_at, '{tz}') = {y_expr}
    GROUP BY assignee_name
    ORDER BY hours DESC
    LIMIT {top_n}
    """

    mtd_sql = base + f"""
    SELECT
      SUM(IF(DATE(completed_at, '{tz}') BETWEEN DATE_TRUNC({y_expr}, MONTH) AND {y_expr}, IFNULL(actual_minutes,0.0), 0.0)) AS mtd_hours,
      COUNTIF(DATE(completed_at, '{tz}') BETWEEN DATE_TRUNC({y_expr}, MONTH) AND {y_expr}) AS mtd_tasks
    FROM unique_tasks
    """

    # 担当者別（昨日）
    y_assignee_sql = base + f"""
    SELECT assignee_name, COUNT(task_id) AS tasks, SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
      AND DATE(completed_at, '{tz}') = {y_expr}
    GROUP BY assignee_name
    """

    # 担当者別の営業日ベースライン（直近7営業日平均）
    hist_assignee_daily_sql = base + f"""
    SELECT assignee_name, d, COUNT(task_id) AS tasks, SUM(IFNULL(actual_time,0.0)) AS hours
    FROM unique_tasks
    CROSS JOIN UNNEST([DATE(completed_at, '{tz}')]) AS d
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
      AND DATE(completed_at, '{tz}') BETWEEN DATE_SUB({y_expr}, INTERVAL 30 DAY) AND DATE_SUB({y_expr}, INTERVAL 1 DAY)
    GROUP BY assignee_name, d
    HAVING EXTRACT(DAYOFWEEK FROM d) NOT IN (1,7)
    """

    y = next(iter(bq.query(y_sql).result()), None)
    w_iter = list(bq.query(w_sql).result())
    w = w_iter[0] if w_iter else None
    projects = list(bq.query(top_projects_sql).result())
    assignees = list(bq.query(top_assignees_sql).result())
    mtd = next(iter(bq.query(mtd_sql).result()), None)
    # 担当者別昨日/履歴
    y_by_assignee = list(bq.query(y_assignee_sql).result())
    hist_assignee_daily = list(bq.query(hist_assignee_daily_sql).result())

    def pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if b is None or b == 0:
            return None
        a_val = float(a or 0.0)
        b_val = float(b or 0.0)
        return round((a_val - b_val) / b_val * 100.0, 1)

    tasks_vs = pct(getattr(y, "tasks_count", 0), getattr(w, "avg_tasks", None) if w else None)
    hours_vs = pct(getattr(y, "total_hours", 0.0), getattr(w, "avg_hours", None) if w else None)
    warn = (hours_vs is not None and abs(hours_vs) >= 50) or (tasks_vs is not None and abs(tasks_vs) >= 50)

    projects_tbl = _as_mrkdwn_table(
        [{"project": r.project_name, "hours": round((r.hours or 0.0), 2), "tasks": r.tasks} for r in projects],
        ["project", "hours", "tasks"],
        ["プロジェクト", "実績h", "件数"],
    )
    assignees_tbl = _as_mrkdwn_table(
        [{"assignee": r.assignee_name, "hours": round((r.hours or 0.0), 2), "tasks": r.tasks} for r in assignees],
        ["assignee", "hours", "tasks"],
        ["担当者", "実績h", "件数"],
    )

    # Resolve the target date string for the title
    day_row = next(iter(bq.query(f"SELECT CAST({y_expr} AS STRING) AS d").result()), None)
    day_str = getattr(day_row, "d", "(date)")

    title = f"*🗓️ 日次ダイジェスト — {day_str}*" + ("  ⚠️" if warn else "")
    baseline = (
        f"（直近7日平均比:  件数 {tasks_vs:+.1f}% / 時間 {hours_vs:+.1f}%）"
        if (tasks_vs is not None and hours_vs is not None) else ""
    )

    total_hours = getattr(y, "total_hours", 0.0) if y else 0.0
    tasks_count = getattr(y, "tasks_count", 0) if y else 0
    estimated_hours = getattr(y, "estimated_hours", 0.0) if y else 0.0
    mtd_hours = getattr(mtd, "mtd_hours", 0.0) if mtd else 0.0
    mtd_tasks = getattr(mtd, "mtd_tasks", 0) if mtd else 0

    # None を安全に数値へ
    try:
        total_hours = float(total_hours or 0.0)
    except Exception:
        total_hours = 0.0
    try:
        estimated_hours = float(estimated_hours or 0.0)
    except Exception:
        estimated_hours = 0.0
    try:
        mtd_hours = float(mtd_hours or 0.0)
    except Exception:
        mtd_hours = 0.0
    try:
        tasks_count = int(tasks_count or 0)
    except Exception:
        tasks_count = 0
    try:
        mtd_tasks = int(mtd_tasks or 0)
    except Exception:
        mtd_tasks = 0

    ratio_str = (
        f"{round((total_hours / estimated_hours) * 100.0, 1)}%" if estimated_hours and estimated_hours > 0 else "0%"
    )

    # 親メッセージ（KPI）
    parent_blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*昨日の実績時間:*\n{round(total_hours, 2)}h"},
                {"type": "mrkdwn", "text": f"*昨日の完了タスク数:*\n{tasks_count}"},
                {"type": "mrkdwn", "text": f"*昨日の見積合計:*\n{round(estimated_hours, 2)}h"},
                {"type": "mrkdwn", "text": f"*実績/見積:*\n{ratio_str}"},
            ],
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": baseline}]},
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*MTD 実績:*\n" + f"{round(mtd_hours, 2)}h"},
                {"type": "mrkdwn", "text": "*MTD 件数:*\n" + f"{mtd_tasks}"},
            ],
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"データ: `{config.BQ_TABLE_FQN}`  / TZ: {tz}"}]},
    ]
    ql = _quick_links_elements()
    if ql:
        parent_blocks.append({"type": "context", "elements": ql})

    thread_ts = _post_message(parent_blocks, text_fallback=f"{day_str} 日次ダイジェスト")

    # スレッドにTopセクションを分割投稿
    if thread_ts:
        _post_message([
            {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Projects（昨日）*"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": projects_tbl}},
        ], text_fallback="Top Projects", thread_ts=thread_ts)
        _post_message([
            {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Assignees（昨日）*"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": assignees_tbl}},
        ], text_fallback="Top Assignees", thread_ts=thread_ts)

    # 担当者ごとの異常候補（上位3）
    anomalies_blocks: Optional[List[Dict[str, Any]]] = None
    if hist_assignee_daily:
        # 平均を計算
        from collections import defaultdict
        total_by_assignee: Dict[str, Dict[str, float]] = defaultdict(lambda: {"tasks_sum": 0.0, "hours_sum": 0.0, "days": 0.0})
        for r in hist_assignee_daily:
            key = r.assignee_name
            total_by_assignee[key]["tasks_sum"] += float(r.tasks or 0.0)
            total_by_assignee[key]["hours_sum"] += float(r.hours or 0.0)
            total_by_assignee[key]["days"] += 1.0
        avg_by_assignee: Dict[str, Dict[str, float]] = {}
        for k, v in total_by_assignee.items():
            d = max(1.0, v["days"])
            avg_by_assignee[k] = {
                "avg_tasks": v["tasks_sum"] / d,
                "avg_hours": v["hours_sum"] / d,
            }
        # 昨日の実績
        y_map: Dict[str, Dict[str, float]] = {r.assignee_name: {"tasks": float(r.tasks or 0.0), "hours": float(r.hours or 0.0)} for r in y_by_assignee}
        # 全キー集合
        all_names = set(avg_by_assignee.keys()) | set(y_map.keys())
        candidates = []
        for name in all_names:
            avg = avg_by_assignee.get(name, {"avg_tasks": 0.0, "avg_hours": 0.0})
            if avg["avg_tasks"] <= 0 and avg["avg_hours"] <= 0:
                continue
            yval = y_map.get(name, {"tasks": 0.0, "hours": 0.0})
            hours_vs_pct = None if avg["avg_hours"] == 0 else round((yval["hours"] - avg["avg_hours"]) / avg["avg_hours"] * 100.0, 1)
            tasks_vs_pct = None if avg["avg_tasks"] == 0 else round((yval["tasks"] - avg["avg_tasks"]) / avg["avg_tasks"] * 100.0, 1)
            is_anom = False
            if avg["avg_hours"] > 0 and yval["hours"] < 0.5 * avg["avg_hours"]:
                is_anom = True
            if avg["avg_tasks"] > 0 and yval["tasks"] < 0.5 * avg["avg_tasks"]:
                is_anom = True
            if avg["avg_tasks"] > 0 and yval["tasks"] == 0:
                is_anom = True
            if is_anom:
                candidates.append({
                    "assignee": name,
                    "hours": round(yval["hours"], 2),
                    "tasks": int(yval["tasks"]),
                    "avg_hours": round(avg["avg_hours"], 2),
                    "avg_tasks": round(avg["avg_tasks"], 2),
                    "hours_vs": hours_vs_pct,
                    "tasks_vs": tasks_vs_pct,
                })
        # 強い下振れ順に上位3
        def severity_key(r: Dict[str, Any]) -> float:
            hv = r.get("hours_vs")
            tv = r.get("tasks_vs")
            worst = min([v for v in [hv, tv] if v is not None] or [0.0])
            return worst
        candidates.sort(key=severity_key)
        top3 = candidates[:3]
        if top3:
            anomalies_tbl = _as_mrkdwn_table(
                [
                    {
                        "担当者": r["assignee"],
                        "実績h": r["hours"],
                        "直近Avg h": r["avg_hours"],
                        "h比%": r["hours_vs"],
                        "件数": r["tasks"],
                        "直近Avg 件": r["avg_tasks"],
                        "件比%": r["tasks_vs"],
                    }
                    for r in top3
                ],
                ["担当者", "実績h", "直近Avg h", "h比%", "件数", "直近Avg 件", "件比%"],
                ["担当者", "実績h", "直近Avg h", "h比%", "件数", "直近Avg 件", "件比%"],
            )
            anomalies_blocks = [
                {"type": "section", "text": {"type": "mrkdwn", "text": "*担当者ごとの異常候補（上位3）*"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": anomalies_tbl}},
            ]
    if thread_ts and anomalies_blocks:
        _post_message(anomalies_blocks, text_fallback="担当者の異常候補", thread_ts=thread_ts)

    # 強い下振れアラート（別チャンネル）。-60% 以下なら通知。
    try:
        if (hours_vs is not None and hours_vs <= -60.0) or (tasks_vs is not None and tasks_vs <= -60.0):
            alert_text = f"⚠️ 下振れ検知 {day_str}: 件数 {tasks_vs if tasks_vs is not None else 'N/A'}% / 時間 {hours_vs if hours_vs is not None else 'N/A'}%"
            _post_message_to(
                SLACK_ALERT_CHANNEL_ID or SLACK_CHANNEL_ID,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": alert_text}}],
                text_fallback=alert_text,
            )
    except Exception as _:
        # non-fatal
        pass


def send_open_tasks_summary(bq: bigquery.Client, snapshot_date: Optional[str] = None, top_n: int = 5) -> None:
    """Post a brief summary of open tasks snapshot to the main channel.

    Shows counts, overdue count, and top projects/assignees by open items for a given snapshot_date (YYYY-MM-DD, JST).
    """
    if not _slack_client or not SLACK_CHANNEL_ID:
        return
    tz = "Asia/Tokyo"
    date_expr = f"DATE '{snapshot_date}'" if snapshot_date else f"CURRENT_DATE('{tz}')"

    base = f"""
    WITH s AS (
      SELECT * FROM `{config.GCP_PROJECT_ID}.{config.BQ_DATASET_ID}.open_tasks_snapshot`
      WHERE snapshot_date = {date_expr}
    )
    """

    kpi_sql = base + """
    SELECT COUNT(1) AS open_count, SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue
    FROM s
    """
    top_projects_sql = base + f"""
    SELECT project_name, COUNT(1) AS cnt
    FROM s
    GROUP BY project_name
    ORDER BY cnt DESC
    LIMIT {top_n}
    """
    top_assignees_sql = base + f"""
    SELECT assignee_name, COUNT(1) AS cnt
    FROM s
    WHERE assignee_name IS NOT NULL AND assignee_name != ''
    GROUP BY assignee_name
    ORDER BY cnt DESC
    LIMIT {top_n}
    """

    kpi = next(iter(bq.query(kpi_sql).result()), None)
    projects = list(bq.query(top_projects_sql).result())
    assignees = list(bq.query(top_assignees_sql).result())

    title = f"*📌 未完了タスク スナップショット — {snapshot_date or '(today JST)'}*"
    projects_tbl = _as_mrkdwn_table(
        [{"project": r.project_name, "open": r.cnt} for r in projects],
        ["project", "open"],
        ["プロジェクト", "未完了"]
    )
    assignees_tbl = _as_mrkdwn_table(
        [{"assignee": r.assignee_name, "open": r.cnt} for r in assignees],
        ["assignee", "open"],
        ["担当者", "未完了"]
    )

    blocks: List[Dict[str, Any]] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*未完了件数: *\n{getattr(kpi, 'open_count', 0)}"},
            {"type": "mrkdwn", "text": f"*期日超過: *\n{getattr(kpi, 'overdue', 0)}"},
        ]},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Projects*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": projects_tbl}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Assignees*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": assignees_tbl}},
    ]
    ql = _quick_links_elements()
    if ql:
        blocks.append({"type": "context", "elements": ql})
    _post_message(blocks, text_fallback="未完了タスク スナップショット")


def send_dm_to_assignees_for_open_tasks(*args, **kwargs) -> None:
    """(disabled) DM機能は運用対象外のため no-op。"""
    return
