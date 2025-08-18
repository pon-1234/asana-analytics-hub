import os
from typing import Optional, List, Dict, Any

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from google.cloud import bigquery

from . import config

# Environment-driven configuration (optional; safe to skip when unset)
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")

_slack_client: Optional[WebClient] = WebClient(token=SLACK_BOT_TOKEN) if SLACK_BOT_TOKEN else None


def _post_message(blocks: List[Dict[str, Any]], text_fallback: str, thread_ts: Optional[str] = None) -> Optional[str]:
    """Post a message to Slack if configured. Returns ts or None.

    This function is intentionally non-fatal: if Slack is not configured or the
    API call fails, it prints a message and returns None without raising.
    """
    if not _slack_client or not SLACK_CHANNEL_ID:
        print("Slack token/channel not configured. Skipping Slack post.")
        return None
    try:
        resp = _slack_client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
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
        links.append(f"<${{sheets_url}}|Sheets>")
    if config.GCP_PROJECT_ID:
        bq_url = f"https://console.cloud.google.com/bigquery?project={config.GCP_PROJECT_ID}"
        links.append(f"<${{bq_url}}|BigQuery>")
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
      SELECT * EXCEPT(row_num) FROM (
        SELECT
          task_id, TRIM(project_name) AS project_name, assignee_name,
          completed_at, actual_time, estimated_time, modified_at, inserted_at,
          ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY modified_at DESC, inserted_at DESC) AS row_num
        FROM `{config.BQ_TABLE_FQN}`
        WHERE completed_at IS NOT NULL
      ) WHERE row_num = 1
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
      SELECT * EXCEPT(row_num) FROM (
        SELECT
          task_id, TRIM(project_name) AS project_name, assignee_name,
          completed_at, actual_time, estimated_time, modified_at, inserted_at,
          ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY modified_at DESC, inserted_at DESC) AS row_num
        FROM `{config.BQ_TABLE_FQN}`
        WHERE completed_at IS NOT NULL
      ) WHERE row_num = 1
    )
    """

    y_sql = base + f"""
    SELECT
      COUNT(task_id) AS tasks_count,
      SUM(IFNULL(actual_time,0.0)) AS total_hours,
      SUM(IFNULL(estimated_time,0.0))/60.0 AS estimated_hours
    FROM unique_tasks
    WHERE DATE(completed_at, '{tz}') = {y_expr}
    """

    w_sql = base + f"""
    SELECT
      AVG(tasks) AS avg_tasks,
      AVG(hours) AS avg_hours
    FROM (
      SELECT
        DATE(completed_at, '{tz}') AS d,
        COUNT(task_id) AS tasks,
        SUM(IFNULL(actual_time,0.0)) AS hours
      FROM unique_tasks
      WHERE DATE(completed_at, '{tz}') BETWEEN DATE_SUB({y_expr}, INTERVAL 8 DAY) AND DATE_SUB({y_expr}, INTERVAL 1 DAY)
      GROUP BY d
    )
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
      SUM(IF(DATE(completed_at, '{tz}') BETWEEN DATE_TRUNC({y_expr}, MONTH) AND {y_expr}, IFNULL(actual_time,0.0), 0.0)) AS mtd_hours,
      COUNTIF(DATE(completed_at, '{tz}') BETWEEN DATE_TRUNC({y_expr}, MONTH) AND {y_expr}) AS mtd_tasks
    FROM unique_tasks
    """

    y = next(iter(bq.query(y_sql).result()), None)
    w_iter = list(bq.query(w_sql).result())
    w = w_iter[0] if w_iter else None
    projects = list(bq.query(top_projects_sql).result())
    assignees = list(bq.query(top_assignees_sql).result())
    mtd = next(iter(bq.query(mtd_sql).result()), None)

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

    blocks: List[Dict[str, Any]] = [
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
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Projects（昨日）*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": projects_tbl}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*Top Assignees（昨日）*"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": assignees_tbl}},
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
        blocks.append({"type": "context", "elements": ql})
    _post_message(blocks, text_fallback=f"{day_str} 日次ダイジェスト")
