import asana
import time
import requests
from typing import List, Dict, Any, Tuple

from . import config

def get_asana_client() -> Tuple[asana.ApiClient, asana.ProjectsApi, asana.TasksApi]:
    """Asana APIクライアントと必要なAPIインスタンスを初期化して返す"""
    configuration = asana.Configuration()
    configuration.access_token = config.ASANA_ACCESS_TOKEN
    api_client = asana.ApiClient(configuration)
    
    projects_api = asana.ProjectsApi(api_client)
    tasks_api = asana.TasksApi(api_client)
    
    return api_client, projects_api, tasks_api

def get_all_projects(api_client: asana.ApiClient) -> List[Dict[str, Any]]:
    """ワークスペース内の全てのプロジェクトを取得する（引数のクライアントを使用）"""
    projects_api = asana.ProjectsApi(api_client)
    print(f"Fetching projects from workspace: {config.ASANA_WORKSPACE_ID}")
    
    try:
        projects = projects_api.get_projects_for_workspace(
            config.ASANA_WORKSPACE_ID,
            opts={'opt_fields': 'name,gid,archived'}
        )
        # アーカイブされていないプロジェクトのみを返す
        active_projects = [p for p in projects if not p.get('archived')]
        print(f"Found {len(active_projects)} active projects.")
        return active_projects
    except asana.rest.ApiException as e:
        print(f"Error fetching projects: {e}")
        raise

def _parse_custom_fields(custom_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """カスタムフィールドを解析して、見積もり時間(分)、実績時間(分)を取得する。

    Asanaのカスタムフィールドは value ではなく number_value/text_value/display_value を持つため、
    これらを優先して解釈する。
    """
    estimated_time_minutes = None
    actual_time_minutes = None

    def parse_numeric_from_field(field: Dict[str, Any]) -> float | None:
        # number_value を最優先
        number_value = field.get('number_value')
        if isinstance(number_value, (int, float)):
            return float(number_value)
        # text_value/display_value から数値を抽出（"10h", "600分" などに対応）
        text = field.get('text_value') or field.get('display_value')
        if not text or not isinstance(text, str):
            return None
        import re
        match = re.search(r"[-+]?[0-9]*\.?[0-9]+", text)
        if not match:
            return None
        value = float(match.group(0))
        # 単位推定（h/時間 → 時間、m/分 → 分）。明記なければ分として扱う。
        lowered = text.lower()
        if 'h' in lowered or '時間' in text:
            return value * 60.0
        # 'm' は曖昧なので '分' か 'min' を優先
        if '分' in text or 'min' in lowered:
            return value
        return value  # 既定: 分

    for field in custom_fields:
        name_lower = field.get('name', '').lower()

        # 見積もり時間
        if any(keyword in name_lower for keyword in ['estimated time', 'estimated_time', 'estimate', '見積', '見積もり']):
            parsed = parse_numeric_from_field(field)
            if parsed is not None:
                estimated_time_minutes = parsed
                continue

        # 実績時間（実働/工数/稼働などもカバー）
        if any(keyword in name_lower for keyword in ['actual time', 'actual_time', 'spent time', 'tracked time', '実績', '実働', '工数', '稼働']):
            parsed = parse_numeric_from_field(field)
            if parsed is not None:
                actual_time_minutes = parsed
                continue

    actual_time_hours = (actual_time_minutes / 60.0) if actual_time_minutes is not None else None

    return {
        'estimated_time': estimated_time_minutes,  # 分
        'actual_time': actual_time_hours,          # 時間
        'actual_time_raw': actual_time_minutes     # 分
    }

def get_subtasks(tasks_api: asana.TasksApi, parent_task_id: str) -> List[Dict[str, Any]]:
    """指定されたタスクのサブタスクを取得する"""
    try:
        subtasks = tasks_api.get_subtasks_for_task(
            parent_task_id,
            opts={
                'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee.name,actual_time_minutes,custom_fields'
            }
        )
        return list(subtasks)
    except asana.rest.ApiException as e:
        print(f"Error fetching subtasks for task {parent_task_id}: {e}")
        return []

# ★改善点: 差分取得/バックフィルに対応
def get_completed_tasks_for_project(
    api_client: asana.ApiClient,
    project: Dict[str, Any],
    modified_since: str | None = None,
    force_parent_sweep: bool = False,
    completed_since_override: str | None = None,
    include_incomplete_subtasks: bool = False,
) -> List[Dict[str, Any]]:
    """プロジェクトから完了したタスクを取得する（差分取得・サブタスク対応）"""
    tasks_api = asana.TasksApi(api_client)
    project_name = project['name']
    project_id = project['gid']

    # APIに渡すオプションを設定
    opts = {
        'opt_fields': [
            'name', 'gid', 'completed', 'completed_at', 'created_at', 'due_on', 'modified_at',
            'assignee', 'assignee.name', 'num_subtasks',
            'actual_time_minutes',
            # カスタムフィールドの実値も明示取得
            'custom_fields', 'custom_fields.name', 'custom_fields.number_value', 'custom_fields.text_value', 'custom_fields.display_value'
        ],
        'limit': 100,
    }

    if force_parent_sweep:
        # 親が未完了でも一覧に出るように、完了基準ではなく更新基準で全親を掃引
        opts['modified_since'] = completed_since_override or '1970-01-01T00:00:00.000Z'
        print(f"  Backfill: sweeping parents with modified_since={opts.get('modified_since')}")
    elif modified_since:
        # 差分取得
        opts['modified_since'] = modified_since
        print(f"  Fetching tasks modified since {modified_since}")
    else:
        # フル取得（通常）
        opts['completed_since'] = completed_since_override or '2023-01-01T00:00:00.000Z'
        print(f"  Performing full fetch for tasks completed since {opts['completed_since']}")

    def _with_retry(call, *args, **kwargs):
        attempts = 0
        while True:
            try:
                return call(*args, **kwargs)
            except asana.rest.ApiException as e:
                # リトライ対象
                if getattr(e, 'status', None) in (429, 500, 502, 503):
                    retry_after = None
                    try:
                        retry_after = int(getattr(e, 'headers', {}).get('Retry-After', '0'))
                    except Exception:
                        retry_after = None
                    sleep_sec = retry_after if retry_after and retry_after > 0 else min(30, 2 ** attempts)
                    attempts += 1
                    if attempts > 5:
                        raise
                    time.sleep(sleep_sec)
                    continue
                # 404などはそのまま
                if getattr(e, 'status', None) == 404:
                    raise
                raise

    try:
        tasks_response = _with_retry(
            tasks_api.get_tasks_for_project,
            project_gid=project_id,
            opts=opts,
        )
    except asana.rest.ApiException as e:
        # プロジェクトが存在しない場合など (404)
        if getattr(e, 'status', None) == 404:
            print(f"  Project '{project_name}' not found or access denied. Skipping.")
            return []
        raise

    processed_tasks = []
    
    for task_dict in tasks_response:
        # まず、サブタスクを処理（親が未完了でも完了済みサブタスクは取り込む）
        if task_dict.get('num_subtasks', 0) > 0:
            def _get_subtasks_with_retry():
                return _with_retry(
                    tasks_api.get_subtasks_for_task,
                    parent_task_id=task_dict['gid'],
                    opts={
                        'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee.name,actual_time_minutes,custom_fields'
                    }
                )
            subtasks = _get_subtasks_with_retry()
            for subtask_dict in subtasks:
                is_completed = subtask_dict.get('completed') and subtask_dict.get('completed_at')
                if is_completed or include_incomplete_subtasks:
                    subtask_time_fields = _parse_custom_fields(subtask_dict.get('custom_fields', []))
                    atm = subtask_dict.get('actual_time_minutes')
                    if isinstance(atm, (int, float)):
                        subtask_time_fields['actual_time_raw'] = float(atm)
                        subtask_time_fields['actual_time'] = float(atm) / 60.0
                    subtask_assignee = subtask_dict.get('assignee')
                    formatted_subtask = {
                        'task_id': subtask_dict['gid'],
                        'task_name': f"[Subtask] {subtask_dict['name']}",
                        'project_id': project['gid'],
                        'project_gid': project['gid'],
                        'project_name': project['name'],
                        'assignee_name': subtask_assignee['name'] if subtask_assignee else None,
                        'assignee_gid': subtask_assignee['gid'] if subtask_assignee else None,
                        'completed_at': subtask_dict.get('completed_at'),
                        'created_at': subtask_dict.get('created_at'),
                        'due_on': subtask_dict.get('due_on'),
                        'modified_at': subtask_dict.get('modified_at'),
                        'estimated_time': subtask_time_fields.get('estimated_time'),
                        'actual_time': subtask_time_fields.get('actual_time'),
                        'actual_time_raw': subtask_time_fields.get('actual_time_raw'),
                        'estimated_minutes': subtask_time_fields.get('estimated_time'),
                        'actual_minutes': subtask_time_fields.get('actual_time_raw'),
                        'is_subtask': True,
                        'parent_task_id': task_dict['gid']
                    }
                    processed_tasks.append(formatted_subtask)
            time.sleep(0.2)  # サブタスク取得後の待機

        # 次に、親タスクが完了済みなら親も取り込む
        if task_dict.get('completed') and task_dict.get('completed_at'):
            time_fields = _parse_custom_fields(task_dict.get('custom_fields', []))
            atm = task_dict.get('actual_time_minutes')
            if isinstance(atm, (int, float)):
                time_fields['actual_time_raw'] = float(atm)
                time_fields['actual_time'] = float(atm) / 60.0
            assignee = task_dict.get('assignee')
            formatted_task = {
                'task_id': task_dict['gid'],
                'task_name': task_dict['name'],
                'project_id': project['gid'],
                'project_gid': project['gid'],
                'project_name': project['name'],
                'assignee_name': assignee['name'] if assignee else None,
                'assignee_gid': assignee['gid'] if assignee else None,
                'completed_at': task_dict.get('completed_at'),
                'created_at': task_dict.get('created_at'),
                'due_on': task_dict.get('due_on'),
                'modified_at': task_dict.get('modified_at'),
                'estimated_time': time_fields.get('estimated_time'),
                'actual_time': time_fields.get('actual_time'),
                'actual_time_raw': time_fields.get('actual_time_raw'),
                'estimated_minutes': time_fields.get('estimated_time'),
                'actual_minutes': time_fields.get('actual_time_raw'),
                'is_subtask': False,
                'parent_task_id': None
            }
            processed_tasks.append(formatted_task)
            
    print(f"  Found {len(processed_tasks)} completed/updated tasks in '{project_name}'.")
    return processed_tasks


def get_open_tasks_for_project(
    api_client: asana.ApiClient,
    project: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """プロジェクト内の未完了タスク（親・サブタスク含む）をスナップショット用に取得する。

    Asanaの仕様:
      - completed_since=now を指定すると未完了タスクのみを返す（完了済みは返らない）
    """
    tasks_api = asana.TasksApi(api_client)
    project_name = project['name']
    project_id = project['gid']

    opts = {
        'completed_since': 'now',
        'opt_fields': [
            'name', 'gid', 'completed', 'completed_at', 'created_at', 'due_on', 'modified_at',
            'assignee', 'assignee.name', 'assignee.gid', 'num_subtasks',
            'actual_time_minutes',
            'custom_fields', 'custom_fields.name', 'custom_fields.number_value', 'custom_fields.text_value', 'custom_fields.display_value'
        ],
        'limit': 100,
    }

    def _with_retry(call, *args, **kwargs):
        attempts = 0
        while True:
            try:
                return call(*args, **kwargs)
            except asana.rest.ApiException as e:
                if getattr(e, 'status', None) in (429, 500, 502, 503):
                    retry_after = None
                    try:
                        retry_after = int(getattr(e, 'headers', {}).get('Retry-After', '0'))
                    except Exception:
                        retry_after = None
                    sleep_sec = retry_after if retry_after and retry_after > 0 else min(30, 2 ** attempts)
                    attempts += 1
                    if attempts > 5:
                        raise
                    time.sleep(sleep_sec)
                    continue
                raise

    try:
        tasks_response = _with_retry(
            tasks_api.get_tasks_for_project,
            project_gid=project_id,
            opts=opts,
        )
    except asana.rest.ApiException as e:
        if getattr(e, 'status', None) == 404:
            print(f"  Project '{project_name}' not found or access denied. Skipping (open tasks).")
            return []
        raise

    open_rows: List[Dict[str, Any]] = []
    for task_dict in tasks_response:
        # 親・未完了
        if not task_dict.get('completed'):
            assignee = task_dict.get('assignee')
            has_time_fields = bool(_parse_custom_fields(task_dict.get('custom_fields', [])).get('estimated_time') or _parse_custom_fields(task_dict.get('custom_fields', [])).get('actual_time_raw'))
            open_rows.append({
                'task_id': task_dict['gid'],
                'task_name': task_dict.get('name'),
                'project_gid': project_id,
                'project_name': project_name,
                'assignee_gid': assignee['gid'] if assignee else None,
                'assignee_name': assignee['name'] if assignee else None,
                'due_on': task_dict.get('due_on'),
                'created_at': task_dict.get('created_at'),
                'modified_at': task_dict.get('modified_at'),
                'is_overdue': False,  # 後段で判定
                'has_time_fields': has_time_fields,
            })

        # サブタスク（未完）も取得
        if task_dict.get('num_subtasks', 0) > 0:
            try:
                subtasks = _with_retry(
                    tasks_api.get_subtasks_for_task,
                    parent_task_id=task_dict['gid'],
                    opts={'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee.name,assignee.gid,actual_time_minutes,custom_fields'}
                )
            except asana.rest.ApiException:
                subtasks = []
            for subtask in subtasks:
                if subtask.get('completed'):
                    continue
                sub_assignee = subtask.get('assignee')
                has_time_fields = bool(_parse_custom_fields(subtask.get('custom_fields', [])).get('estimated_time') or _parse_custom_fields(subtask.get('custom_fields', [])).get('actual_time_raw'))
                open_rows.append({
                    'task_id': subtask['gid'],
                    'task_name': f"[Subtask] {subtask.get('name')}",
                    'project_gid': project_id,
                    'project_name': project_name,
                    'assignee_gid': sub_assignee['gid'] if sub_assignee else None,
                    'assignee_name': sub_assignee['name'] if sub_assignee else None,
                    'due_on': subtask.get('due_on'),
                    'created_at': subtask.get('created_at'),
                    'modified_at': subtask.get('modified_at'),
                    'is_overdue': False,
                    'has_time_fields': has_time_fields,
                })

    return open_rows


def get_time_tracking_entries_between(
    start_on: str,
    end_on: str,
    workspace_gid: str,
    user_gid: str | None = None,
    project_gid: str | None = None,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """/time_tracking_entries を使って期間内のエントリを取得（entered_onベース）。"""
    url = "https://app.asana.com/api/1.0/time_tracking_entries"
    from . import config as _cfg
    headers = {"Authorization": f"Bearer {_cfg.ASANA_ACCESS_TOKEN}"}
    params = {
        "start_on": start_on,
        "end_on": end_on,
        "limit": page_size,
        "opt_fields": "gid,entered_on,duration_minutes,created_at,modified_at,task.gid,task.name,user.name,attributed_project.gid,attributed_project.name",
        "workspace": workspace_gid,
    }
    if user_gid:
        params["user"] = user_gid
    if project_gid:
        params["attributed_project"] = project_gid

    entries: List[Dict[str, Any]] = []
    offset = None
    while True:
        q = dict(params)
        if offset:
            q["offset"] = offset
        resp = requests.get(url, headers=headers, params=q, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        entries.extend(body.get("data", []))
        offset = (body.get("next_page") or {}).get("offset")
        if not offset:
            break
    return entries


def format_entries_for_bq(raw_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for e in raw_entries:
        task = e.get("task") or {}
        user = e.get("user") or {}
        proj = e.get("attributed_project") or {}
        rows.append({
            "entry_id": e.get("gid"),
            "task_id": task.get("gid"),
            "task_name": task.get("name"),
            "project_id": proj.get("gid"),
            "project_name": proj.get("name"),
            "user_name": user.get("name"),
            "entered_on": e.get("entered_on"),
            "duration_minutes": e.get("duration_minutes"),
            "created_at": e.get("created_at"),
            "modified_at": e.get("modified_at"),
        })
    return rows

