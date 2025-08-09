import asana
import time
import random
from typing import List, Dict, Any, Tuple, Callable

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
    """ワークスペース内の全てのプロジェクトを取得する"""
    # 既存呼び出しとの互換性のため、引数は未使用でも可
    _, projects_api, _ = get_asana_client()
    print(f"Fetching projects from workspace: {config.ASANA_WORKSPACE_ID}")
    
    try:
        # opt_fields を指定して必要なフィールドを取得
        projects = _with_retry(
            projects_api.get_projects_for_workspace,
            config.ASANA_WORKSPACE_ID,
            opts={'opt_fields': 'name,gid'}
        )
        return list(projects)
    except asana.rest.ApiException as e:
        print(f"Error fetching projects: {e}")
        raise

def _with_retry(fn: Callable, *args, **kwargs):
    """429/5xx に対する指数バックオフ付きリトライ"""
    max_attempts = 6
    base_delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args, **kwargs)
        except asana.rest.ApiException as e:
            status = getattr(e, 'status', None) or getattr(e, 'code', None)
            is_retryable = status in (429, 500, 502, 503, 504)
            if not is_retryable or attempt == max_attempts:
                print(f"Asana API error (status={status}) on attempt {attempt}/{max_attempts}: {e}")
                raise
            # Exponential backoff with jitter
            sleep_sec = base_delay * (2 ** (attempt - 1))
            sleep_sec = sleep_sec * (0.8 + 0.4 * random.random())
            print(f"Retrying Asana API (status={status}) in {sleep_sec:.1f}s... (attempt {attempt})")
            time.sleep(sleep_sec)

def _parse_custom_fields(custom_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """カスタムフィールドを解析して、見積もり時間、実績時間、時間達成率を取得する"""
    estimated_time = None
    actual_time_raw = None
    achievement_rate = None
    achievement_rate_field_name = None
    
    for field in custom_fields:
        original_name = field.get('name', '')
        field_name = original_name.lower()
        field_value = field.get('value')
        
        # 見積もり時間の取得
        if field_name in ['estimated time', 'estimated_time', '見積時間', '見積もり時間', 'estimate']:
            if field_value and field_value.get('number_value') is not None:
                estimated_time = field_value['number_value']
        
        # 実績時間の取得
        elif field_name in ['actual_time_raw', 'actual time raw']:
            if field_value and field_value.get('number_value') is not None:
                actual_time_raw = field_value['number_value']
                print(f"  Found actual_time_raw: {actual_time_raw} (minutes) = {actual_time_raw / 60} (hours)")
        elif field_name in ['actual time', 'actual_time', '実績時間']:
            # 時間単位の実績が入力されているケースをサポート（hours想定）
            if field_value and field_value.get('number_value') is not None:
                hours_value = field_value['number_value']
                actual_time_raw = hours_value * 60
                print(f"  Found actual_time (hours): {hours_value} -> {actual_time_raw} (minutes)")
        
        # 時間達成率の取得
        elif field_name in ['時間達成率', 'achievement_rate']:
            if field_value:
                if field_value.get('number_value') is not None:
                    achievement_rate = field_value['number_value']
                elif field_value.get('text_value'):
                    try:
                        text_val = str(field_value['text_value']).strip()
                        # 例: "120%" を 1.2 に変換
                        if text_val.endswith('%'):
                            achievement_rate = float(text_val.rstrip('%')) / 100.0
                        else:
                            achievement_rate = float(text_val)
                    except (ValueError, TypeError):
                        pass
                achievement_rate_field_name = original_name
                print(f"  Found achievement_rate (exact match): {achievement_rate}")
    
    # 時間達成率から実績時間を計算
    if achievement_rate is not None and estimated_time is not None and actual_time_raw is None:
        # 一般的に「達成率」= 実績/見積。比率(0〜1) または パーセント(0〜100) の両方に対応
        rate = achievement_rate
        if rate > 1.0:
            # 100 を 1.0 に正規化（1000%以上の異常値はそのまま扱う）
            if rate <= 1000:
                rate = rate / 100.0
        if rate > 0:
            actual_time_raw = estimated_time * rate
            actual_time = actual_time_raw / 60
            print(f"  Calculated actual_time from achievement_rate '{achievement_rate_field_name}': {actual_time_raw} (minutes) = {actual_time} (hours)")
        else:
            print(f"  Skipping calculation: non-positive achievement_rate: {achievement_rate}")
    
    # 実績時間が取得できない場合の処理
    if actual_time_raw is None:
        print(f"  Skipping achievement rate calculation: has_actual_time_raw=False, achievement_rate={achievement_rate}, estimated_time={estimated_time}")
        return {
            'estimated_time': estimated_time,
            'actual_time': None,
            'actual_time_raw': None
        }
    
    actual_time = actual_time_raw / 60  # 分から時間に変換
    
    return {
        'estimated_time': estimated_time,
        'actual_time': actual_time,
        'actual_time_raw': actual_time_raw
    }

def get_subtasks(tasks_api: asana.TasksApi, parent_task_id: str) -> List[Dict[str, Any]]:
    """指定されたタスクのサブタスクを取得する"""
    try:
        subtasks = _with_retry(
            tasks_api.get_subtasks_for_task,
            parent_task_id,
            opts={
                'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee.name,custom_fields'
            }
        )
        return list(subtasks)
    except asana.rest.ApiException as e:
        print(f"Error fetching subtasks for task {parent_task_id}: {e}")
        return []

def get_completed_tasks_for_project(api_client: asana.ApiClient, project: Dict[str, Any]) -> List[Dict[str, Any]]:
    """プロジェクトから完了したタスクを取得する（サブタスク対応）"""
    tasks_api = asana.TasksApi(api_client)
    project_name = project['name']
    project_id = project['gid']
    
    print(f"Fetching completed tasks from project: {project_name}")
    
    # 完了したタスクを取得
    tasks_response = _with_retry(
        tasks_api.get_tasks_for_project,
        project_id,
        opts={
            'opt_fields': 'name,gid,completed,completed_at,created_at,due_on,modified_at,assignee.name,custom_fields,num_subtasks',
            'completed_since': '2024-01-01'
        }
    )
    
    completed_tasks = []
    
    for task_dict in tasks_response:
        parent_is_completed = bool(task_dict.get('completed') and task_dict.get('completed_at'))

        # 親タスクが完了している場合のみ、親タスク自体をレコードとして追加
        if parent_is_completed:
            time_fields = _parse_custom_fields(task_dict.get('custom_fields', []))
            assignee = task_dict.get('assignee')

            formatted_task = {
                'task_id': task_dict['gid'],
                'task_name': task_dict['name'],
                'project_id': project['gid'],
                'project_name': project['name'],
                'assignee_name': assignee['name'] if assignee else None,
                'completed_at': task_dict.get('completed_at'),
                'created_at': task_dict.get('created_at'),
                'due_on': task_dict.get('due_on'),
                'modified_at': task_dict.get('modified_at'),
                'estimated_time': time_fields.get('estimated_time'),
                'actual_time': time_fields.get('actual_time'),
                'actual_time_raw': time_fields.get('actual_time_raw'),
                'is_subtask': False,
                'parent_task_id': None
            }

            completed_tasks.append(formatted_task)

        # 親タスクの完了有無に関わらず、配下の完了済みサブタスクは取り込む
        num_subtasks = task_dict.get('num_subtasks', 0)
        if num_subtasks > 0:
            subtasks = get_subtasks(tasks_api, task_dict['gid'])
            print(f"Found {len(subtasks)} subtasks for task {task_dict['name']}")

            for subtask_dict in subtasks:
                if subtask_dict.get('completed') and subtask_dict.get('completed_at'):
                    subtask_time_fields = _parse_custom_fields(subtask_dict.get('custom_fields', []))
                    subtask_assignee = subtask_dict.get('assignee')

                    formatted_subtask = {
                        'task_id': subtask_dict['gid'],
                        'task_name': f"[Subtask] {subtask_dict['name']}",
                        'project_id': project['gid'],
                        'project_name': project['name'],
                        'assignee_name': subtask_assignee['name'] if subtask_assignee else None,
                        'completed_at': subtask_dict.get('completed_at'),
                        'created_at': subtask_dict.get('created_at'),
                        'due_on': subtask_dict.get('due_on'),
                        'modified_at': subtask_dict.get('modified_at'),
                        'estimated_time': subtask_time_fields.get('estimated_time'),
                        'actual_time': subtask_time_fields.get('actual_time'),
                        'actual_time_raw': subtask_time_fields.get('actual_time_raw'),
                        'is_subtask': True,
                        'parent_task_id': task_dict['gid']
                    }

                    completed_tasks.append(formatted_subtask)

            time.sleep(0.2)
        
    print(f"Found {len(completed_tasks)} completed tasks (including subtasks) in '{project_name}'.")
    return completed_tasks