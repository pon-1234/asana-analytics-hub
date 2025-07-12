import asana
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
    """ワークスペース内の全てのプロジェクトを取得する"""
    _, projects_api, _ = get_asana_client()
    print(f"Fetching projects from workspace: {config.ASANA_WORKSPACE_ID}")
    
    try:
        # opt_fields を指定して必要なフィールドを取得
        projects = projects_api.get_projects_for_workspace(
            config.ASANA_WORKSPACE_ID,
            opts={'opt_fields': 'name,gid'}
        )
        return list(projects)
    except asana.rest.ApiException as e:
        print(f"Error fetching projects: {e}")
        raise

def _parse_custom_fields(task: Dict[str, Any]) -> Dict[str, Any]:
    """タスクのカスタムフィールドを解析し、時間関連の値を抽出する"""
    estimated_time = None
    actual_time = None
    actual_time_raw = None
    has_actual_time_raw = False

    for field in task.get('custom_fields', []):
        if not field:
            continue
        
        # 見積もり時間 (分単位)
        if field.get('name') == 'Estimated time' and field.get('number_value') is not None:
            estimated_time = field['number_value']
        
        # 実績時間_raw (分単位)
        elif field.get('name') == 'actual_time_raw' and field.get('number_value') is not None:
            actual_time_raw = field['number_value']
            actual_time = actual_time_raw / 60  # 時間単位に変換
            has_actual_time_raw = True
            
        # 時間達成率 (実績時間_rawがない場合のみ使用)
        elif field.get('name') == '時間達成率' and field.get('number_value') is not None and not has_actual_time_raw:
            achievement_rate = field['number_value']
            if estimated_time is not None and achievement_rate > 0:
                actual_time_raw = estimated_time * achievement_rate
                actual_time = actual_time_raw / 60 # 時間単位に変換

    return {
        "estimated_time": estimated_time,
        "actual_time": actual_time,
        "actual_time_raw": actual_time_raw
    }

def get_completed_tasks_for_project(api_client: asana.ApiClient, project: Dict[str, Any]) -> List[Dict[str, Any]]:
    """指定されたプロジェクトの完了タスクを取得し、整形する"""
    _, _, tasks_api = get_asana_client()
    project_id = project['gid']
    project_name = project['name']
    
    print(f"Fetching tasks for project: '{project_name}' ({project_id})")
    
    try:
        # タスクを取得
        tasks = tasks_api.get_tasks_for_project(
            project_id,
            opts={
                'completed_since': 'now',  # 最近完了したタスクに絞る
                'opt_fields': 'name,completed,completed_at,created_at,modified_at,due_on,assignee.name,custom_fields'
            }
        )
        
        completed_tasks = []
        for task in tasks:
            task_dict = task if isinstance(task, dict) else task
            if not task_dict.get('completed') or not task_dict.get('completed_at'):
                continue

            time_fields = _parse_custom_fields(task_dict)
            
            assignee = task_dict.get('assignee')
            
            formatted_task = {
                'task_id': task_dict['gid'],
                'task_name': task_dict['name'],
                'project_id': project_id,
                'project_name': project_name,
                'assignee_name': assignee.get('name') if assignee else None,
                'completed_at': task_dict['completed_at'],
                'created_at': task_dict['created_at'],
                'due_on': task_dict.get('due_on'),
                'modified_at': task_dict['modified_at'],
                **time_fields
            }
            completed_tasks.append(formatted_task)
            
        print(f"Found {len(completed_tasks)} completed tasks in '{project_name}'.")
        return completed_tasks

    except asana.rest.ApiException as e:
        print(f"Error fetching tasks for project '{project_name}': {e}")
        return []