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

def _parse_custom_fields(custom_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """カスタムフィールドを解析して、見積もり時間、実績時間、時間達成率を取得する"""
    estimated_time = None
    actual_time_raw = None
    achievement_rate = None
    
    for field in custom_fields:
        field_name = field.get('name', '').lower()
        field_value = field.get('value')
        
        # 見積もり時間の取得
        if field_name in ['estimated time', 'estimated_time']:
            if field_value and field_value.get('number_value') is not None:
                estimated_time = field_value['number_value']
        
        # 実績時間の取得
        elif field_name in ['actual_time_raw', 'actual time raw']:
            if field_value and field_value.get('number_value') is not None:
                actual_time_raw = field_value['number_value']
                print(f"  Found actual_time_raw: {actual_time_raw} (minutes) = {actual_time_raw / 60} (hours)")
        
        # 時間達成率の取得
        elif field_name in ['時間達成率', 'achievement_rate']:
            if field_value:
                if field_value.get('number_value') is not None:
                    achievement_rate = field_value['number_value']
                elif field_value.get('text_value'):
                    try:
                        achievement_rate = float(field_value['text_value'])
                    except (ValueError, TypeError):
                        pass
                print(f"  Found achievement_rate (exact match): {achievement_rate}")
    
    # 時間達成率から実績時間を計算
    if achievement_rate is not None and estimated_time is not None and actual_time_raw is None:
        # 時間達成率 = 見積もり時間 ÷ 実績時間
        # よって、実績時間 = 見積もり時間 ÷ 時間達成率
        if achievement_rate != 0:  # ゼロ除算を防ぐ
            actual_time_raw = estimated_time / achievement_rate
            actual_time = actual_time_raw / 60  # 分から時間に変換
            print(f"  Calculated from achievement_rate: {actual_time_raw} (minutes) = {actual_time} (hours)")
        else:
            print(f"  Skipping calculation: achievement_rate is 0")
    
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
        subtasks = tasks_api.get_subtasks_for_task(
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
    tasks_response = tasks_api.get_tasks_for_project(
        project_gid=project_id,
        opt_fields=['name', 'gid', 'completed', 'completed_at', 'created_at', 'due_on', 'modified_at', 'assignee', 'custom_fields', 'num_subtasks'],
        completed_since='2024-01-01'  # 2024年以降の完了タスクを取得
    )
    
    completed_tasks = []
    
    for task_dict in tasks_response:
        # 完了したタスクのみを処理
        if not task_dict.get('completed') or not task_dict.get('completed_at'):
            continue
            
        # 親タスクの時間を取得（サブタスクがある場合も含める）
        time_fields = _parse_custom_fields(task_dict.get('custom_fields', []))
        assignee = task_dict.get('assignee')
        
        # 親タスクをフォーマット
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
        
        # サブタスクを取得
        num_subtasks = task_dict.get('num_subtasks', 0)
        if num_subtasks > 0:
            subtasks = get_subtasks(tasks_api, task_dict['gid'])
            print(f"Found {len(subtasks)} subtasks for task {task_dict['name']}")
            
            for subtask_dict in subtasks:
                # 完了したサブタスクのみを追加
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
            
            # APIレート制限を避けるため少し待機
            time.sleep(0.2)
        
    print(f"Found {len(completed_tasks)} completed tasks (including subtasks) in '{project_name}'.")
    return completed_tasks