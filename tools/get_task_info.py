import asana
import os
import sys

from asana_reporter import config


def main(task_gid: str):
    configuration = asana.Configuration()
    configuration.access_token = config.ASANA_ACCESS_TOKEN
    api_client = asana.ApiClient(configuration)
    tasks_api = asana.TasksApi(api_client)

    fields = [
        'gid', 'name', 'completed', 'completed_at', 'modified_at', 'created_at',
        'assignee.name', 'projects.name', 'parent',
        'actual_time_minutes',
        'custom_fields', 'custom_fields.name', 'custom_fields.number_value', 'custom_fields.text_value', 'custom_fields.display_value'
    ]
    task = tasks_api.get_task(task_gid, opts={'opt_fields': ','.join(fields)})

    print(f"task_id: {task.get('gid')}")
    print(f"name: {task.get('name')}")
    print(f"completed: {task.get('completed')}")
    print(f"completed_at: {task.get('completed_at')}")
    print(f"modified_at: {task.get('modified_at')}")
    assignee = task.get('assignee')
    print(f"assignee: {assignee.get('name') if assignee else None}")
    print(f"projects: {[p.get('name') for p in (task.get('projects') or [])]}")
    print(f"actual_time_minutes(native): {task.get('actual_time_minutes')}")

    # parse custom fields rough
    est = None
    act_min = None
    act_hours = None
    for f in task.get('custom_fields', []) or []:
        name_lower = (f.get('name') or '').lower()
        if any(k in name_lower for k in ['estimated', '見積']):
            val = f.get('number_value')
            if isinstance(val, (int, float)):
                est = val
        if any(k in name_lower for k in ['actual', '実績', '稼働', '工数']):
            val = f.get('number_value')
            if isinstance(val, (int, float)):
                act_min = val
                act_hours = act_min / 60.0
    print(f"estimated_time(minutes): {est}")
    print(f"actual_time_raw(minutes): {act_min}")
    print(f"actual_time(hours): {act_hours}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python tools/get_task_info.py <task_gid>')
        sys.exit(1)
    main(sys.argv[1])


