import sys
from typing import Tuple

# Cloud Functions (Gen2) entry points live in this module.
# Delegate actual implementations to asana_reporter.main to avoid duplication.

from asana_reporter.main import (
    fetch_asana_tasks_to_bq as _impl_fetch_asana_tasks_to_bq,
    export_reports_to_sheets as _impl_export_reports_to_sheets,
)


def fetch_asana_tasks_to_bq(request=None):
    return _impl_fetch_asana_tasks_to_bq(request)


def export_reports_to_sheets(request=None):
    return _impl_export_reports_to_sheets(request)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == 'fetch':
            print("Running 'fetch_asana_tasks_to_bq' locally...")
            print(fetch_asana_tasks_to_bq())
        elif command == 'export':
            print("Running 'export_reports_to_sheets' locally...")
            print(export_reports_to_sheets())
        else:
            print(f"Unknown command: {command}. Use 'fetch' or 'export'.")
    else:
        print("Please provide a command: 'fetch' or 'export'.")
