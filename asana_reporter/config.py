import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む (ローカル実行用)
# Cloud Functions環境ではランタイムの環境変数が使われる
load_dotenv()

# --- GCP Settings ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
# ローカル実行時はファイルパスから、GCP環境ではADC (Application Default Credentials) を使用
GCP_CREDENTIALS_PATH = os.getenv('GCP_CREDENTIALS_PATH')

# --- Asana Settings ---
ASANA_ACCESS_TOKEN = os.getenv('ASANA_ACCESS_TOKEN')
ASANA_WORKSPACE_ID = os.getenv('ASANA_WORKSPACE_ID')

# --- Google Sheets Settings ---
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

# --- BigQuery Settings ---
BQ_DATASET_ID = "asana_analytics"
BQ_TABLE_ID = "completed_tasks"
BQ_TABLE_FQN = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"

# --- Validation ---
def validate_config():
    """必要な環境変数が設定されているか検証する"""
    required_vars = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "ASANA_ACCESS_TOKEN": ASANA_ACCESS_TOKEN,
        "ASANA_WORKSPACE_ID": ASANA_WORKSPACE_ID,
        "SPREADSHEET_ID": SPREADSHEET_ID,
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # ローカル実行時のみ認証情報ファイルの存在を確認
    if 'GOOGLE_FUNCTION_TARGET' not in os.environ and not GCP_CREDENTIALS_PATH:
        print("Warning: GCP_CREDENTIALS_PATH is not set for local execution. "
              "Falling back to Application Default Credentials (ADC).")

# モジュール読み込み時に検証を実行
validate_config()