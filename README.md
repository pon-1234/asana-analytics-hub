# Asana Analytics Hub

Asanaのタスクデータを取得し、BigQueryに保存して分析するためのツールです。また、分析結果をGoogle Sheetsに出力する機能も備えています。

## 機能

- Asanaからタスクデータを取得し、BigQueryに保存
- タスクの実績時間を計算（見積時間 × 時間達成率）
- BigQueryからプロジェクト別の実績時間データを取得
- Google Sheetsにデータを出力
- Cronジョブによる定期的な自動実行

## 前提条件

- Python 3.6以上
- Asana API トークン
- Google Cloud Platform アカウント
- BigQuery データセット
- Google Sheets API アクセス権

## セットアップ

### 1. 必要なライブラリのインストール

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib google-cloud-bigquery python-crontab
```

### 2. 環境変数の設定

`src/setup_env.sh`ファイルを編集して、必要な環境変数を設定します：

```bash
# 環境変数を設定
export ASANA_ACCESS_TOKEN="your_asana_token"
export ASANA_WORKSPACE_ID="your_workspace_id"
export GCP_PROJECT_ID="your_gcp_project_id"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
```

設定後、以下のコマンドを実行して環境変数を読み込みます：

```bash
source src/setup_env.sh
```

### 3. Google Sheetsの設定

1. Google Cloud Consoleで「Google Sheets API」を有効化
2. サービスアカウントを作成し、JSONキーファイルをダウンロード
3. スプレッドシートをサービスアカウントと共有

### 4. Cronジョブの設定

以下のコマンドを実行して、Cronジョブを設定します：

```bash
python3 src/setup_cron.py
```

これにより、以下のジョブが設定されます：
- 毎日午前8時30分：Asanaからデータを取得してBigQueryに保存
- 毎日午前9時：BigQueryからデータを取得してGoogle Sheetsに出力

## 使い方

### 手動実行

```bash
# Asanaからデータを取得してBigQueryに保存
python3 src/get_completed_tasks.py

# BigQueryからデータを取得してGoogle Sheetsに出力
python3 src/export_to_sheets.py
```

### ログの確認

ログファイルは以下の場所に保存されます：

- Asanaデータ取得のログ：`logs/asana_tasks.log`
- スプレッドシート出力のログ：`logs/sheets_export.log`

## ファイル構成

- `src/get_completed_tasks.py`: Asanaからタスクデータを取得し、BigQueryに保存するスクリプト
- `src/export_to_sheets.py`: BigQueryからデータを取得し、Google Sheetsに出力するスクリプト
- `src/setup_cron.py`: Cronジョブを設定するスクリプト
- `src/setup_env.sh`: 環境変数を設定するスクリプト
- `README.md`: プロジェクトの説明
- `README_SHEETS_EXPORT.md`: Google Sheets出力機能の詳細な説明

## データ構造

### BigQueryテーブル

- tasks: Asanaのタスク情報
- projects: プロジェクト情報
- users: ユーザー情報
- tags: タグ情報 