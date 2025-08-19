# Asana Analytics Hub

Asanaから完了タスクのデータを取得し、BigQueryに保存、集計結果をGoogle Sheetsにエクスポートするシステムです。

## 主な機能

-   **データ取得**: Asanaから全てのプロジェクトの完了タスクを定期的に取得し、BigQueryに保存します。
-   **レポート生成**: BigQuery上のデータを月別に集計し、以下の3つのレポートを生成します。
    -   プロジェクト別実績時間
    -   担当者別実績時間
    -   プロジェクト・担当者別実績時間
-   **自動化**: Google Cloud FunctionsとCloud Schedulerにより、データ取得とレポート生成を完全に自動化します。

## アーキテクチャ

1.  **Cloud Function (fetch-asana-tasks)**: Cloud Schedulerに毎日トリガーされ、Asana APIから完了タスクを取得し、BigQueryの`completed_tasks`テーブルに追記します。
2.  **Cloud Function (export-to-sheets)**: Cloud Schedulerにより、BigQueryの`completed_tasks`の集計結果をGoogle Sheetsに書き込みます（頻度は運用ポリシーに合わせて変更可）。
3.  **Cloud Function (snapshot-open-tasks)**: 毎朝（JST）未完了タスクをスナップショットとして `open_tasks_snapshot` に保存します。

 <!-- 図は後で作成・挿入するとより分かりやすいです -->

## セットアップ手順

### 1. 前提条件

-   Python 3.9以上
-   Google Cloud Platform (GCP) アカウント
    -   Cloud Functions, Cloud Scheduler, Cloud Build, BigQuery, Google Sheets APIが有効になっていること。
-   AsanaアカウントとPersonal Access Token

### 2. リポジトリのクローンと仮想環境の作成

```bash
git clone <your-repository-url>
cd asana-analytics-hub
python3 -m venv venv
source venv/bin/activate
```

### 3. 依存ライブラリのインストール

```bash
pip install -r requirements.txt
```

### 4. 環境変数の設定

`.env.example`をコピーして`.env`ファイルを作成し、ご自身の環境に合わせて値を設定します。

```bash
cp .env.example .env
```

**`.env`ファイルの中身:**

```
# Asana設定
ASANA_ACCESS_TOKEN="<Your Asana Personal Access Token>"
ASANA_WORKSPACE_ID="<Your Asana Workspace ID>"

# GCP設定
GCP_PROJECT_ID="<Your GCP Project ID>"
GCP_CREDENTIALS_PATH="<Path to your service-account-key.json>" # ローカル実行時のみ

# Google Sheets設定
SPREADSHEET_ID="<Your Google Spreadsheet ID>"

# Slack設定（任意：未設定なら通知はスキップ）
SLACK_BOT_TOKEN="xoxb-..."            # Secret Manager 推奨
SLACK_CHANNEL_ID="C0123456789"        # 投稿先チャンネルID
```

**重要**: `.env`ファイルは`.gitignore`で管理対象外になっています。**絶対にGitにコミットしないでください。**

### 5. GCPサービスアカウントの設定

1.  GCPコンソールでサービスアカウントを作成し、キー（JSON形式）をダウンロードします。
2.  ダウンロードしたキーファイルを、プロジェクト内の安全な場所（例: `credentials/key.json`）に配置し、`.env`の`GCP_CREDENTIALS_PATH`にそのパスを記述します。
3.  サービスアカウントに以下のロールを付与します:
    -   `BigQuery データ編集者`
    -   `BigQuery ジョブユーザー`
    -   `Cloud Run Invoker`（Scheduler→FunctionsのOIDC実行に必要）
4.  出力先のGoogleスプレッドシートの「共有」設定で、このサービスアカウントのメールアドレスを追加し、「編集者」の権限を与えます。

## ローカルでの手動実行

ローカルで各機能をテスト実行できます。

1.  **Asanaからデータを取得してBigQueryに保存:**
    ```bash
    # `asana_reporter`ディレクトリをPythonパスに追加して実行
    PYTHONPATH=. python3 asana_reporter/main.py fetch
    ```

2.  **BigQueryからデータを取得してGoogle Sheetsにエクスポート:**
    ```bash
    PYTHONPATH=. python3 asana_reporter/main.py export
    ```

## GCPへのデプロイ

### 1. 環境変数ファイル `env.yaml` の作成

デプロイ用に、以下の内容で`env.yaml`ファイルを作成します。**このファイルには機密情報を含めず、Gitで管理しても安全な情報のみを記述します。**

```yaml
GCP_PROJECT_ID: "<Your GCP Project ID>"
ASANA_WORKSPACE_ID: "<Your Asana Workspace ID>"
SPREADSHEET_ID: "<Your Google Spreadsheet ID>"
```

### 2. Cloud Functionsのデプロイ

**ASANA_ACCESS_TOKENやSLACK_BOT_TOKENは、デプロイ時に`--set-secrets`フラグでSecret Managerから読み込みます。HTTPはOIDCで保護され、Cloud Schedulerのみが呼び出せます。**

**事前準備: Secret ManagerにAsanaのトークンを保存**
```bash
gcloud secrets create asana-access-token --project=<Your GCP Project ID>
gcloud secrets versions add asana-access-token --data-from-file=- --project=<Your GCP Project ID>
# 上記コマンド実行後、ターミナルにトークンをペーストしてCtrl+Dで完了
```

**データ取得用Function (`fetch-asana-tasks`):**

```bash
gcloud functions deploy fetch-asana-tasks \
  --project=<Your GCP Project ID> \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=fetch_asana_tasks_to_bq \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --gen2
```

**スプレッドシート出力用Function (`export-to-sheets`):**

```bash
gcloud functions deploy export-to-sheets \
  --project=<Your GCP Project ID> \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=. \
  --entry-point=export_reports_to_sheets \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file=env.yaml \
  --set-secrets=ASANA_ACCESS_TOKEN=asana-access-token:latest,SLACK_BOT_TOKEN=slack-bot-token:latest \
  --timeout=540s \
  --gen2
```

### 3. Cloud Schedulerの設定

**データ取得用ジョブ（毎日朝5:00）:**

```bash
gcloud scheduler jobs create http fetch-tasks-daily \
  --project=<Your GCP Project ID> \
  --location=asia-northeast1 \
  --schedule="0 5 * * *" \
  --time-zone="Asia/Tokyo" \
  --uri="<YOUR_FETCH_TASKS_FUNCTION_URL>" \
  --http-method=POST
```

**スプレッドシート出力用ジョブ（毎月1日朝6:00）:**

```bash
gcloud scheduler jobs create http export-sheets-monthly \
  --project=<Your GCP Project ID> \
  --location=asia-northeast1 \
  --schedule="0 6 1 * *" \
  --time-zone="Asia/Tokyo" \
  --uri="<YOUR_EXPORT_TO_SHEETS_FUNCTION_URL>" \
  --http-method=POST
```
**注意**: `<YOUR_..._FUNCTION_URL>`は、各Functionをデプロイした際に表示されるトリガーURLに置き換えてください。