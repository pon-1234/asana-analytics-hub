# Asana Analytics Hub

Asanaから完了タスクのデータを取得し、BigQueryに保存、Google Sheetsにエクスポートするシステム

## 機能

- Asanaからタスクデータを取得し、BigQueryに保存
- タスクの実績時間を計算（見積時間 × 時間達成率）
- BigQueryから月ごとのプロジェクト別実績時間データを取得
- Google Sheetsに月ごとのデータを別々のシートとして出力
- Google Cloud FunctionsとCloud Schedulerによる定期的な自動実行

## 前提条件

- Python 3.6以上
- Asana API トークン
- Google Cloud Platform アカウント
  - **Cloud Functions, Cloud Scheduler, Cloud Build APIが有効になっていること**
- BigQuery データセット
- Google Sheets API アクセス権

## セットアップ

1. 必要なライブラリをインストール
```bash
pip install -r requirements.txt
```

2. 環境変数の設定
ローカルでの手動実行、またはGCPへのデプロイのために、環境変数を設定します。

### GCPデプロイ用 (`env.yaml`)
GCPへデプロイする際は、プロジェクトのルートに`env.yaml`というファイル名で、以下の内容を作成します。`ASANA_ACCESS_TOKEN`には実際のトークンを設定してください。
```yaml
ASANA_WORKSPACE_ID: "1206940156947514"
GCP_PROJECT_ID: "asana-analytics-hub"
SPREADSHEET_ID: "1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ"
ASANA_ACCESS_TOKEN: "your_personal_access_token"
```

### ローカル実行用 (`.env`)
ローカル環境で手動実行する場合は、プロジェクトのルートに`.env`ファイルを作成し、同様の内容を`KEY=VALUE`形式で記述します。

### 3. Google Sheetsの設定

1. Google Cloud Consoleで「Google Sheets API」を有効化
2. サービスアカウントを作成し、JSONキーファイルをダウンロード
3. スプレッドシートをサービスアカウントと共有
4. **GCPで自動実行する場合**: サービスアカウントに「Cloud Functions起動元」と「Cloud Schedulerサービスエージェント」のロールを付与します。

## GCPでのデプロイと自動実行

このシステムは、Google Cloud Functionsとしてデプロイし、Cloud Schedulerによって定期的に実行されます。

### 1. Cloud Functionsのデプロイ

以下のコマンドを実行して、2つのCloud Functionをデプロイします。

**データ取得用Function (`get-completed-tasks`):**
```bash
gcloud functions deploy get-completed-tasks \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=src/ \
  --entry-point=gcf_entrypoint \
  --trigger-http \
  --env-vars-file=env.yaml \
  --timeout=540s
```

**スプレッドシート出力用Function (`export-to-sheets`):**
```bash
gcloud functions deploy export-to-sheets \
  --project=asana-analytics-hub \
  --region=asia-northeast1 \
  --runtime=python311 \
  --source=src/ \
  --entry-point=gcf_entrypoint \
  --trigger-http \
  --env-vars-file=env.yaml \
  --timeout=540s
```

### 2. Cloud Schedulerの設定

デプロイしたCloud Functionsを定期的に呼び出すためのジョブを設定します。

**データ取得用ジョブ（毎日朝8:30）:**
```bash
gcloud scheduler jobs create http get-tasks-daily \
  --project=asana-analytics-hub \
  --location=asia-northeast1 \
  --schedule="30 8 * * *" \
  --uri="YOUR_GET_COMPLETED_TASKS_FUNCTION_URL" \
  --http-method=POST
```

**スプレッドシート出力用ジョブ（毎月1日朝9:00）:**
```bash
gcloud scheduler jobs create http export-sheets-monthly \
  --project=asana-analytics-hub \
  --location=asia-northeast1 \
  --schedule="0 9 1 * *" \
  --uri="YOUR_EXPORT_TO_SHEETS_FUNCTION_URL" \
  --http-method=POST
```
**注意:** `YOUR_..._FUNCTION_URL`の部分は、各Cloud Functionをデプロイした際に表示されるトリガーURLに置き換えてください。

## 使い方 (ローカルでの手動実行)

1. Asanaからタスクを取得してBigQueryに保存
```bash
python src/get_completed_tasks.py
```

2. BigQueryからデータを取得してGoogle Sheetsにエクスポート
```bash
python src/export_to_sheets.py
```

## ログの確認

- **Cloud Functionsのログ**: GCPコンソールのCloud Loggingページで各関数のログを確認できます。
- **ローカル実行時のログ**:
  - Asanaデータ取得のログ：`logs/asana_tasks.log`
  - スプレッドシート出力のログ：`logs/sheets_export.log`

## ファイル構成

- `src/get_completed_tasks.py`: Asanaから完了タスクを取得してBigQueryに保存
- `src/export_to_sheets.py`: BigQueryからデータを取得してGoogle Sheetsにエクスポート
- `env.yaml`: GCPデプロイ用の環境変数設定ファイル