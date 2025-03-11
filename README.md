# Asana Analytics Hub

Asanaから完了タスクのデータを取得し、BigQueryに保存、Google Sheetsにエクスポートするシステム

## 機能

- Asanaからタスクデータを取得し、BigQueryに保存
- タスクの実績時間を計算（見積時間 × 時間達成率）
- BigQueryから月ごとのプロジェクト別実績時間データを取得
- Google Sheetsに月ごとのデータを別々のシートとして出力
- Cronジョブによる定期的な自動実行

## 前提条件

- Python 3.6以上
- Asana API トークン
- Google Cloud Platform アカウント
- BigQuery データセット
- Google Sheets API アクセス権

## セットアップ

1. 必要なライブラリをインストール
```bash
pip install -r requirements.txt
```

2. 環境変数の設定
`.env.example`を`.env`としてコピーし、必要な情報を入力してください。

```
ASANA_PAT=your_personal_access_token
GOOGLE_APPLICATION_CREDENTIALS=path_to_your_service_account_json
GCP_PROJECT_ID=your_google_cloud_project_id
SPREADSHEET_ID=your_google_sheets_id
```

### 3. Google Sheetsの設定

1. Google Cloud Consoleで「Google Sheets API」を有効化
2. サービスアカウントを作成し、JSONキーファイルをダウンロード
3. スプレッドシートをサービスアカウントと共有

### 4. Cronジョブの設定

以下のコマンドを実行して、Cronジョブを設定します：

```bash
python src/setup_cron.py
```

これにより、以下のジョブが設定されます：
- 毎日朝8:30に新しいデータをAsanaから取得 (`get_completed_tasks.py`)
- 毎月1日朝9:00にGoogle Sheetsにデータをエクスポート (`export_to_sheets.py`)

**注意**: `export_to_sheets.py`は月ごとのデータを別々のシートに出力します。各シートは「YYYY年M月」という名前で作成されます。

## 使い方

1. Asanaからタスクを取得してBigQueryに保存
```bash
python src/get_completed_tasks.py
```

2. BigQueryからデータを取得してGoogle Sheetsにエクスポート
```bash
python src/export_to_sheets.py
```

3. Cronジョブの設定
```bash
python src/setup_cron.py
```

Cronジョブにより、以下の処理が自動的に実行されます：
- 毎日朝8:30に新しいデータをAsanaから取得 (`get_completed_tasks.py`)
- 毎月1日朝9:00にGoogle Sheetsにデータをエクスポート (`export_to_sheets.py`)

**注意**: スプレッドシートへのデータ出力は月ごとに別々のシートに分けて行われます。シート名は「YYYY年M月」の形式（例: 2024年3月）となります。

### ログの確認

ログファイルは以下の場所に保存されます：

- Asanaデータ取得のログ：`logs/asana_tasks.log`
- スプレッドシート出力のログ：`logs/sheets_export.log`

## ファイル構成

- `src/get_completed_tasks.py`: Asanaから完了タスクを取得してBigQueryに保存
- `src/export_to_sheets.py`: BigQueryからデータを取得してGoogle Sheetsにエクスポート
- `src/setup_cron.py`: Cronジョブを設定するスクリプト
- `src/setup_env.sh`: 環境変数を設定するスクリプト