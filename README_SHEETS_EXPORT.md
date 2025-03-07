# Asana実績時間のGoogle Sheets出力

このツールは、AsanaのタスクデータをBigQueryから取得し、Google Sheetsに出力するためのものです。

## 前提条件

- Python 3.6以上
- Google Cloud Platformのアカウント
- Google Sheets APIが有効化されたプロジェクト
- サービスアカウントとJSONキーファイル
- BigQueryにAsanaのタスクデータが保存されていること

## セットアップ手順

### 1. 必要なライブラリのインストール

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib python-crontab
```

### 2. Google Cloud認証情報の設定

1. Google Cloud Consoleにアクセスし、プロジェクトを選択または新規作成します。
2. 「APIとサービス」→「ライブラリ」から「Google Sheets API」を検索して有効化します。
3. 「APIとサービス」→「認証情報」→「認証情報を作成」→「サービスアカウント」を選択します。
4. サービスアカウントの名前と説明を入力し、「作成」をクリックします。
5. 役割として「編集者」を選択し、「続行」をクリックします。
6. 「完了」をクリックしてサービスアカウントを作成します。
7. 作成したサービスアカウントをクリックし、「キー」タブを選択します。
8. 「鍵を追加」→「新しい鍵を作成」→「JSON」を選択し、「作成」をクリックします。
9. JSONキーファイルがダウンロードされます。このファイルを安全な場所に保存します。

### 3. スプレッドシートの共有設定

1. 対象のスプレッドシート（https://docs.google.com/spreadsheets/d/1JpL-_kDN0X2GZYBnvVRqCuLUmHFKBYnTAbIXAuqilXQ/edit）を開きます。
2. 右上の「共有」ボタンをクリックします。
3. サービスアカウントのメールアドレス（例：service-account-name@project-id.iam.gserviceaccount.com）を入力します。
4. 権限を「編集者」に設定し、「送信」をクリックします。

### 4. 環境変数の設定

`src/setup_env.sh`ファイルを編集して、実際のJSONキーファイルのパスとプロジェクトIDを設定します：

```bash
# JSONキーファイルのパスを設定
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"

# GCPプロジェクトIDを設定
export GCP_PROJECT_ID="your-project-id"
```

設定後、以下のコマンドを実行して環境変数を読み込みます：

```bash
source src/setup_env.sh
```

### 5. Cronジョブの設定

以下のコマンドを実行して、Cronジョブを設定します：

```bash
python3 src/setup_cron.py
```

これにより、毎日午前9時にデータがスプレッドシートに出力されるようになります。

## 手動実行

以下のコマンドを実行して、手動でデータをスプレッドシートに出力することもできます：

```bash
python3 src/export_to_sheets.py
```

## トラブルシューティング

### エラーログの確認

エラーが発生した場合は、`logs/sheets_export.log`ファイルを確認してください。

### 認証エラー

認証エラーが発生した場合は、以下を確認してください：

1. 環境変数`GOOGLE_APPLICATION_CREDENTIALS`が正しく設定されているか
2. サービスアカウントがスプレッドシートに対して編集権限を持っているか
3. Google Sheets APIが有効化されているか

### BigQueryエラー

BigQueryからのデータ取得でエラーが発生した場合は、以下を確認してください：

1. 環境変数`GCP_PROJECT_ID`が正しく設定されているか
2. サービスアカウントがBigQueryに対してアクセス権限を持っているか
3. BigQueryのデータセットとテーブルが存在するか 