#!/bin/bash

# 環境変数の設定
echo "環境変数を設定しています..."

# Google Cloud認証情報のパス
# 実際のJSONキーファイルのパス
export GOOGLE_APPLICATION_CREDENTIALS="/Users/pon/dev/asana-analytics-hub/asana-analytics-hub-159b5c8ab38f.json"

# GCPプロジェクトID
export GCP_PROJECT_ID="asana-analytics-hub"

# 環境変数を.envファイルに保存
echo "GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS" > .env
echo "GCP_PROJECT_ID=$GCP_PROJECT_ID" >> .env

echo "環境変数を設定しました。"
echo "注意: setup_env.shファイルを編集して、実際のJSONキーファイルのパスとプロジェクトIDを設定してください。" 