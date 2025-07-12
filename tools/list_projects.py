import os
import requests
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

def list_projects():
    """ワークスペース内のプロジェクト一覧を取得"""
    # アクセストークンの取得
    access_token = os.getenv('ASANA_ACCESS_TOKEN')
    workspace_id = os.getenv('ASANA_WORKSPACE_ID')
    
    # APIリクエストの設定
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    # プロジェクト一覧の取得
    url = f'https://app.asana.com/api/1.0/workspaces/{workspace_id}/projects'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        projects = response.json()['data']
        
        print("\nプロジェクト一覧:")
        print("-" * 50)
        for project in projects:
            print(f"プロジェクト名: {project['name']}")
            print(f"プロジェクトID: {project['gid']}")
            if 'created_at' in project:
                print(f"作成日: {project['created_at']}")
            if 'modified_at' in project:
                print(f"最終更新: {project['modified_at']}")
            if 'archived' in project:
                print(f"アーカイブ状態: {'アーカイブ済み' if project['archived'] else 'アクティブ'}")
            print("-" * 50)
    else:
        print(f"エラーが発生しました: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    list_projects() 