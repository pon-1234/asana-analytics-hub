import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.parse
import json
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

CLIENT_ID = "1208371053404112"
CLIENT_SECRET = "e8743ee082d2057584949fd852a730fe"
REDIRECT_URI = "http://localhost:8000/callback"

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # URLから認証コードを取得
        query_components = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if 'code' in query_components:
            code = query_components['code'][0]
            
            # アクセストークンを取得
            import requests
            token_url = "https://app.asana.com/-/oauth_token"
            data = {
                "grant_type": "authorization_code",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
                "code": code
            }
            
            response = requests.post(token_url, data=data)
            token_data = response.json()
            
            if 'access_token' in token_data:
                # .envファイルにアクセストークンを保存
                with open('.env', 'a') as f:
                    f.write(f"\nASANA_ACCESS_TOKEN={token_data['access_token']}")
                
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write("認証が完了しました。このウィンドウを閉じてください。".encode('utf-8'))
            else:
                self.send_response(400)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write("認証に失敗しました。".encode('utf-8'))
        else:
            self.send_response(400)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write("認証コードが見つかりません。".encode('utf-8'))

def main():
    # 認証URLの生成
    auth_url = f"https://app.asana.com/-/oauth_authorize?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}&response_type=code"
    
    # ローカルサーバーの起動
    server = HTTPServer(('localhost', 8000), OAuthCallbackHandler)
    print("認証サーバーを起動しました。ブラウザが開きます...")
    
    # ブラウザで認証URLを開く
    webbrowser.open(auth_url)
    
    # コールバックを待機
    server.handle_request()
    server.server_close()
    print("認証が完了しました。")

if __name__ == "__main__":
    main() 