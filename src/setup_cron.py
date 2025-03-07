#!/usr/bin/env python3
import os
import sys
from crontab import CronTab

def setup_cron_job():
    """Cronジョブを設定する"""
    try:
        # 現在のユーザーのcrontabを取得
        cron = CronTab(user=True)
        
        # 既存のジョブを確認し、同じコマンドがあれば削除
        for job in cron:
            if 'export_to_sheets.py' in str(job) or 'get_completed_tasks.py' in str(job):
                cron.remove(job)
                print("既存のCronジョブを削除しました。")
        
        # 現在の作業ディレクトリを取得
        current_dir = os.getcwd()
        
        # Asanaからデータを取得するジョブを作成（毎日午前8時30分に実行）
        job1 = cron.new(command=f'cd {current_dir} && python3 {current_dir}/src/get_completed_tasks.py >> {current_dir}/logs/asana_tasks.log 2>&1')
        job1.setall('30 8 * * *')  # 毎日午前8時30分に実行
        
        # BigQueryからスプレッドシートにデータを出力するジョブを作成（毎月1日の午前9時に実行）
        job2 = cron.new(command=f'cd {current_dir} && python3 {current_dir}/src/export_to_sheets.py >> {current_dir}/logs/sheets_export.log 2>&1')
        job2.setall('0 9 1 * *')  # 毎月1日の午前9時に実行
        
        # 変更を保存
        cron.write()
        
        print("Cronジョブを設定しました。")
        print("- 毎日午前8時30分にAsanaからデータを取得してBigQueryに保存")
        print("- 毎月1日の午前9時にBigQueryからデータを取得してスプレッドシートに出力")
        return True
    
    except Exception as e:
        print(f"Cronジョブの設定中にエラーが発生しました: {e}")
        return False

def main():
    """メイン処理"""
    # logsディレクトリが存在しない場合は作成
    logs_dir = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        print("logsディレクトリを作成しました。")
    
    # Cronジョブを設定
    success = setup_cron_job()
    
    if success:
        print("セットアップが完了しました。")
    else:
        print("セットアップに失敗しました。")
        sys.exit(1)

if __name__ == "__main__":
    main() 