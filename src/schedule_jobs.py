import schedule
import time
from datetime import datetime
from fetch_asana_data import main as fetch_data
from generate_monthly_report import main as generate_report

def fetch_data_job():
    print("データ取得ジョブを開始します...")
    try:
        fetch_data()
        print("データ取得ジョブが正常に完了しました")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

def generate_report_job():
    print("月次レポート生成ジョブを開始します...")
    try:
        generate_report()
        print("月次レポート生成ジョブが正常に完了しました")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

def main():
    # 毎日午前0時にデータ取得を実行
    schedule.every().day.at("00:00").do(fetch_data_job)
    
    # 毎月1日の午前1時にレポート生成を実行
    schedule.every().month.at("01:00").do(generate_report_job)
    
    # 初回実行
    fetch_data_job()
    
    # 現在の日付が1日の場合、レポートも生成
    if datetime.now().day == 1:
        generate_report_job()
    
    # スケジュール実行
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main() 