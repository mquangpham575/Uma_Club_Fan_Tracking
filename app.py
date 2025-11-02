# app.py

import asyncio
import os
import time

from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

# Import hàm chính từ file main.py của bạn
# Đảm bảo tất cả các hàm con khác (như fetch_json, build_dataframe, export_to_gsheets...) 
# vẫn có thể truy cập được thông qua run_automatic_export trong main.py.
try:
    from main import run_automatic_export
except ImportError as e:
    print(f"Lỗi: Không thể import run_automatic_export từ main.py. Đảm bảo main.py tồn tại và hàm đã được định nghĩa. Chi tiết: {e}")
    # Thoát nếu không thể tìm thấy hàm
    exit(1)


# === Flask App and Scheduler Setup ===
app = Flask(__name__)

# Function wrapper để chạy logic async trong scheduler
def scheduled_job():
    """
    Hàm này được gọi bởi APScheduler, 
    nó tạo một event loop mới để chạy hàm async run_automatic_export.
    """
    print("--- Running scheduled job... ---")
    try:
        # Tạo event loop mới để chạy trong thread của BackgroundScheduler
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_automatic_export())
    except Exception as e:
        print(f"FATAL ERROR in scheduled_job: {e}")
    print("--- Scheduled job finished. ---")


# Initialize and start the scheduler
scheduler = BackgroundScheduler()

# Thêm tác vụ để chạy mỗi 12 giờ
# interval=12 tương đương 12 giờ
print("Starting scheduler...")
scheduler.add_job(scheduled_job, 'interval', hours=12, id='club_export_job') 

# Chạy tác vụ lần đầu tiên ngay khi khởi động
# Hoặc bạn có thể chọn bỏ dòng này nếu muốn đợi 12h đầu tiên
scheduler.add_job(scheduled_job, 'date', run_date=time.strftime('%Y-%m-%d %H:%M:%S'), id='club_export_job_initial')

scheduler.start()


@app.route('/')
def home():
    """
    Endpoint này là cần thiết để Render nhận biết ứng dụng vẫn đang chạy 
    (Health Check) và giữ tiến trình hoạt động.
    """
    return f"Club Export Scheduler is running and scheduled to run every 12 hours. Last started: {time.strftime('%Y-%m-%d %H:%M:%S')}"


if __name__ == '__main__':
    # Chạy ứng dụng Flask
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)