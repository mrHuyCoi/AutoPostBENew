import subprocess
from app.celery_app import celery_app  # Đảm bảo celery_app được import để Celery hoạt động

"""
Celery Worker 2
----------------
File này dùng để khởi động Celery worker thứ 2 với hostname riêng,
xử lý tất cả các task từ queue 'default' với concurrency = 4.

Cách sử dụng:
    - python celery_worker_2.py
    - Hoặc trực tiếp:
        celery -A celery_worker_2.celery_app worker --loglevel=info --hostname=worker2@%h --queues=default --concurrency=4
"""

if __name__ == '__main__':
    # Sử dụng subprocess thay vì os.system để dễ debug và rõ ràng hơn
    subprocess.run([
        "celery",
        "-A", "celery_worker_2.celery_app",
        "worker",
        "--loglevel=info",
        "--hostname=worker2@%h",
        "--queues=default",
        "--concurrency=4"
    ])
