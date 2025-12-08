import os
from app.celery_app import celery_app

"""
File này dùng để khởi động Celery worker chính, xử lý tất cả các loại tác vụ.

Cách sử dụng:
    - Chạy worker: python celery_worker.py
    - Hoặc dùng lệnh Celery: celery -A celery_worker.celery_app worker --loglevel=info
"""

if __name__ == '__main__':
    # Windows: dùng pool=solo để tránh lỗi PermissionError/handle invalid
    os.system(
        'celery -A celery_worker.celery_app worker '
        '--loglevel=info --pool=solo --concurrency=1 --queues=default'
    )
