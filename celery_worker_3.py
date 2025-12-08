import os
from app.celery_app import celery_app

"""
File này dùng để khởi động Celery worker thứ 3.

Cách sử dụng:
    - Chạy worker: python celery_worker_3.py
    - Hoặc sử dụng lệnh Celery trực tiếp: celery -A celery_worker_3.celery_app worker --loglevel=info --hostname=worker3@%h --queues=high_priority
"""

if __name__ == '__main__':
    # Khởi động Celery worker với hostname riêng biệt và queue ưu tiên cao
    os.system('celery -A celery_worker_3.celery_app worker --loglevel=info --hostname=worker3@%h --queues=default') 