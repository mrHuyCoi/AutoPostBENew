import os
from app.celery_app import celery_app

"""
File này dùng để khởi động Flower - công cụ giám sát Celery.

Cách sử dụng:
    - Chạy Flower: python celery_flower.py
    - Hoặc sử dụng lệnh Celery trực tiếp: celery -A celery_flower.celery_app flower --port=5555
    - Sau đó truy cập http://localhost:5555 để xem giao diện giám sát
"""

if __name__ == '__main__':
    # Khởi động Flower
    os.system('celery -A celery_flower.celery_app flower --port=5555')