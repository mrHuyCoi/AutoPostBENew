import os
from app.celery_app import celery_app
from celery.schedules import crontab

"""
File này dùng để khởi động Celery beat scheduler.

Cách sử dụng:
    - Chạy beat scheduler: python celery_beat.py
    - Hoặc sử dụng lệnh Celery trực tiếp: celery -A celery_beat.celery_app beat --loglevel=info
"""

# Cấu hình các task định kỳ
celery_app.conf.beat_schedule = {
    'check-scheduled-posts-every-minute': {
        'task': 'publish_scheduled_posts',
        'schedule': 10.0,  # Chạy mỗi 10 giây
        'options': {'queue': 'default'},  # Đảm bảo task được đưa vào queue mặc định
    },
    'cleanup-old-media-daily': {
        'task': 'cleanup_old_media',
        'schedule': 86400.0,  # Chạy mỗi 24 giờ (86400 giây)
        'options': {'queue': 'default', 'loglevel': 'info'},
    },
    'sync-user-urls-daily-at-3am': {
        'task': 'sync_user_urls_daily',
        'schedule': crontab(hour=3, minute=0),  # Chạy lúc 03:00 hàng ngày
        'options': {'queue': 'default', 'loglevel': 'info'},
    },
    'sync-api-data-daily-at-3am': {
        'task': 'scheduled_sync_from_api_and_export',
        'schedule': crontab(hour=3, minute=0),  # Chạy lúc 03:00 hàng ngày
        'options': {'queue': 'default', 'loglevel': 'info'},
    },
}

if __name__ == '__main__':
    # Khởi động Celery beat scheduler với log level info; filter trong app.celery_app sẽ chỉ hiển thị các task được chọn
    os.system('celery -A celery_beat.celery_app beat --loglevel=info')