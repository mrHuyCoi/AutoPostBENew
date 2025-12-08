from celery import Celery
from app.configs.settings import settings
import os
from celery.schedules import crontab
import logging
from celery.signals import after_setup_logger, after_setup_task_logger

# Cấu hình Celery
celery_app = Celery(
    'dangbaitudong',
    broker=settings.CELERY_BROKER_URL,  # Sử dụng Redis làm message broker
    backend=settings.CELERY_RESULT_BACKEND,  # Sử dụng Redis làm result backend
    include=[
        'app.tasks.content_publisher_tasks',
        'app.tasks.sync_tasks',
        'app.tasks.soft_delete_tasks',
        'app.tasks.messenger_tasks',
        'app.tasks.chatbot_tasks',
        'app.tasks.zalo_oa_tasks',
        'app.tasks.elasticsearch_tasks',
        'app.tasks.document_tasks',
        'app.tasks.faq_tasks',
    ]  # Import các module chứa task
)

# Cấu hình tùy chọn cho Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh',
    enable_utc=False,
    task_track_started=True,
    worker_hijack_root_logger=False,
    task_time_limit=1800,  # 30 phút timeout cho mỗi task
    broker_connection_retry_on_startup=True,
    # Cấu hình để tất cả các task đều được xử lý bởi cùng một worker
    task_default_queue='default',
    task_routes=None,  # Đảm bảo không có routing đặc biệt
    
    # === RELIABILITY CONFIG ===
    # Task result expiration (7 ngày)
    result_expires=604800,
    # Chỉ ack sau khi task hoàn thành (tránh mất task nếu worker crash)
    task_acks_late=True,
    # Reject task nếu worker bị kill (task sẽ được retry)
    task_reject_on_worker_lost=True,
    # Prefetch multiplier (số task prefetch per worker)
    worker_prefetch_multiplier=1,
    # Rate limits cho các task ES (tránh overwhelm Elasticsearch)
    task_annotations={
        'es_upsert_product': {'rate_limit': '20/s'},
        'es_upsert_service': {'rate_limit': '20/s'},
        'es_upsert_accessory': {'rate_limit': '20/s'},
        'es_bulk_upsert_products': {'rate_limit': '5/s', 'time_limit': 1200},
        'es_bulk_upsert_services': {'rate_limit': '5/s', 'time_limit': 1200},
        'es_bulk_upsert_accessories': {'rate_limit': '5/s', 'time_limit': 1200},
        'doc_upload_file': {'time_limit': 120},
        'doc_trigger_reindex': {'time_limit': 600},
        'faq_import': {'time_limit': 120},
    },
)

# Tự động tìm và đăng ký các task từ các module được chỉ định
celery_app.autodiscover_tasks(['app.tasks'])

ALLOWED_BEAT_ENTRIES = {
    'sync-api-data-daily-at-3am',
    'sync-user-urls-daily-at-3am',
    'purge-soft-deleted-records-daily-at-1am',
}

ALLOWED_TASK_NAMES = {
    'scheduled_sync_from_api_and_export',
    'sync_user_urls_daily',
    'purge_soft_deleted_records',
    # ES tasks
    'es_bulk_upsert_products',
    'es_bulk_upsert_services',
    'es_bulk_upsert_accessories',
    'es_upsert_product',
    'es_upsert_service',
    'es_upsert_accessory',
    'es_delete_product',
    'es_delete_service',
    'es_delete_accessory',
    'es_bulk_delete_products',
    'es_bulk_delete_services',
    'es_bulk_delete_accessories',
    'es_delete_all_products',
    'es_delete_all_services',
    'es_delete_all_accessories',
}

class SelectedTasksFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(getattr(record, 'msg', ''))
        # Always allow explicit FB comment debug logs
        if "[FB_COMMENT]" in msg:
            return True
        if any(name in msg for name in ALLOWED_BEAT_ENTRIES):
            return True
        task_name = getattr(record, 'task_name', None)
        if task_name and task_name in ALLOWED_TASK_NAMES:
            return True
        if any(name in msg for name in ALLOWED_TASK_NAMES):
            return True
        return False

def _apply_selected_tasks_filter(logger):
    flt = SelectedTasksFilter()
    try:
        logger.addFilter(flt)
    except Exception:
        pass
    for h in getattr(logger, 'handlers', []) or []:
        try:
            h.addFilter(flt)
        except Exception:
            pass

@after_setup_logger.connect
def _setup_celery_logger(logger, *args, **kwargs):
    _apply_selected_tasks_filter(logger)

@after_setup_task_logger.connect
def _setup_celery_task_logger(logger, *args, **kwargs):
    _apply_selected_tasks_filter(logger)

# Cho phép gọi task đồng bộ khi chạy trong chế độ debug/development
celery_app.conf.task_always_eager = settings.CELERY_TASK_ALWAYS_EAGER
# settings.CELERY_TASK_ALWAYS_EAGER

celery_app.conf.beat_schedule = {
    'check-scheduled-posts-every-minute': {
        'task': 'publish_scheduled_posts',
        'schedule': 10.0,
        'options': {'queue': 'default'},
    },
    'cleanup-old-media-daily': {
        'task': 'cleanup_old_media',
        'schedule': 86400.0,  # Chạy mỗi 24 giờ (86400 giây)
        'options': {'queue': 'default'},
    },
    'sync-api-data-daily-at-3am': {
        'task': 'scheduled_sync_from_api_and_export',
        'schedule': crontab(hour=3, minute=0),
        'options': {'queue': 'default'},
    },
    'sync-user-urls-daily-at-3am': {
        'task': 'sync_user_urls_daily',
        'schedule': crontab(hour=3, minute=0),
        'options': {'queue': 'default'},
    },
    'purge-soft-deleted-records-daily-at-1am': {
        'task': 'purge_soft_deleted_records',
        'schedule': crontab(hour=1, minute=0),
        'options': {'queue': 'default'},
    },
}
# celery -A app.celery_app beat --loglevel=info
