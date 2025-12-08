import os
import logging
from celery.signals import task_prerun, task_postrun, task_failure
from app.celery_app import celery_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('celery_worker')

"""
File này dùng để khởi động Celery worker chính, xử lý tất cả các loại tác vụ.

Cách sử dụng:
    - Chạy worker: python celery_worker.py
    - Hoặc sử dụng lệnh Celery trực tiếp: celery -A celery_worker.celery_app worker --loglevel=info
"""


@task_prerun.connect
def task_started_handler(task_id, task, args, kwargs, **kw):
    """Log khi task bắt đầu chạy"""
    logger.info(f"[TASK START] {task.name} | task_id={task_id}")


@task_postrun.connect
def task_finished_handler(task_id, task, args, kwargs, retval, state, **kw):
    """Log khi task hoàn thành"""
    logger.info(f"[TASK DONE] {task.name} | task_id={task_id} | state={state} | result={retval}")


@task_failure.connect
def task_failed_handler(task_id, exception, args, kwargs, traceback, einfo, **kw):
    """Log khi task thất bại"""
    logger.error(f"[TASK FAILED] task_id={task_id} | error={exception}")


if __name__ == '__main__':
    # Khởi động Celery worker để xử lý tất cả các tác vụ (cả đăng ngay và lên lịch) từ queue mặc định
    # -E: Enable task events để theo dõi
    os.system('celery -A celery_worker.celery_app worker --loglevel=info --concurrency=4 --queues=default -E')
