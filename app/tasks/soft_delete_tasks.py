"""
soft_delete_tasks.py
Task định kỳ xóa vĩnh viễn các bản ghi đã soft-delete sau khi hết hạn (purge_after <= now).
Chạy lúc 1:00 sáng theo múi giờ Asia/Ho_Chi_Minh (được cấu hình trong celery_app).
"""

import asyncio
import traceback
from typing import Dict

from app.celery_app import celery_app
from app.database.database import async_session
from app.utils.soft_delete import SoftDeleteMixin

# Các models cần áp dụng hard delete khi quá hạn
from app.models.brand import Brand
from app.models.product_component import ProductComponent
from app.models.service import Service
from app.models.user_device import UserDevice


def _get_loop() -> asyncio.AbstractEventLoop:
    """Đảm bảo luôn có event-loop cho worker Celery."""
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


@celery_app.task(name="purge_soft_deleted_records")
def purge_soft_deleted_records() -> Dict[str, int]:
    """Celery task: xóa vĩnh viễn các bản ghi đã soft-delete và hết hạn khôi phục.

    Trả về số lượng bản ghi đã xóa theo từng model.
    """
    loop = _get_loop()
    try:
        return loop.run_until_complete(_purge_soft_deleted_records())
    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}


async def _purge_soft_deleted_records() -> Dict[str, int]:
    results: Dict[str, int] = {}

    async with async_session() as db:
        # Xóa theo từng model
        brand_deleted = await SoftDeleteMixin.hard_delete_expired(db, Brand)
        service_deleted = await SoftDeleteMixin.hard_delete_expired(db, Service)
        product_component_deleted = await SoftDeleteMixin.hard_delete_expired(db, ProductComponent)
        user_device_deleted = await SoftDeleteMixin.hard_delete_expired(db, UserDevice)

        results = {
            "brands": brand_deleted or 0,
            "services": service_deleted or 0,
            "product_components": product_component_deleted or 0,
            "user_devices": user_device_deleted or 0,
            "total": (brand_deleted or 0)
                + (service_deleted or 0)
                + (product_component_deleted or 0)
                + (user_device_deleted or 0),
        }

    return results
