from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.dto.response import ResponseModel
from app.dto.user_device_dto import UserDeviceDetailRead
from app.repositories.user_device_from_url_repository import UserDeviceFromUrlRepository
from app.logging.decorators import with_log_context

router = APIRouter(prefix="/user-devices-from-url", tags=["User Devices From URL"])


@router.get("/my-devices", response_model=ResponseModel[List[UserDeviceDetailRead]])
@with_log_context(
    event_action="user_device_from_url.my_devices.list",
    event_category="user_device_from_url",
    source_layer="controller",
    source_controller="user_device_from_url_controller",
)
async def get_my_devices_from_url(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    sort_by: Optional[str] = Query(None, description="Tên trường để sắp xếp"),
    sort_order: Optional[str] = Query('asc', description="Thứ tự sắp xếp ('asc' hoặc 'desc')"),
    skip: int = Query(0, ge=0, description="Số lượng bản ghi bỏ qua"),
    limit: int = Query(10, ge=1, le=100, description="Số lượng bản ghi tối đa trả về"),
    search: Optional[str] = Query(None, description="Tìm kiếm theo model hoặc mã sản phẩm"),
    brand: Optional[str] = Query(None, description="Lọc theo tên hãng sản xuất"),
    inventory_min: Optional[int] = Query(None, description="Tồn kho tối thiểu"),
    inventory_max: Optional[int] = Query(None, description="Tồn kho tối đa"),
    price_min: Optional[float] = Query(None, description="Giá tối thiểu"),
    price_max: Optional[float] = Query(None, description="Giá tối đa"),
    storage_capacity: Optional[int] = Query(None, description="Lọc theo dung lượng bộ nhớ (GB)"),
):
    """
    Lấy danh sách thiết bị được đồng bộ từ URL (bảng user_devices_from_url) của người dùng hiện tại
    với bộ lọc, chi tiết và phân trang.
    """
    filters = {
        "search": search,
        "brand": brand,
        "inventory_min": inventory_min,
        "inventory_max": inventory_max,
        "price_min": price_min,
        "price_max": price_max,
        "storage_capacity": storage_capacity,
    }
    active_filters = {k: v for k, v in filters.items() if v is not None}

    try:
        total = await UserDeviceFromUrlRepository.count_by_user_id(db, current_user.id, filters=active_filters)
        devices = await UserDeviceFromUrlRepository.get_by_user_id_with_details(
            db,
            current_user.id,
            skip=skip,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            filters=active_filters,
        )
        total_pages = (total + limit - 1) // limit if limit > 0 else 0
        return ResponseModel.success(
            data=devices,
            message="Lấy danh sách thiết bị từ URL thành công",
            total=total,
            totalPages=total_pages,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
