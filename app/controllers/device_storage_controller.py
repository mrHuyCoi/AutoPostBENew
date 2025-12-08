from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.database.database import get_db
from app.models.user import User
from app.dto.device_storage_dto import DeviceStorageCreate, DeviceStorageUpdate, DeviceStorageResponse, DeviceStorageWithDeviceResponse
from app.services.device_info_service import DeviceInfoService
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.dto.response import ResponseModel
from app.logging.decorators import with_log_context


router = APIRouter(prefix="/device-storages")

# Đặt endpoint /all lên trên để tránh bị bắt nhầm
@router.get("/all", response_model=ResponseModel[list[DeviceStorageWithDeviceResponse]])
@with_log_context(
    event_action="device_storage.all.list",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def get_all_device_storages(
    db: AsyncSession = Depends(get_db),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách tất cả cặp thiết bị-dung lượng, có phân trang và tìm kiếm.
    """
    try:
        result, total = await DeviceInfoService.get_all_device_storages(db, search, page, limit)
        return ResponseModel.success(
            data=result,
            message="Lấy danh sách thiết bị-dung lượng thành công",
            pagination={
                "page": page,
                "limit": limit,
                "total": total,
                "totalPages": (total + limit - 1) // limit
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{device_storage_id}", response_model=ResponseModel[DeviceStorageResponse])
@with_log_context(
    event_action="device_storage.get",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def get_device_storage_by_id(
    device_storage_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin dung lượng thiết bị theo ID.
    """
    try:
        storage = await DeviceInfoService.get_device_storage_by_id(db, device_storage_id)
        return ResponseModel.success(
            data=storage,
            message="Lấy thông tin dung lượng thiết bị thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.post("", response_model=DeviceStorageResponse, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="device_storage.create",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def create_device_storage(
    data: DeviceStorageCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Thêm một tùy chọn dung lượng cho thiết bị.
    Cho phép user thêm bộ nhớ cho thiết bị của mình.
    """
    try:
        device_storage = await DeviceInfoService.add_storage_to_device(db, data.device_info_id, data.capacity, current_user)
        return device_storage
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{device_storage_id}", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="device_storage.delete",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def delete_device_storage(
    device_storage_id: uuid.UUID,
    device_info_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa một tùy chọn dung lượng khỏi thiết bị.
    Cho phép user xóa bộ nhớ của thiết bị của họ.
    """
    try:
        success = await DeviceInfoService.remove_storage_from_device(db, device_info_id, device_storage_id, current_user)
        if success:
            return {"message": "Đã xóa dung lượng thành công"}
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn không có quyền xóa dung lượng này."
            )
    except Exception as e:
        # Giữ lại phần xử lý lỗi Foreign Key
        if "ForeignKeyViolationError" in str(e) or "violates foreign key constraint" in str(e):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Không thể xóa: Dung lượng này đang được một thiết bị của người dùng sử dụng."
            )
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/by-device/{device_info_id}", response_model=List[DeviceStorageResponse])
@with_log_context(
    event_action="device_storage.by_device.list",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def get_device_storages(
    device_info_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách dung lượng của một thiết bị.
    """
    try:
        device_storages = await DeviceInfoService.get_device_storages(db, device_info_id,current_user)
        return device_storages
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# Thêm endpoint mới để tương thích với frontend
@router.get("/{device_storage_id}", response_model=ResponseModel[DeviceStorageResponse])
@with_log_context(
    event_action="device_storage.get_response",
    event_category="device_storage",
    source_layer="controller",
    source_controller="device_storage_controller",
)
async def get_device_storage(
    device_storage_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin dung lượng theo ID.
    """
    try:
        device_storage = await DeviceInfoService.get_device_storage_by_id(db, device_storage_id)
        return ResponseModel.success(
            data=device_storage,
            message="Lấy thông tin dung lượng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
