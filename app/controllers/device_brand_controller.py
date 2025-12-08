from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.database.database import get_db
from app.dto.device_brand_dto import DeviceBrandCreate, DeviceBrandUpdate, DeviceBrandRead
from app.services.device_brand_service import DeviceBrandService
from app.dto.response import ResponseModel
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.logging.decorators import with_log_context

router = APIRouter(prefix="/device-brands", tags=["Device Brands"])

@router.post("", response_model=ResponseModel[DeviceBrandRead])
@with_log_context(
    event_action="device_brand.create",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def create_device_brand(
    data: DeviceBrandCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # For admin users, set user_id to None (null) so it's available to all users
        # For regular users, set user_id to their own ID
        if current_user.role == 'admin' or current_user.is_superuser:
            data.user_id = None
        else:
            data.user_id = current_user.id
        device_brand = await DeviceBrandService.create_device_brand(db, data)
        return ResponseModel.success(data=DeviceBrandRead.model_validate(device_brand), message="Tạo hãng điện thoại thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/distinct", response_model=ResponseModel[List[DeviceBrandRead]])
@with_log_context(
    event_action="device_brand.distinct.list",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def get_distinct_device_brands(
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        user_id_to_filter = None
        if not (current_user.role == 'admin' or current_user.is_superuser):
            user_id_to_filter = current_user.id

        distinct_brands = await DeviceBrandService.get_distinct_device_brands(db, user_id_to_filter, search)
        brands_read = [DeviceBrandRead.model_validate(b) for b in distinct_brands]
        return ResponseModel.success(data=brands_read, message="Lấy danh sách hãng điện thoại duy nhất thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/{device_brand_id}", response_model=ResponseModel[DeviceBrandRead])
@with_log_context(
    event_action="device_brand.get",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def get_device_brand(
    device_brand_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        device_brand = await DeviceBrandService.get_device_brand(db, device_brand_id)
        return ResponseModel.success(data=DeviceBrandRead.model_validate(device_brand), message="Lấy thông tin hãng điện thoại thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("", response_model=ResponseModel[List[DeviceBrandRead]])
@with_log_context(
    event_action="device_brand.list",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def get_all_device_brands(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # For admin users, show all brands (no user filter)
        # For regular users, the repository will show both their own brands and admin-created brands
        if current_user.role == 'admin' or current_user.is_superuser:
            device_brands = await DeviceBrandService.get_all_device_brands(db, None, skip, limit, search)
        else:
            device_brands = await DeviceBrandService.get_all_device_brands(db, current_user.id, skip, limit, search)
        brands_read = [DeviceBrandRead.model_validate(b) for b in device_brands]
        return ResponseModel.success(data=brands_read, message="Lấy danh sách hãng điện thoại thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.put("/{device_brand_id}", response_model=ResponseModel[DeviceBrandRead])
@with_log_context(
    event_action="device_brand.update",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def update_device_brand(
    device_brand_id: uuid.UUID,
    data: DeviceBrandUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        device_brand = await DeviceBrandService.update_device_brand(db, device_brand_id, data)
        return ResponseModel.success(data=DeviceBrandRead.model_validate(device_brand), message="Cập nhật hãng điện thoại thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{device_brand_id}")
@with_log_context(
    event_action="device_brand.delete",
    event_category="device_brand",
    source_layer="controller",
    source_controller="device_brand_controller",
)
async def delete_device_brand(
    device_brand_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # For admin users, allow deletion of any brand
        # For regular users, only allow deletion of their own brands
        if current_user.role == 'admin' or current_user.is_superuser:
            result = await DeviceBrandService.delete_device_brand(db, device_brand_id, None)
        else:
            result = await DeviceBrandService.delete_device_brand(db, device_brand_id, current_user.id)
        return ResponseModel.success(data=result, message="Xóa hãng điện thoại thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
