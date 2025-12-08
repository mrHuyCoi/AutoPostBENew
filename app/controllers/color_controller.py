from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.database.database import get_db
from app.dto.color_dto import ColorCreate, ColorUpdate, ColorRead
from app.dto.device_info_dto import DeviceInfoRead
from app.services.color_service import ColorService
from app.dto.response import ResponseModel
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.repositories.color_repository import ColorRepository
from app.exceptions.not_found_exception import NotFoundException
from app.logging.decorators import with_log_context

# Tạo router
router = APIRouter(prefix="/colors", tags=["Colors"])


@router.post("", response_model=ResponseModel[ColorRead])
@with_log_context(
    event_action="color.create",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def create_color(
    data: ColorCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo màu sắc mới.
    """
    try:
        # Sử dụng current_user để phân quyền admin
        color = await ColorService.create_color(db, data, current_user)
        return ResponseModel.success(
            data=color,
            message="Tạo màu sắc thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{color_id}", response_model=ResponseModel[ColorRead])
@with_log_context(
    event_action="color.get",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def get_color(
    color_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin màu sắc theo ID.
    """
    try:
        # Chỉ lấy màu sắc của user hiện tại hoặc màu sắc mặc định
        color = await ColorService.get_color(db, color_id, current_user)
        return ResponseModel.success(
            data=color,
            message="Lấy thông tin màu sắc thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/{color_id}/devices", response_model=ResponseModel[List[DeviceInfoRead]])
@with_log_context(
    event_action="color.devices.list",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def get_devices_by_color_id(
    color_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách thiết bị theo ID của màu sắc.
    """
    try:
        # Chỉ lấy thiết bị của user hiện tại hoặc thiết bị mặc định
        devices = await ColorService.get_devices_by_color_id(db, color_id, current_user)
        return ResponseModel(
            data=devices,
            message="Lấy danh sách thiết bị theo màu sắc thành công"
        )
    except NotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Đã xảy ra lỗi: {str(e)}"
        )


@router.get("", response_model=ResponseModel[List[ColorRead]])
@with_log_context(
    event_action="color.list",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def get_all_colors(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách màu sắc với phân trang và tìm kiếm.
    """
    try:
        # Lấy tổng số màu sắc để phân trang (chỉ màu sắc của user hoặc mặc định)
        total = await ColorService.count_colors(db, search, current_user)
        
        # Lấy danh sách màu sắc với phân trang và tìm kiếm (chỉ màu sắc của user hoặc mặc định)
        colors = await ColorService.get_all_colors(db, skip, limit, search, current_user)
        total_pages = (total + limit - 1) // limit
        page_num = (skip // limit) + 1

        return ResponseModel.success(
            data=colors,
            message="Lấy danh sách màu sắc thành công",
            total=total,
            totalPages=max(1, (total + limit - 1) // limit),
            pagination = {
                "pageNum": page_num,
                "pageSize": limit,
                "total": total,
                "totalPages": total_pages
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{color_id}", response_model=ResponseModel[ColorRead])
@with_log_context(
    event_action="color.update",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def update_color(
    color_id: uuid.UUID,
    data: ColorUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật thông tin màu sắc.
    """
    try:
        # Chỉ cho phép cập nhật màu sắc của chính mình
        color = await ColorService.update_color(db, color_id, data, current_user)
        return ResponseModel.success(
            data=color,
            message="Cập nhật màu sắc thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{color_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="color.delete",
    event_category="color",
    source_layer="controller",
    source_controller="color_controller",
)
async def delete_color(
    color_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa màu sắc.
    """
    try:
        result = await ColorService.delete_color(db, color_id, current_user)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn không có quyền xóa màu này hoặc màu không tồn tại."
            )
        return ResponseModel.success(
            data=result,
            message="Xóa màu sắc thành công"
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )