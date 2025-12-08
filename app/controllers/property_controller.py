from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.database.database import get_db
from app.dto.property_dto import PropertyCreate, PropertyRead, PropertyUpdate
from app.services.property_service import PropertyService
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.models.user import User
from app.logging.decorators import with_log_context

router = APIRouter()

@router.post("/", response_model=PropertyRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="property.create",
    event_category="property",
    source_layer="controller",
    source_controller="property_controller",
)
async def create_property(
    data: PropertyCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Tạo một thuộc tính mới."""
    # Cho phép người dùng đã xác thực tạo thuộc tính
    return await PropertyService.create_property(db=db, data=data)

@router.get("/{property_id}", response_model=PropertyRead)
@with_log_context(
    event_action="property.get",
    event_category="property",
    source_layer="controller",
    source_controller="property_controller",
)
async def get_property(
    property_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy thông tin thuộc tính theo ID."""
    # Cho phép tất cả người dùng đã xác thực xem thuộc tính
    return await PropertyService.get_property(db=db, property_id=property_id)

@router.put("/{property_id}", response_model=PropertyRead)
@with_log_context(
    event_action="property.update",
    event_category="property",
    source_layer="controller",
    source_controller="property_controller",
)
async def update_property(
    property_id: str,
    data: PropertyUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Cập nhật thông tin thuộc tính."""
    # Cho phép người dùng đã xác thực cập nhật thuộc tính
    return await PropertyService.update_property(db=db, property_id=property_id, data=data)

@router.get("/", response_model=List[PropertyRead])
@with_log_context(
    event_action="property.list",
    event_category="property",
    source_layer="controller",
    source_controller="property_controller",
)
async def get_all_properties(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy danh sách tất cả thuộc tính."""
    # Cho phép tất cả người dùng đã xác thực xem danh sách thuộc tính
    return await PropertyService.get_all_properties(db=db, skip=skip, limit=limit)

@router.delete("/{property_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="property.delete",
    event_category="property",
    source_layer="controller",
    source_controller="property_controller",
)
async def delete_property(
    property_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xóa thuộc tính."""
    # Cho phép người dùng đã xác thực xóa thuộc tính
    success = await PropertyService.delete_property(db=db, property_id=property_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Không tìm thấy thuộc tính"
        )
    return {"message": "Thuộc tính đã được xóa thành công"}
