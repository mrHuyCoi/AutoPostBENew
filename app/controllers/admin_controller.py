from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
import uuid

from app.database.database import get_db
from app.dto.user_dto import UserRead, UserUpdate
from app.services.user_service import UserService
from app.middlewares.auth_middleware import get_current_active_superuser
from app.logging.decorators import with_log_context

router = APIRouter()

@router.get("/users", response_model=List[UserRead])
@with_log_context(
    event_action="admin.users.list",
    event_category="admin",
    source_layer="controller",
    source_controller="admin_controller",
)
async def get_all_users(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Lấy danh sách tất cả người dùng.
    """
    return await UserService.get_all_users(db, skip, limit)

@router.get("/users/{user_id}", response_model=UserRead)
@with_log_context(
    event_action="admin.user.get",
    event_category="admin",
    source_layer="controller",
    source_controller="admin_controller",
)
async def get_user_by_id(
    user_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Lấy thông tin chi tiết của một người dùng bằng ID.
    """
    user = await UserService.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng")
    return user

@router.put("/users/{user_id}", response_model=UserRead)
@with_log_context(
    event_action="admin.user.update",
    event_category="admin",
    source_layer="controller",
    source_controller="admin_controller",
)
async def update_user_by_id(
    user_id: uuid.UUID,
    data: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Cập nhật thông tin của một người dùng.
    """
    return await UserService.update_user(db, user_id, data)

@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="admin.user.delete",
    event_category="admin",
    source_layer="controller",
    source_controller="admin_controller",
)
async def delete_user_by_id(
    user_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_active_superuser)
):
    """
    [ADMIN] Xóa một người dùng.
    """
    success = await UserService.delete_user(db, user_id)
    if not success:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng")
    return
