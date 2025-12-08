from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from app.database.database import get_db
from app.dto.category_dto import CategoryCreate, CategoryRead, CategoryUpdate
from app.services.category_service import CategoryService
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.models.user import User
from app.logging.decorators import with_log_context

router = APIRouter()

@router.post("/", response_model=CategoryRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="category.create",
    event_category="category",
    source_layer="controller",
    source_controller="category_controller",
)
async def create_category(
    data: CategoryCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Tạo một danh mục mới."""
    # Cho phép người dùng đã xác thực tạo danh mục
    return await CategoryService.create_category(db=db, data=data)

@router.get("/{category_id}", response_model=CategoryRead)
@with_log_context(
    event_action="category.get",
    event_category="category",
    source_layer="controller",
    source_controller="category_controller",
)
async def get_category(
    category_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy thông tin danh mục theo ID."""
    # Cho phép tất cả người dùng đã xác thực xem danh mục
    return await CategoryService.get_category(db=db, category_id=category_id)

@router.put("/{category_id}", response_model=CategoryRead)
@with_log_context(
    event_action="category.update",
    event_category="category",
    source_layer="controller",
    source_controller="category_controller",
)
async def update_category(
    category_id: str,
    data: CategoryUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Cập nhật thông tin danh mục."""
    # Cho phép người dùng đã xác thực cập nhật danh mục
    return await CategoryService.update_category(db=db, category_id=category_id, data=data)

@router.get("/", response_model=List[CategoryRead])
@with_log_context(
    event_action="category.list",
    event_category="category",
    source_layer="controller",
    source_controller="category_controller",
)
async def get_all_categories(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy danh sách tất cả danh mục."""
    # Cho phép tất cả người dùng đã xác thực xem danh sách danh mục
    return await CategoryService.get_all_categories(db=db, skip=skip, limit=limit)

@router.delete("/{category_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="category.delete",
    event_category="category",
    source_layer="controller",
    source_controller="category_controller",
)
async def delete_category(
    category_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xóa danh mục."""
    # Cho phép người dùng đã xác thực xóa danh mục
    success = await CategoryService.delete_category(db=db, category_id=category_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Không tìm thấy danh mục"
        )
    return {"message": "Danh mục đã được xóa thành công"}
