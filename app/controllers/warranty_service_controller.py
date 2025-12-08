from typing import List
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.database import get_db
from app.models.user import User
from app.schemas import warranty_service as warranty_service_schema
from app.repositories.warranty_service_repository import WarrantyServiceRepository
from app.middlewares.auth_middleware import get_current_user
from app.logging.decorators import with_log_context

router = APIRouter()


@router.post("/", response_model=warranty_service_schema.WarrantyService, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="warranty_service.create",
    event_category="warranty_service",
    source_layer="controller",
    source_controller="warranty_service_controller",
)
async def create_warranty_service(
    warranty_service: warranty_service_schema.WarrantyServiceCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo một dịch vụ bảo hành mới.
    """
    repo = WarrantyServiceRepository(db)
    return await repo.create_warranty_service(warranty_service=warranty_service, user_id=current_user.id)


@router.get("/", response_model=List[warranty_service_schema.WarrantyService])
@with_log_context(
    event_action="warranty_service.list",
    event_category="warranty_service",
    source_layer="controller",
    source_controller="warranty_service_controller",
)
async def read_warranty_services(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách các dịch vụ bảo hành của người dùng hiện tại.
    """
    repo = WarrantyServiceRepository(db)
    return await repo.get_warranty_services_by_user(user_id=current_user.id, skip=skip, limit=limit)


@router.get("/{warranty_service_id}", response_model=warranty_service_schema.WarrantyService)
@with_log_context(
    event_action="warranty_service.get",
    event_category="warranty_service",
    source_layer="controller",
    source_controller="warranty_service_controller",
)
async def read_warranty_service(
    warranty_service_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin chi tiết của một dịch vụ bảo hành.
    """
    repo = WarrantyServiceRepository(db)
    db_warranty_service = await repo.get_warranty_service(warranty_service_id=warranty_service_id)
    if db_warranty_service is None or db_warranty_service.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Warranty service not found")
    return db_warranty_service


@router.put("/{warranty_service_id}", response_model=warranty_service_schema.WarrantyService)
@with_log_context(
    event_action="warranty_service.update",
    event_category="warranty_service",
    source_layer="controller",
    source_controller="warranty_service_controller",
)
async def update_warranty_service(
    warranty_service_id: UUID,
    warranty_service_update: warranty_service_schema.WarrantyServiceUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật thông tin một dịch vụ bảo hành.
    """
    repo = WarrantyServiceRepository(db)
    db_warranty_service = await repo.get_warranty_service(warranty_service_id=warranty_service_id)
    if db_warranty_service is None or db_warranty_service.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Warranty service not found")
    
    return await repo.update_warranty_service(warranty_service_id=warranty_service_id, warranty_service_update=warranty_service_update)


@router.delete("/{warranty_service_id}", response_model=warranty_service_schema.WarrantyService)
@with_log_context(
    event_action="warranty_service.delete",
    event_category="warranty_service",
    source_layer="controller",
    source_controller="warranty_service_controller",
)
async def delete_warranty_service(
    warranty_service_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa một dịch vụ bảo hành.
    """
    repo = WarrantyServiceRepository(db)
    db_warranty_service = await repo.get_warranty_service(warranty_service_id=warranty_service_id)
    if db_warranty_service is None or db_warranty_service.user_id != current_user.id:
        raise HTTPException(status_code=404, detail="Warranty service not found")
        
    return await repo.delete_warranty_service(warranty_service_id=warranty_service_id) 