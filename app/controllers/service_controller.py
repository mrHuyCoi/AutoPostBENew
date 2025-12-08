from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.database.database import get_db
from app.dto.service_dto import ServiceCreate, ServiceUpdate, ServiceRead, DeletedServiceWithBrands
# SỬA: Import DTO 'BulkDeletePayload'
from app.dto.service_dto import (
    ServiceCreate, ServiceUpdate, ServiceRead, DeletedServiceWithBrands,
    BulkDeletePayload 
)
from app.services.service_service import ServiceService
from app.dto.response import ResponseModel
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User

router = APIRouter(prefix="/services", tags=["Services"])

# (Các hàm .../deleted-today, .../restore-all-today giữ nguyên)
# ...
from app.logging.decorators import with_log_context
@router.get("/deleted-today", response_model=ResponseModel[List[DeletedServiceWithBrands]])
async def get_deleted_services_today(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách các services đã bị xóa mềm trong ngày hôm nay.
    """
    try:
        services = await ServiceService.get_deleted_services_today(db, current_user.id)
        return ResponseModel.success(
            data=services,
            message="Lấy danh sách services đã xóa trong ngày thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/restore-all-today", response_model=ResponseModel[dict])
@with_log_context(
    event_action="service.deleted_today.restore_all",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def restore_all_deleted_services_today(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Khôi phục tất cả các services đã bị xóa mềm trong ngày hôm nay.
    """
    try:
        result = await ServiceService.restore_all_deleted_services_today(db, current_user.id)
        return ResponseModel.success(
            data=result,
            message="Khôi phục tất cả services đã xóa trong ngày thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("", response_model=ResponseModel[ServiceRead])
@with_log_context(
    event_action="service.create",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def create_service(
    data: ServiceCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        service = await ServiceService.create_service(db, data, current_user.id)
        return ResponseModel.success(data=service, message="Tạo dịch vụ thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/{service_id}", response_model=ResponseModel[ServiceRead])
@with_log_context(
    event_action="service.get",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def get_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        service = await ServiceService.get_service(db, service_id)
        
        # SỬA: Kiểm tra admin bằng 'role'
        is_admin = getattr(current_user, 'role', 'user') == 'admin'
        if not is_admin and service.user_id != current_user.id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Không tìm thấy dịch vụ")
            
        return ResponseModel.success(data=service, message="Lấy thông tin dịch vụ thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("", response_model=ResponseModel[List[ServiceRead]])
@with_log_context(
    event_action="service.list",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def get_all_services(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    search: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        skip = (page - 1) * limit
        
        # SỬA: Kiểm tra Admin bằng 'role'
        user_id_to_filter = None
        if getattr(current_user, 'role', 'user') != 'admin':
            user_id_to_filter = current_user.id
        
        # user_id_to_filter sẽ là None nếu là Admin
        total = await ServiceService.count_services(db, search, user_id_to_filter)
        services = await ServiceService.get_all_services(db, skip, limit, search, user_id_to_filter)
        
        total_pages = (total + limit - 1) // limit

        return ResponseModel(
            data=services,
            message="Lấy danh sách dịch vụ thành công",
            metadata={
                "total": total,
                "page": page,
                "limit": limit,
                "total_pages": total_pages
            }
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.put("/{service_id}", response_model=ResponseModel[ServiceRead])
@with_log_context(
    event_action="service.update",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def update_service(
    service_id: uuid.UUID,
    data: ServiceUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # Thêm kiểm tra quyền
        service_to_update = await ServiceService.get_service(db, service_id)
        is_admin = getattr(current_user, 'role', 'user') == 'admin'
        if not is_admin and service_to_update.user_id != current_user.id:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Không có quyền cập nhật dịch vụ này")

        service = await ServiceService.update_service(db, service_id, data)
        return ResponseModel.success(data=service, message="Cập nhật dịch vụ thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/bulk", response_model=ResponseModel[dict])
@with_log_context(
    event_action="service.bulk_delete",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def bulk_delete_services(
    payload: BulkDeletePayload, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        service_ids = payload.ids 
        if not service_ids:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Danh sách ID dịch vụ không được để trống")
        
        # Truyền current_user xuống service để kiểm tra quyền
        # (Giả định service_service.bulk_delete_services đã được cập nhật để nhận current_user)
        result = await ServiceService.bulk_delete_services(db, service_ids, current_user) 
        
        if result["error_count"] > 0:
            message = f"Xóa thành công {result['success_count']} dịch vụ, {result['error_count']} dịch vụ lỗi"
        else:
            message = f"Xóa thành công {result['success_count']} dịch vụ"
            
        return ResponseModel.success(data=result, message=message)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{service_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="service.delete",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def delete_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # Thêm kiểm tra quyền
        service_to_delete = await ServiceService.get_service(db, service_id)
        is_admin = getattr(current_user, 'role', 'user') == 'admin'
        if not is_admin and service_to_delete.user_id != current_user.id:
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Không có quyền xóa dịch vụ này")

        result = await ServiceService.delete_service(db, service_id)
        return ResponseModel.success(data=result, message="Xóa mềm dịch vụ thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

# (Đã khai báo ở trên: /deleted-today và /restore-all-today)

@router.post("/{service_id}/restore", response_model=ResponseModel[bool])
@with_log_context(
    event_action="service.restore",
    event_category="service",
    source_layer="controller",
    source_controller="service_controller",
)
async def restore_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        # (Bạn có thể thêm logic kiểm tra quyền ở đây)
        result = await ServiceService.restore_service(db, service_id)
        return ResponseModel.success(data=result, message="Khôi phục dịch vụ thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
