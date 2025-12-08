from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks, File, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid
import io

from app.database.database import get_db
from app.dto.brand_dto import BrandCreate, BrandUpdate, BrandRead
from app.services.brand_service import BrandService
from app.services.excel_service import ExcelService
from app.dto.response import ResponseModel
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.logging.decorators import with_log_context

router = APIRouter(prefix="/brands", tags=["Brands"])

@router.post("/import", summary="Import brands from Excel for all services")
@with_log_context(
    event_action="brand.import",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def import_brands_from_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import brands from an Excel file for all services.
    The Excel file must contain a 'Tên dịch vụ' column.
    """
    content = await file.read()
    result = await ExcelService.import_brands(db, content, current_user.id, background_tasks, current_user)
    if result.error > 0:
        return ResponseModel.success(message="Import hoàn thành với một số lỗi.", data=result)
    return ResponseModel.success(message="Import thành công", data=result)

@router.get("/export", summary="Export brands to Excel")
@with_log_context(
    event_action="brand.export",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def export_brands_to_excel(
    service_ids: List[uuid.UUID] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Export brands for specified services to an Excel file.
    If no service_ids provided, export all brands for the current user.
    """
    if service_ids:
        # Export brands for specified services only for the current user
        brands_db = []
        for service_id in service_ids:
            service_brands = await BrandService.get_all_brands(db, service_id=service_id, limit=10000, user_id=current_user.id)
            brands_db.extend(service_brands)
        brands = [BrandRead.model_validate(b) for b in brands_db]
        filename = f"danh_sach_dich_vu_{len(service_ids)}.xlsx"
    else:
        # Export all brands for the current user
        brands_db = await BrandService.get_all_brands(db, service_id=None, limit=10000, user_id=current_user.id)
        brands = [BrandRead.model_validate(b) for b in brands_db]
        filename = "danh_sach_tat_ca_dich_vu.xlsx"
    
    excel_data = await ExcelService.export_brands(brands)
    
    return StreamingResponse(
        io.BytesIO(excel_data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@router.get("/export-template", summary="Export brands template Excel")
@with_log_context(
    event_action="brand.export_template",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def export_brands_template(
    current_user: User = Depends(get_current_user)
):
    """
    Export một file Excel mẫu cho brands với hướng dẫn sử dụng.
    Không yêu cầu database access.
    """
    excel_data = await ExcelService.export_brands_template()
    
    return StreamingResponse(
        io.BytesIO(excel_data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=mau_dich_vu.xlsx"}
    )

@router.post("", response_model=ResponseModel[BrandRead])
@with_log_context(
    event_action="brand.create",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def create_brand(
    data: BrandCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        brand = await BrandService.create_brand(db, data, background_tasks, current_user)
        return ResponseModel.success(data=brand, message="Tạo thương hiệu thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/{brand_id}", response_model=ResponseModel[BrandRead])
@with_log_context(
    event_action="brand.get",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def get_brand(
    brand_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        brand = await BrandService.get_brand(db, brand_id)
        return ResponseModel.success(data=brand, message="Lấy thông tin thương hiệu thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

@router.get("", response_model=ResponseModel[List[BrandRead]])
@with_log_context(
    event_action="brand.list",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def get_all_brands(
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=1000),
    search: Optional[str] = Query(None),
    service_id: Optional[uuid.UUID] = Query(None),
    sort_by: Optional[str] = Query(None, description="Tên trường để sắp xếp"),
    sort_order: Optional[str] = Query('asc', description="Thứ tự sắp xếp (asc hoặc desc)"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        total = await BrandService.count_brands(db, search, service_id, user_id=current_user.id)
        brands = await BrandService.get_all_brands(db, skip, limit, search, service_id, sort_by, sort_order, user_id=current_user.id)
        return ResponseModel(
            data=brands,
            message="Lấy danh sách thương hiệu thành công",
            total=total,
            totalPages=(total + limit - 1) // limit
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/unique-names/{service_id}", response_model=List[dict])
@with_log_context(
    event_action="brand.unique_names_by_service.list",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def get_unique_brand_names_for_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách các tên loại duy nhất và bảo hành mới nhất cho một dịch vụ.
    """
    try:
        unique_brands = await BrandService.get_unique_brand_names(db, service_id)
        return unique_brands
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.put("/{brand_id}", response_model=ResponseModel[BrandRead])
@with_log_context(
    event_action="brand.update",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def update_brand(
    brand_id: uuid.UUID,
    data: BrandUpdate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        brand = await BrandService.update_brand(db, brand_id, data, background_tasks, current_user)
        return ResponseModel.success(data=brand, message="Cập nhật thương hiệu thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{brand_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="brand.delete",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def delete_brand(
    brand_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        result = await BrandService.delete_brand(db, brand_id, background_tasks, current_user)
        return ResponseModel.success(data=result, message="Xóa mềm thương hiệu thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/deleted-today", response_model=ResponseModel[List[BrandRead]])
@with_log_context(
    event_action="brand.deleted_today.list",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def get_deleted_brands_today(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách các brands đã bị xóa mềm trong ngày hôm nay.
    """
    try:
        brands = await BrandService.get_deleted_brands_today(db, current_user.id)
        return ResponseModel.success(
            data=brands,
            message="Lấy danh sách brands đã xóa trong ngày thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/restore-all-today", response_model=ResponseModel[bool])
@with_log_context(
    event_action="brand.deleted_today.restore_all",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def restore_all_deleted_brands_today(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Khôi phục tất cả các brands đã bị xóa mềm trong ngày hôm nay.
    """
    try:
        result = await BrandService.restore_all_deleted_brands_today(db, current_user.id, background_tasks, current_user)
        return ResponseModel.success(
            data=result,
            message="Khôi phục tất cả brands đã xóa trong ngày thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{brand_id}/restore", response_model=ResponseModel[bool])
@with_log_context(
    event_action="brand.restore",
    event_category="brand",
    source_layer="controller",
    source_controller="brand_controller",
)
async def restore_brand(
    brand_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    try:
        result = await BrandService.restore_brand(db, brand_id, background_tasks, current_user)
        return ResponseModel.success(data=result, message="Khôi phục thương hiệu thành công")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
