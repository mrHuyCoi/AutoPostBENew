from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
import io

from app.database.database import get_db
from app.dto.device_info_dto import DeviceInfoCreate, DeviceInfoUpdate, DeviceInfoRead
from app.services.device_info_service import DeviceInfoService
from app.services.excel_service import ExcelService
from app.dto.response import ResponseModel
from app.dto.excel_dto import ImportResult
from app.dto.color_dto import ColorRead
from app.dto.device_storage_dto import DeviceStorageResponse
from app.middlewares.auth_middleware import get_current_user, get_current_user_optional
from app.models.user import User
from app.exceptions.not_found_exception import NotFoundException
from app.logging.decorators import with_log_context

# Tạo router
router = APIRouter(prefix="/device-infos", tags=["Device Info"])


@router.post("", response_model=ResponseModel[DeviceInfoRead])
@with_log_context(
    event_action="device_info.create",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def create_device_info(
    data: DeviceInfoCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo thông tin thiết bị mới.
    """
    try:
        device_info = await DeviceInfoService.create_device_info(db, data, current_user)
        return ResponseModel.success(
            data=device_info,
            message="Tạo thông tin thiết bị thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/brands", response_model=ResponseModel[List[str]])
@with_log_context(
    event_action="device_info.brands.list",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def get_distinct_brands(
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy danh sách các thương hiệu duy nhất.
    """
    try:
        brands = await DeviceInfoService.get_all_brands(db)
        return ResponseModel.success(
            data=brands,
            message="Lấy danh sách thương hiệu thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/import", summary="Import thông tin thiết bị từ Excel", response_model=ResponseModel[ImportResult])
@with_log_context(
    event_action="device_info.import",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def import_device_infos_from_excel(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import thông tin thiết bị, bao gồm dung lượng và màu sắc, từ một file Excel.
    - Cập nhật thiết bị nếu model đã tồn tại và thuộc về người dùng.
    - Tạo mới nếu model chưa tồn tại.
    - Báo lỗi nếu model thuộc về hệ thống (không có user_id).
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Định dạng file không hợp lệ, chỉ chấp nhận .xlsx và .xls")
    
    content = await file.read()
    try:
        result = await ExcelService.import_device_infos(db, content, current_user)
        return ResponseModel.success(
            data=result,
            message="Import file Excel hoàn tất."
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi khi import file: {str(e)}")


@router.get("/export-template", summary="Export file Excel mẫu cho thông tin thiết bị")
@with_log_context(
    event_action="device_info.export_template",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def export_device_info_template():
    """
    Tải về một file Excel mẫu với các cột cần thiết để import thông tin thiết bị.
    """
    try:
        file_content = await ExcelService.export_device_info_template()
        return StreamingResponse(
            io.BytesIO(file_content),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": "attachment; filename=mau_import_thiet_bi.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Không thể xuất file mẫu: {str(e)}")


@router.get("/export", summary="Export thông tin thiết bị ra Excel")
@with_log_context(
    event_action="device_info.export",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def export_device_infos_to_excel(
    search: Optional[str] = Query(None, description="Tìm kiếm theo tên model"),
    brand: Optional[str] = Query(None, description="Lọc theo tên thương hiệu"),
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Export danh sách thông tin thiết bị ra file Excel.
    """
    try:
        file_content = await DeviceInfoService.export_device_infos(db, current_user, search, brand)
        return StreamingResponse(
            io.BytesIO(file_content),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": "attachment; filename=danh_sach_thiet_bi.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Không thể xuất file Excel: {str(e)}")


@router.get("/{device_info_id}", response_model=ResponseModel[DeviceInfoRead])
@with_log_context(
    event_action="device_info.get",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def get_device_info(
    device_info_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin thiết bị theo ID.
    """
    try:
        device_info = await DeviceInfoService.get_device_info(db, device_info_id)
        return ResponseModel.success(
            data=device_info,
            message="Lấy thông tin thiết bị thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("", response_model=ResponseModel[List[DeviceInfoRead]])
@with_log_context(
    event_action="device_info.list",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def get_all_device_infos(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    search: Optional[str] = Query(None, description="Tìm kiếm theo tên model"),
    brand: Optional[str] = Query(None, description="Lọc theo tên thương hiệu"),
    sort_by: Optional[str] = Query(None, description="Trường để sắp xếp"),
    sort_order: Optional[str] = Query("desc", description="Thứ tự sắp xếp (asc hoặc desc)"),
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Lấy danh sách thông tin thiết bị với phân trang và tìm kiếm.
    """
    try:
        device_infos = await DeviceInfoService.get_all_device_infos(db, skip, limit, current_user, search, brand, sort_by, sort_order)
        # Đếm tổng số bản ghi để phân trang
        total_count = await DeviceInfoService.count_device_infos(db, current_user, search, brand)
        total_pages = max(1, (total_count + limit - 1) // limit)  # Ceiling division
        
        # Tính pageNum từ skip và limit (pageNum bắt đầu từ 1)
        page_num = (skip // limit) + 1
        
        return ResponseModel.success(
            data=device_infos,
            message="Lấy danh sách thông tin thiết bị thành công",
            pagination={
                "pageNum": page_num,
                "pageSize": limit,
                "total": total_count,
                "totalPages": total_pages
            },
            total=total_count,
            totalPages=total_pages
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/delete-all", response_model=ResponseModel[bool])
@with_log_context(
    event_action="device_info.delete_all",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def delete_all_device_infos(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa tất cả thông tin thiết bị của người dùng hiện tại.
    """
    try:
        await DeviceInfoService.delete_all_user_device_infos(db, current_user)
        return ResponseModel.success(
            data=True,
            message="Xóa tất cả thông tin thiết bị thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{device_info_id}", response_model=ResponseModel[DeviceInfoRead])
@with_log_context(
    event_action="device_info.update",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def update_device_info(
    device_info_id: uuid.UUID,
    data: DeviceInfoUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật thông tin thiết bị.
    """
    try:
        device_info = await DeviceInfoService.update_device_info(db, device_info_id, data, current_user)
        return ResponseModel.success(
            data=device_info,
            message="Cập nhật thông tin thiết bị thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{device_info_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="device_info.delete",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def delete_device_info(
    device_info_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa thông tin thiết bị.
    """
    try:
        result = await DeviceInfoService.delete_device_info(db, device_info_id, current_user)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn không có quyền xóa mục này hoặc mục không tồn tại."
            )
        return ResponseModel.success(
            data=result,
            message="Xóa thông tin thiết bị thành công"
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{device_info_id}/colors/{color_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="device_info.colors.add",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def add_color_to_device(
    device_info_id: uuid.UUID,
    color_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Thêm một màu sắc cho thiết bị.
    """
    try:
        result = await DeviceInfoService.add_color_to_device(db, device_info_id, color_id, current_user)
        return ResponseModel(
            data=result,
            message="Thêm màu sắc cho thiết bị thành công"
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


@router.delete("/{device_info_id}/colors/{color_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="device_info.colors.remove",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def remove_color_from_device(
    device_info_id: uuid.UUID,
    color_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa một màu sắc khỏi thiết bị.
    """
    try:
        result = await DeviceInfoService.remove_color_from_device(db, device_info_id, color_id, current_user)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Bạn không có quyền xóa màu khỏi thiết bị này."
            )
        return ResponseModel.success(
            data=result,
            message="Xóa màu sắc khỏi thiết bị thành công"
        )
    except NotFoundException as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Đã xảy ra lỗi: {str(e)}"
        )


@router.get("/{device_info_id}/colors", response_model=ResponseModel[List[ColorRead]])
@with_log_context(
    event_action="device_info.colors.list",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def get_device_colors(
    device_info_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách màu sắc của một thiết bị.
    """
    try:
        colors = await DeviceInfoService.get_device_colors(db, device_info_id, current_user)
        return ResponseModel(
            data=colors,
            message="Lấy danh sách màu sắc của thiết bị thành công"
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



class DeleteDeviceInfosRequest(BaseModel):
    ids: List[uuid.UUID]

@router.post("/delete-multiple", response_model=ResponseModel[bool])
@with_log_context(
    event_action="device_info.bulk_delete",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
)
async def delete_multiple_device_infos(
    request: DeleteDeviceInfosRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa nhiều thông tin thiết bị dựa trên danh sách ID.
    """
    try:
        await DeviceInfoService.delete_multiple_device_infos(db, request.ids, current_user)
        return ResponseModel.success(
            data=True,
            message="Xóa các thông tin thiết bị đã chọn thành công"
        )
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/{device_info_id}/storages", response_model=ResponseModel[List[DeviceStorageResponse]])
@with_log_context(
    event_action="device_info.storages.list",
    event_category="device_info",
    source_layer="controller",
    source_controller="device_info_controller",
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
        storages = await DeviceInfoService.get_device_storages(db, device_info_id, current_user)
        return ResponseModel(
            data=storages,
            message="Lấy danh sách dung lượng của thiết bị thành công"
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