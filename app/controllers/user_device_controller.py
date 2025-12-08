from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional, Dict, Any
import uuid
import io

from app.database.database import get_db
from app.dto.user_device_dto import UserDeviceCreate, UserDeviceUpdate, UserDeviceRead, UserDeviceDetailRead, BulkDeleteRequest
from app.services.user_device_service import UserDeviceService
from app.services.excel_service import ExcelService
from app.dto.response import ResponseModel
from app.dto.excel_dto import ImportResult
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.logging.decorators import with_log_context

# Tạo router
router = APIRouter(prefix="/user-devices", tags=["User Devices"])


@router.post("/import", response_model=ResponseModel[ImportResult])
@with_log_context(
    event_action="user_device.import",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def import_user_devices(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import danh sách thiết bị người dùng từ file Excel.
    """
    # Tất cả người dùng đều có quyền import thiết bị của họ
    try:
        # Đọc nội dung file
        file_content = await file.read()
        
        # Import dữ liệu với user_id của người dùng hiện tại
        result = await ExcelService.import_user_devices(db, file_content, current_user.id, background_tasks)
        return ResponseModel.success(
            data=result,
            message="Import danh sách thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/export", response_class=StreamingResponse)
@with_log_context(
    event_action="user_device.export_my_devices",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def export_my_devices(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Export danh sách thiết bị của người dùng hiện tại ra file Excel.
    """
    try:
        # Lấy danh sách thiết bị của người dùng hiện tại (không giới hạn số lượng cho export)
        user_devices = await UserDeviceService.get_user_devices_by_user_id(db, current_user.id, with_details=True, limit=None)
        filename = f"my_devices.xlsx"
        
        # Export ra Excel
        excel_data = await ExcelService.export_user_devices(db, user_devices)
        
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.get("/export-template", response_class=StreamingResponse)
@with_log_context(
    event_action="user_device.export_template",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def export_user_devices_template(
    current_user: User = Depends(get_current_user)
):
    """
    Export một file Excel mẫu cho user devices với hướng dẫn sử dụng.
    Không yêu cầu database access.
    """
    try:
        excel_data = await ExcelService.export_user_devices_template()
        
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=mau_thiet_bi.xlsx"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )





@router.get("/template", response_class=StreamingResponse)
@with_log_context(
    event_action="user_device.import_template",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_import_template(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tải xuống template Excel để import thiết bị người dùng.
    """
    # Tất cả người dùng đều có quyền tải template
    try:
        # Tạo template Excel
        excel_data, _ = await ExcelService.generate_template(db)
        
        return StreamingResponse(
            io.BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=user_device_template.xlsx"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("", response_model=ResponseModel[UserDeviceRead])
@with_log_context(
    event_action="user_device.create",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def create_user_device(
    data: dict,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Tạo thiết bị người dùng mới.
    """
    try:
        # Tự động sử dụng ID của người dùng hiện tại từ token
        # Chuyển đổi dữ liệu từ dict sang UserDeviceCreate
        device_data = UserDeviceCreate(
            user_id=current_user.id,  # Lấy user_id từ token
            device_info_id=data.get("device_info_id"),
            color_id=data.get("color_id"),
            device_storage_id=data.get("device_storage_id"),
            product_code=data.get("product_code"),
            warranty=data.get("warranty"),
            device_condition=data.get("device_condition"),
            device_type=data.get("device_type"),
            battery_condition=data.get("battery_condition"),
            price=data.get("price"),
            wholesale_price=data.get("wholesale_price"),
            inventory=data.get("inventory"),
            notes=data.get("notes")
        )
        
        user_device = await UserDeviceService.create_user_device(db, device_data, current_user, background_tasks)
        return ResponseModel.success(
            data=user_device,
            message="Tạo thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/my-devices", response_model=ResponseModel[List[UserDeviceDetailRead]])
@with_log_context(
    event_action="user_device.my_devices.list",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_my_devices(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    sort_by: Optional[str] = Query(None, description="Tên trường để sắp xếp"),
    sort_order: Optional[str] = Query('asc', description="Thứ tự sắp xếp ('asc' hoặc 'desc')"),
    skip: int = Query(0, ge=0, description="Số lượng bản ghi bỏ qua"),
    limit: int = Query(10, ge=1, le=100, description="Số lượng bản ghi tối đa trả về"),
    search: Optional[str] = Query(None, description="Tìm kiếm theo model hoặc mã sản phẩm"),
    brand: Optional[str] = Query(None, description="Lọc theo tên hãng sản xuất"),
    inventory_min: Optional[int] = Query(None, description="Tồn kho tối thiểu"),
    inventory_max: Optional[int] = Query(None, description="Tồn kho tối đa"),
    price_min: Optional[float] = Query(None, description="Giá tối thiểu"),
    price_max: Optional[float] = Query(None, description="Giá tối đa"),
    storage_capacity: Optional[int] = Query(None, description="Lọc theo dung lượng bộ nhớ (GB)")
):
    """
    Lấy danh sách thiết bị của người dùng hiện tại với bộ lọc, chi tiết và phân trang.
    """
    filters = {
        "search": search,
        "brand": brand,
        "inventory_min": inventory_min,
        "inventory_max": inventory_max,
        "price_min": price_min,
        "price_max": price_max,
        "storage_capacity": storage_capacity
    }
    # Loại bỏ các giá trị None khỏi bộ lọc
    active_filters = {k: v for k, v in filters.items() if v is not None}

    try:
        # Lấy tổng số thiết bị của người dùng với bộ lọc
        total_devices = await UserDeviceService.count_user_devices(db, current_user.id, filters=active_filters)
        
        # Lấy danh sách thiết bị với phân trang và bộ lọc
        user_devices = await UserDeviceService.get_user_devices_by_user_id(
            db, 
            current_user.id, 
            with_details=True, 
            sort_by=sort_by, 
            sort_order=sort_order, 
            skip=skip, 
            limit=limit,
            filters=active_filters
        )
        
        # Tính toán thông tin phân trang
        total_pages = (total_devices + limit - 1) // limit if limit > 0 else 0
        
        return ResponseModel.success(
            data=user_devices,
            message="Lấy danh sách thiết bị người dùng thành công",
            total=total_devices,
            totalPages=total_pages
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/deleted-today", response_model=ResponseModel[List[UserDeviceDetailRead]])
@with_log_context(
    event_action="user_device.deleted_today.list",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_deleted_devices_today(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách thiết bị đã bị xóa mềm trong ngày hôm nay của người dùng hiện tại.
    """
    try:
        deleted_devices = await UserDeviceService.get_deleted_devices_today(db, current_user.id)
        return ResponseModel.success(
            data=deleted_devices,
            message="Lấy danh sách thiết bị đã xóa trong ngày thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{device_id}", response_model=ResponseModel[UserDeviceDetailRead])
@with_log_context(
    event_action="user_device.get",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_user_device(
    device_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy thông tin thiết bị người dùng theo ID với đầy đủ chi tiết.
    """
    try:
        user_device = await UserDeviceService.get_user_device(db, device_id, with_details=True)
        
        # Nếu không phải admin, chỉ có thể xem thiết bị của chính mình
        if not current_user.is_admin and user_device.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Không có quyền thực hiện hành động này"
            )
        
        return ResponseModel.success(
            data=user_device,
            message="Lấy thông tin thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/user/{user_id}", response_model=ResponseModel[List[UserDeviceDetailRead]])
@with_log_context(
    event_action="user_device.user_devices.list",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_user_devices_by_user_id(
    user_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách thiết bị của một người dùng với đầy đủ chi tiết.
    Chỉ admin hoặc chính người dùng đó mới có quyền xem.
    """
    # Nếu không phải admin, chỉ có thể xem thiết bị của chính mình
    if not current_user.is_admin and user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Không có quyền thực hiện hành động này"
        )
    
    try:
        user_devices = await UserDeviceService.get_user_devices_by_user_id(db, user_id, with_details=True)
        return ResponseModel.success(
            data=user_devices,
            message="Lấy danh sách thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("", response_model=ResponseModel[List[UserDeviceDetailRead]])
@with_log_context(
    event_action="user_device.list",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def get_all_user_devices(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách thiết bị người dùng với phân trang và đầy đủ chi tiết.
    Chỉ admin mới có quyền xem tất cả thiết bị.
    """
    # Chỉ admin mới có quyền xem tất cả thiết bị
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Không có quyền thực hiện hành động này"
        )
    
    try:
        user_devices = await UserDeviceService.get_all_user_devices(db, skip, limit, with_details=True)
        return ResponseModel.success(
            data=user_devices,
            message="Lấy danh sách thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{device_id}", response_model=ResponseModel[UserDeviceRead])
@with_log_context(
    event_action="user_device.update",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def update_user_device(
    device_id: uuid.UUID,
    data: UserDeviceUpdate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật thông tin thiết bị người dùng.
    """
    try:
        # Lấy thông tin thiết bị hiện tại để kiểm tra quyền
        current_device = await UserDeviceService.get_user_device(db, device_id)
        
        # Nếu không phải admin, chỉ có thể cập nhật thiết bị của chính mình
        if not current_user.is_admin and current_device.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Không có quyền thực hiện hành động này"
            )
        
        user_device = await UserDeviceService.update_user_device(db, device_id, data, current_user, background_tasks)
        return ResponseModel.success(
            data=user_device,
            message="Cập nhật thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/bulk", response_model=ResponseModel[int])
@with_log_context(
    event_action="user_device.bulk_delete",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def delete_many_user_devices(
    request_data: BulkDeleteRequest,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa nhiều thiết bị người dùng.
    """
    try:
        deleted_count = await UserDeviceService.delete_many_devices(db, request_data.user_device_ids, current_user, background_tasks)
        return ResponseModel.success(
            data=deleted_count,
            message=f"Đã xóa thành công {deleted_count} thiết bị."
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/all", response_model=ResponseModel[int])
@with_log_context(
    event_action="user_device.delete_all",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def delete_all_user_devices(
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa tất cả thiết bị của người dùng hiện tại.
    """
    try:
        deleted_count = await UserDeviceService.delete_all_devices(db, current_user, background_tasks)
        return ResponseModel.success(
            data=deleted_count,
            message=f"Đã xóa thành công tất cả {deleted_count} thiết bị của bạn."
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.delete("/{device_id}", response_model=ResponseModel[bool])
@with_log_context(
    event_action="user_device.delete",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def delete_user_device(
    device_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Xóa mềm thiết bị người dùng.
    """
    try:
        # Lấy thông tin thiết bị hiện tại để kiểm tra quyền
        current_device = await UserDeviceService.get_user_device(db, device_id)
        
        # Nếu không phải admin, chỉ có thể xóa thiết bị của chính mình
        if not current_user.is_admin and current_device.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Không có quyền thực hiện hành động này"
            )
        
        result = await UserDeviceService.delete_user_device(db, device_id, current_user, background_tasks)
        return ResponseModel.success(
            data=result,
            message="Xóa mềm thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/{device_id}/restore", response_model=ResponseModel[bool])
@with_log_context(
    event_action="user_device.restore",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def restore_user_device(
    device_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Khôi phục thiết bị người dùng đã bị xóa mềm.
    """
    try:
        result = await UserDeviceService.restore_user_device(db, device_id, current_user, background_tasks)
        return ResponseModel.success(
            data=result,
            message="Khôi phục thiết bị người dùng thành công"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/restore-all-today", response_model=ResponseModel[Dict[str, Any]])
@with_log_context(
    event_action="user_device.deleted_today.restore_all",
    event_category="user_device",
    source_layer="controller",
    source_controller="user_device_controller",
)
async def restore_all_deleted_today(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Khôi phục tất cả thiết bị đã bị xóa mềm trong ngày hôm nay của người dùng hiện tại.
    """
    try:
        result = await UserDeviceService.restore_all_deleted_today(db, current_user.id, current_user, background_tasks)
        return ResponseModel.success(
            data=result,
            message=f"Đã khôi phục thành công {result['restored_count']} thiết bị"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )