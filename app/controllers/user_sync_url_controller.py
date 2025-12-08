from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.dto.user_sync_url_dto import UserSyncUrlCreate, UserSyncUrlUpdate, UserSyncUrlRead
from app.repositories.user_sync_url_repository import UserSyncUrlRepository
from fastapi import BackgroundTasks, Query
from app.dto.response import ResponseModel
from app.services.user_sync_from_url_service import UserSyncFromUrlService
from app.logging.decorators import with_log_context

router = APIRouter()

@router.get("/sync-url", response_model=Optional[UserSyncUrlRead])
@with_log_context(
    event_action="user_sync_url.get",
    event_category="user_sync_url",
    source_layer="controller",
    source_controller="user_sync_url_controller",
)
async def get_sync_url(
    type_url: Optional[str] = Query(None, description="Loại dữ liệu: device | component | service"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    record = await UserSyncUrlRepository.get_by_user_id(db, current_user.id, only_active=False, type_url=type_url)
    if not record:
        return None
    return UserSyncUrlRead(user_id=record.user_id, url=record.url, is_active=record.is_active, type_url=record.type_url, url_today=record.url_today)

@router.post("/sync-url", response_model=UserSyncUrlRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="user_sync_url.upsert",
    event_category="user_sync_url",
    source_layer="controller",
    source_controller="user_sync_url_controller",
)
async def upsert_sync_url(
    payload: UserSyncUrlCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if not payload.url:
        raise HTTPException(status_code=400, detail="URL không được để trống")
    record = await UserSyncUrlRepository.upsert(db, current_user.id, payload.url, payload.is_active, payload.type_url, payload.url_today)
    return UserSyncUrlRead(user_id=record.user_id, url=record.url, is_active=record.is_active, type_url=record.type_url, url_today=record.url_today)

@router.put("/sync-url", response_model=UserSyncUrlRead)
@with_log_context(
    event_action="user_sync_url.update",
    event_category="user_sync_url",
    source_layer="controller",
    source_controller="user_sync_url_controller",
)
async def update_sync_url(
    payload: UserSyncUrlUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    record = await UserSyncUrlRepository.update(
        db,
        current_user.id,
        url=payload.url,
        is_active=payload.is_active,
        type_url=payload.type_url,
        url_today=payload.url_today,
    )
    if not record:
        raise HTTPException(status_code=404, detail="Chưa có sync URL để cập nhật")
    return UserSyncUrlRead(user_id=record.user_id, url=record.url, is_active=record.is_active, type_url=record.type_url, url_today=record.url_today)

@router.delete("/sync-url", response_model=dict)
@with_log_context(
    event_action="user_sync_url.deactivate",
    event_category="user_sync_url",
    source_layer="controller",
    source_controller="user_sync_url_controller",
)
async def deactivate_sync_url(
    type_url: Optional[str] = Query(None, description="Loại dữ liệu cần vô hiệu hoá: device | component | service. Nếu bỏ trống sẽ vô hiệu hoá tất cả."),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    ok = await UserSyncUrlRepository.deactivate(db, current_user.id, type_url=type_url)
    if not ok:
        raise HTTPException(status_code=404, detail="Không tìm thấy sync URL để vô hiệu hoá")
    return {"success": True}


@router.post("/sync-url/sync-devices", response_model=ResponseModel[dict])
@with_log_context(
    event_action="user_sync_url.sync_devices",
    event_category="user_sync_url",
    source_layer="controller",
    source_controller="user_sync_url_controller",
)
async def sync_devices_from_url(
    background_tasks: BackgroundTasks,
    updated_today: bool = Query(False, description="Nếu true, chỉ đồng bộ các sản phẩm cập nhật hôm nay"),
    type_url: Optional[str] = Query(None, description="Loại dữ liệu để đồng bộ: device | component | service"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Đồng bộ danh sách thiết bị từ URL người dùng đã cấu hình vào bảng `user_devices` hiện có.
    Không yêu cầu migration. Tự động thêm/cập nhật và đồng bộ Chatbot như `UserDeviceService`.
    """
    try:
        result = await UserSyncFromUrlService.sync_from_url(
            db=db,
            user=current_user,
            updated_today=updated_today,
            background_tasks=background_tasks,
            type_url=type_url,
        )
        msg = "Đồng bộ thành công"
        t = result.get("type")
        if t == "component":
            msg = "Đồng bộ linh kiện từ URL thành công"
        elif t == "service":
            msg = "Đồng bộ dịch vụ từ URL thành công"
        else:
            msg = "Đồng bộ thiết bị từ URL thành công"
        return ResponseModel.success(
            data=result,
            message=msg
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
