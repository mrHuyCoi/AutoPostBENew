"""
Data Sync Controller - API endpoints for PostgreSQL-Elasticsearch synchronization.

This controller provides endpoints to:
1. Manually trigger full sync from PostgreSQL to Elasticsearch
2. Sync specific data types (products, accessories, services)
3. Check Elasticsearch connection status
4. Clear and rebuild ES data
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
import uuid

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.models.user import User
from app.services.data_sync_service import DataSyncService
from app.dto.response import ResponseModel
from app.logging.decorators import with_log_context

router = APIRouter(prefix="/data-sync", tags=["Data Synchronization"])


@router.get("/health", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.health_check",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def check_elasticsearch_health(
    current_user: User = Depends(get_current_user)
):
    """
    Kiểm tra kết nối với Elasticsearch.
    """
    try:
        result = await DataSyncService.check_es_connection()
        if result.get("connected"):
            return ResponseModel.success(
                data=result,
                message="Kết nối Elasticsearch thành công"
            )
        else:
            return ResponseModel(
                success=False,
                data=result,
                message="Không thể kết nối Elasticsearch"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi kiểm tra kết nối: {str(e)}"
        )


@router.post("/sync-all", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_all",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_all_data_to_elasticsearch(
    background_tasks: BackgroundTasks,
    run_in_background: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ TẤT CẢ dữ liệu từ PostgreSQL sang Elasticsearch.
    
    Bao gồm:
    - Products (thiết bị người dùng)
    - Accessories (linh kiện)
    - Services (dịch vụ)
    
    Sử dụng khi nghi ngờ dữ liệu bị lệch giữa PostgreSQL và Elasticsearch.
    
    Args:
        run_in_background: Nếu True, chạy nền và trả về ngay. Nếu False, chờ hoàn thành.
    """
    if run_in_background:
        async def _background_sync(user: User):
            from app.database.database import async_session
            async with async_session() as db:
                await DataSyncService.sync_all_data_to_es(db, user)
        
        background_tasks.add_task(_background_sync, current_user)
        return ResponseModel.success(
            data={"status": "started"},
            message="Đã bắt đầu đồng bộ dữ liệu ở chế độ nền. Vui lòng đợi vài phút."
        )
    else:
        try:
            result = await DataSyncService.sync_all_data_to_es(db, current_user)
            return ResponseModel.success(
                data=result,
                message=f"Đồng bộ hoàn tất: {result['total_success']} thành công, {result['total_failed']} thất bại"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi đồng bộ: {str(e)}"
            )


@router.post("/sync-products", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_products",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_products_to_elasticsearch(
    background_tasks: BackgroundTasks,
    run_in_background: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ dữ liệu PRODUCTS (thiết bị) từ PostgreSQL sang Elasticsearch.
    """
    if run_in_background:
        async def _background_sync(user: User):
            from app.database.database import async_session
            async with async_session() as db:
                await DataSyncService.sync_all_products_to_es(db, user)
        
        background_tasks.add_task(_background_sync, current_user)
        return ResponseModel.success(
            data={"status": "started"},
            message="Đã bắt đầu đồng bộ sản phẩm ở chế độ nền."
        )
    else:
        try:
            result = await DataSyncService.sync_all_products_to_es(db, current_user)
            return ResponseModel.success(
                data=result,
                message=f"Đồng bộ sản phẩm hoàn tất: {result['success_count']} thành công, {result['failed_count']} thất bại"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi đồng bộ sản phẩm: {str(e)}"
            )


@router.post("/sync-accessories", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_accessories",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_accessories_to_elasticsearch(
    background_tasks: BackgroundTasks,
    run_in_background: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ dữ liệu ACCESSORIES (linh kiện) từ PostgreSQL sang Elasticsearch.
    """
    if run_in_background:
        async def _background_sync(user: User):
            from app.database.database import async_session
            async with async_session() as db:
                await DataSyncService.sync_all_accessories_to_es(db, user)
        
        background_tasks.add_task(_background_sync, current_user)
        return ResponseModel.success(
            data={"status": "started"},
            message="Đã bắt đầu đồng bộ linh kiện ở chế độ nền."
        )
    else:
        try:
            result = await DataSyncService.sync_all_accessories_to_es(db, current_user)
            return ResponseModel.success(
                data=result,
                message=f"Đồng bộ linh kiện hoàn tất: {result['success_count']} thành công, {result['failed_count']} thất bại"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi đồng bộ linh kiện: {str(e)}"
            )


@router.post("/sync-services", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_services",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_services_to_elasticsearch(
    background_tasks: BackgroundTasks,
    run_in_background: bool = True,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ dữ liệu SERVICES (dịch vụ) từ PostgreSQL sang Elasticsearch.
    """
    if run_in_background:
        async def _background_sync(user: User):
            from app.database.database import async_session
            async with async_session() as db:
                await DataSyncService.sync_all_services_to_es(db, user)
        
        background_tasks.add_task(_background_sync, current_user)
        return ResponseModel.success(
            data={"status": "started"},
            message="Đã bắt đầu đồng bộ dịch vụ ở chế độ nền."
        )
    else:
        try:
            result = await DataSyncService.sync_all_services_to_es(db, current_user)
            return ResponseModel.success(
                data=result,
                message=f"Đồng bộ dịch vụ hoàn tất: {result['success_count']} thành công, {result['failed_count']} thất bại"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Lỗi đồng bộ dịch vụ: {str(e)}"
            )


@router.post("/sync-product/{product_id}", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_single_product",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_single_product(
    product_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ một sản phẩm cụ thể sang Elasticsearch.
    """
    try:
        success, error = await DataSyncService.sync_single_product_to_es(
            db, product_id, current_user
        )
        if success:
            return ResponseModel.success(
                data={"synced": True, "product_id": str(product_id)},
                message="Đồng bộ sản phẩm thành công"
            )
        else:
            return ResponseModel(
                success=False,
                data={"synced": False, "product_id": str(product_id), "error": error},
                message=f"Đồng bộ sản phẩm thất bại: {error}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi đồng bộ sản phẩm: {str(e)}"
        )


@router.post("/sync-accessory/{accessory_id}", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_single_accessory",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_single_accessory(
    accessory_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ một linh kiện cụ thể sang Elasticsearch.
    """
    try:
        success, error = await DataSyncService.sync_single_accessory_to_es(
            db, accessory_id, current_user
        )
        if success:
            return ResponseModel.success(
                data={"synced": True, "accessory_id": str(accessory_id)},
                message="Đồng bộ linh kiện thành công"
            )
        else:
            return ResponseModel(
                success=False,
                data={"synced": False, "accessory_id": str(accessory_id), "error": error},
                message=f"Đồng bộ linh kiện thất bại: {error}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi đồng bộ linh kiện: {str(e)}"
        )


@router.post("/sync-service/{service_id}", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.sync_single_service",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def sync_single_service(
    service_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ một dịch vụ cụ thể sang Elasticsearch.
    """
    try:
        success, error = await DataSyncService.sync_single_service_to_es(
            db, service_id, current_user
        )
        if success:
            return ResponseModel.success(
                data={"synced": True, "service_id": str(service_id)},
                message="Đồng bộ dịch vụ thành công"
            )
        else:
            return ResponseModel(
                success=False,
                data={"synced": False, "service_id": str(service_id), "error": error},
                message=f"Đồng bộ dịch vụ thất bại: {error}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi đồng bộ dịch vụ: {str(e)}"
        )


@router.delete("/clear-es-data", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.clear_es_data",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def clear_elasticsearch_data(
    confirm: bool = False,
    current_user: User = Depends(get_current_user)
):
    """
    XÓA TẤT CẢ dữ liệu Elasticsearch của user.
    
    CẢNH BÁO: Thao tác này không thể hoàn tác!
    Cần truyền confirm=true để xác nhận.
    """
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vui lòng xác nhận bằng cách truyền confirm=true"
        )
    
    try:
        result = await DataSyncService.clear_all_es_data(current_user)
        return ResponseModel.success(
            data=result,
            message="Đã xóa tất cả dữ liệu Elasticsearch của bạn"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi xóa dữ liệu: {str(e)}"
        )


@router.post("/rebuild-es-data", response_model=ResponseModel)
@with_log_context(
    event_action="data_sync.rebuild_es_data",
    event_category="data_sync",
    source_layer="controller",
    source_controller="data_sync_controller",
)
async def rebuild_elasticsearch_data(
    background_tasks: BackgroundTasks,
    confirm: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    XÓA và TẠO LẠI tất cả dữ liệu Elasticsearch từ PostgreSQL.
    
    Quy trình:
    1. Xóa tất cả dữ liệu ES hiện tại của user
    2. Đồng bộ lại toàn bộ từ PostgreSQL
    
    CẢNH BÁO: Thao tác này có thể mất vài phút!
    Cần truyền confirm=true để xác nhận.
    """
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vui lòng xác nhận bằng cách truyền confirm=true"
        )
    
    async def _background_rebuild(user: User):
        from app.database.database import async_session
        # Step 1: Clear all ES data
        await DataSyncService.clear_all_es_data(user)
        # Step 2: Resync from PostgreSQL
        async with async_session() as db:
            await DataSyncService.sync_all_data_to_es(db, user)
    
    background_tasks.add_task(_background_rebuild, current_user)
    
    return ResponseModel.success(
        data={"status": "started"},
        message="Đã bắt đầu xây dựng lại dữ liệu Elasticsearch. Vui lòng đợi vài phút."
    )
