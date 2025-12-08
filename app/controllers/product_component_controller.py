from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import uuid
import io
from typing import Optional

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.dto.product_component_dto import ProductComponentCreate, ProductComponentUpdate, ProductComponentRead, PaginatedProductComponentResponse
from app.services.product_component_service import ProductComponentService
from app.models.user import User
from app.services.chatbot_service import ChatbotService
from app.exceptions.api_exceptions import NotFoundException
from app.services.excel_service import ExcelService
from fastapi import File, UploadFile
from app.services.chatbot_sync_service import ChatbotSyncService
from app.services.api_data_service import ApiDataService
from app.database.database import async_session
from app.repositories.user_sync_url_repository import UserSyncUrlRepository
from app.logging.decorators import with_log_context
from app.tasks.elasticsearch_tasks import es_bulk_upsert_accessories
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

# Static routes should come before parameterized routes to avoid 422 path-matching against UUID
@router.get("/deleted-today", response_model=list[ProductComponentRead])
@with_log_context(
    event_action="product_component.deleted_today.list",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def get_deleted_product_components_today(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy danh sách linh kiện đã xóa trong ngày hôm nay của người dùng hiện tại."""
    try:
        return await ProductComponentService.get_deleted_today(db=db, user_id=current_user.id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.post("/restore-all-today", response_model=dict)
@with_log_context(
    event_action="product_component.deleted_today.restore_all",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def restore_all_deleted_product_components_today(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Khôi phục tất cả linh kiện đã xóa trong ngày hôm nay của người dùng hiện tại và đồng bộ chatbot."""
    try:
        result = await ProductComponentService.restore_all_deleted_today(db=db, user_id=current_user.id)
        restored_ids = result.get("restored_ids", [])
        # Enqueue background sync for each restored component
        for component_id in restored_ids:
            background_tasks.add_task(ChatbotService.add_product_component, component_id, current_user)
            background_tasks.add_task(ChatbotService.add_product_component_to_custom, component_id, current_user)
        return result
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.post("/", response_model=ProductComponentRead, status_code=status.HTTP_201_CREATED)
@with_log_context(
    event_action="product_component.create",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def create_product_component(
    data: ProductComponentCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Tạo một thành phần sản phẩm mới."""
    # Cho phép người dùng đã xác thực tạo thành phần sản phẩm
    # Gán user_id từ người dùng hiện tại
    data.user_id = current_user.id
    
    # Tạo linh kiện và đồng bộ với ChatbotCustom
    created_component = await ProductComponentService.create_product_component_with_sync(db=db, data=data, user=current_user)
    
    # Thêm background task để đồng bộ với ChatbotMobileStore (Elasticsearch)
    background_tasks.add_task(ChatbotService.add_product_component, created_component.id, current_user)
    
    # Thêm background task để đồng bộ với ChatbotCustom (Hoàng Mai)
    background_tasks.add_task(ChatbotService.add_product_component_to_custom, created_component.id, current_user)
    
    return created_component

@router.get("/export", response_class=StreamingResponse)
@with_log_context(
    event_action="product_component.export",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def export_product_components(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xuất danh sách thành phần sản phẩm ra file Excel."""
    # Lấy tất cả thành phần sản phẩm của người dùng hiện tại
    product_components = await ProductComponentService.get_all_product_components(db=db, skip=0, limit=10000, user_id=current_user.id)
    
    # Xuất file Excel
    excel_data = await ExcelService.export_product_components(db=db, product_components=product_components)
    
    # Trả về file Excel
    return StreamingResponse(
        io.BytesIO(excel_data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=linh_kien.xlsx"}
    )

@router.get("/export-sample", response_class=StreamingResponse)
@with_log_context(
    event_action="product_component.export_sample",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def export_sample_excel(
    current_user: User = Depends(get_current_user)
):
    """Xuất file Excel mẫu để import linh kiện."""
    # Tạo file Excel mẫu với các cột tiếng Việt
    excel_data = await ExcelService.export_sample_product_components()
    
    # Trả về file Excel mẫu
    return StreamingResponse(
        io.BytesIO(excel_data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=mau_linh_kien.xlsx"}
    )

@router.post("/import")
@with_log_context(
    event_action="product_component.import",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def import_product_components(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Nhập danh sách thành phần sản phẩm từ file Excel."""
    # Đọc nội dung file
    file_content = await file.read()
    
    # Nhập dữ liệu từ file Excel
    import_result = await ExcelService.import_product_components(
        db=db, 
        file_content=file_content, 
        user_id=current_user.id,
        background_tasks=background_tasks,
        current_user=current_user
    )
    
    return import_result

@router.get("/filter-options", response_model=dict)
@with_log_context(
    event_action="product_component.filter_options",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def get_filter_options(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy các tùy chọn bộ lọc cho thành phần sản phẩm."""
    return await ProductComponentService.get_filter_options(db=db, user_id=current_user.id)

@router.delete("/bulk", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="product_component.bulk_delete",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def bulk_delete_product_components(
    background_tasks: BackgroundTasks,
    product_component_ids: list[uuid.UUID] = Query(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xóa hàng loạt thành phần sản phẩm."""
    if not product_component_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vui lòng cung cấp danh sách ID để xóa"
        )
    
    # Lấy thông tin các thành phần sản phẩm trước khi xóa để đồng bộ
    product_components = []
    for product_component_id in product_component_ids:
        product_component = await ProductComponentService.get_product_component(db=db, product_component_id=product_component_id, user_id=current_user.id)
        if product_component.user_id != current_user.id and not current_user.is_superuser:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Bạn không có quyền xóa thành phần sản phẩm với ID {product_component_id}"
            )
        product_components.append(product_component)
    
    # Xóa hàng loạt linh kiện và đồng bộ với ChatbotCustom
    deleted_count = await ProductComponentService.bulk_delete_product_components_with_sync(
        db=db, 
        product_component_ids=product_component_ids, 
        user=current_user
    )
    
    # Đồng bộ xóa hàng loạt trên các hệ thống khác
    product_codes_for_mobile_store = [pc.product_code for pc in product_components if pc.product_code]
    if product_codes_for_mobile_store:
        background_tasks.add_task(ChatbotService.bulk_delete_product_components, product_codes_for_mobile_store, current_user)

    for component in product_components:
        background_tasks.add_task(ChatbotSyncService.sync_product_component, None, current_user, "delete", component.id)
    
    return {
        "message": f"Đã xóa thành công {deleted_count} thành phần sản phẩm",
        "deleted_count": deleted_count
    }

@router.delete("/all", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="product_component.delete_all",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def delete_all_product_components(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xóa tất cả thành phần sản phẩm của người dùng hiện tại."""
    
    # Lấy tất cả product_codes trước khi xóa để đồng bộ
    all_components = await ProductComponentService.get_all_product_components(
        db=db, 
        skip=0, 
        limit=10000, # Giới hạn lớn để lấy hết
        user_id=current_user.id
    )
    product_codes = [pc.product_code for pc in all_components['data'] if pc.product_code]

    deleted_count = await ProductComponentService.delete_all_product_components(db=db, user_id=current_user.id)

    # Đồng bộ xóa tất cả trên các hệ thống khác
    if product_codes:
        background_tasks.add_task(ChatbotService.delete_all_product_components, current_user)
        # For ChatbotCustom, we still need to call sync for each item to be deleted
        all_component_ids = [pc.id for pc in all_components['data']]
        for component_id in all_component_ids:
            background_tasks.add_task(ChatbotSyncService.sync_product_component, None, current_user, "delete", component_id)

    return {
        "message": f"Đã xóa thành công {deleted_count} thành phần sản phẩm",
        "deleted_count": deleted_count
    }

@router.get("/{product_component_id}", response_model=ProductComponentRead)
@with_log_context(
    event_action="product_component.get",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def get_product_component(
    product_component_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy thông tin thành phần sản phẩm theo ID."""
    # Cho phép người dùng đã xác thực xem thành phần sản phẩm
    product_component = await ProductComponentService.get_product_component(db=db, product_component_id=product_component_id, user_id=current_user.id)
    # Kiểm tra xem người dùng có quyền xem thành phần sản phẩm này không
    if product_component.user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền xem thành phần sản phẩm này"
        )
    return product_component

@router.put("/{product_component_id}", response_model=ProductComponentRead)
@with_log_context(
    event_action="product_component.update",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def update_product_component(
    product_component_id: uuid.UUID,
    data: ProductComponentUpdate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Cập nhật thông tin thành phần sản phẩm."""
    # Cho phép người dùng đã xác thực cập nhật thành phần sản phẩm của họ
    product_component = await ProductComponentService.get_product_component(db=db, product_component_id=product_component_id, user_id=current_user.id)
    if product_component.user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền cập nhật thành phần sản phẩm này"
        )
    
    # Cập nhật linh kiện và đồng bộ với ChatbotCustom
    updated_component = await ProductComponentService.update_product_component_with_sync(db=db, product_component_id=product_component_id, data=data, user=current_user)
    
    # Thêm background task để đồng bộ với ChatbotMobileStore (Elasticsearch)
    background_tasks.add_task(ChatbotService.update_product_component, updated_component.id, current_user)
    
    # Thêm background task để đồng bộ với ChatbotCustom (Hoàng Mai)
    background_tasks.add_task(ChatbotService.update_product_component_in_custom, updated_component.id, current_user)
    
    return updated_component

@router.get("/", response_model=dict)
@with_log_context(
    event_action="product_component.list",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def get_all_product_components(
    request: Request,
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    category: Optional[str] = None,
    trademark: Optional[str] = None,
    price_range_min: Optional[float] = None,
    price_range_max: Optional[float] = None,
    sort_by: Optional[str] = None,
    sort_order: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Lấy danh sách tất cả thành phần sản phẩm với phân trang, tìm kiếm và filter."""
    # Tạo dict filters từ các tham số query
    filters = {}
    if category:
        filters['category'] = category
    if trademark:
        filters['trademark'] = trademark
    
    # Xử lý các property filters (property_COMBO, property_RAM, etc.)
    for key, value in request.query_params.items():
        if key.startswith('property_') and value:
            filters[key] = value
    
    if price_range_min is not None:
        filters['price_range_min'] = price_range_min
    if price_range_max is not None:
        filters['price_range_max'] = price_range_max
    
    # Cho phép người dùng đã xác thực xem danh sách thành phần sản phẩm của họ
    return await ProductComponentService.get_all_product_components(
        db=db, 
        skip=skip, 
        limit=limit, 
        search=search,
        sort_by=sort_by, 
        sort_order=sort_order, 
        user_id=current_user.id,
        filters=filters if filters else None
    )

@router.delete("/{product_component_id}", status_code=status.HTTP_204_NO_CONTENT)
@with_log_context(
    event_action="product_component.delete",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def delete_product_component(
    product_component_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Xóa mềm thành phần sản phẩm."""
    # Cho phép người dùng đã xác thực xóa thành phần sản phẩm của họ
    product_component = await ProductComponentService.get_product_component(db=db, product_component_id=product_component_id, user_id=current_user.id)
    if product_component.user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền xóa thành phần sản phẩm này"
        )
    
    # Lưu product_code trước khi xóa để sử dụng trong background task
    product_code = product_component.product_code
    
    # Xóa mềm linh kiện và đồng bộ với ChatbotCustom
    success = await ProductComponentService.delete_product_component_with_sync(db=db, product_component_id=product_component_id, user=current_user)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Không tìm thấy thành phần sản phẩm"
        )
    
    # Thêm background task để đồng bộ với ChatbotMobileStore (Elasticsearch)
    if product_code:
        background_tasks.add_task(ChatbotService.delete_product_component, product_code, current_user)
    
    # Thêm background task để đồng bộ với ChatbotCustom (Hoàng Mai)
    background_tasks.add_task(ChatbotService.delete_product_component_from_custom, product_code, current_user)
    
    return {"message": "Thành phần sản phẩm đã được xóa mềm thành công"}

@router.post("/{product_component_id}/restore", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="product_component.restore",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def restore_product_component(
    product_component_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Khôi phục thành phần sản phẩm đã bị xóa mềm."""
    try:
        success = await ProductComponentService.restore_product_component(db=db, product_component_id=product_component_id)
        if success:
            # Lấy thông tin component để đồng bộ lại với chatbot
            product_component = await ProductComponentService.get_product_component(db=db, product_component_id=product_component_id)
            if product_component.user_id != current_user.id and not current_user.is_superuser:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Bạn không có quyền khôi phục thành phần sản phẩm này"
                )
            
            # Thêm background task để đồng bộ với ChatbotMobileStore (Elasticsearch)
            background_tasks.add_task(ChatbotService.add_product_component, product_component_id, current_user)
            
            # Thêm background task để đồng bộ với ChatbotCustom (Hoàng Mai)
            background_tasks.add_task(ChatbotService.add_product_component_to_custom, product_component_id, current_user)
            
            return {"message": "Khôi phục thành phần sản phẩm thành công"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Không tìm thấy thành phần sản phẩm để khôi phục"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

async def background_master_sync(user: User, is_today: bool, background_tasks: BackgroundTasks):
    """
    Background task tổng hợp:
    1. Đồng bộ dữ liệu từ API bên ngoài.
    2. Xuất tất cả dữ liệu ra file Excel.
    3. Đồng bộ file Excel với các dịch vụ chatbot.
    """
    try:
        async with async_session() as db:
            # 1. Đồng bộ dữ liệu từ API, không trigger sync lẻ
            await ApiDataService.sync_product_components_from_api(
                db=db,
                user_id=user.id,
                background_tasks=background_tasks,  # Vẫn cần cho xử lý nội bộ của service
                current_user=user,
                is_today=is_today,
                sync_individually=False  # Quan trọng: không đồng bộ lẻ
            )

            # 2. Lấy tất cả linh kiện sau khi đã đồng bộ
            all_components_response = await ProductComponentService.get_all_product_components(db, user_id=user.id, limit=100000)
            
            if all_components_response and all_components_response['data']:
                components_data = all_components_response['data']
                
                # 3. Đồng bộ với Elasticsearch via Celery task
                try:
                    es_accessories = []
                    for comp in components_data:
                        es_accessories.append({
                            "accessory_code": comp.get("product_code") if isinstance(comp, dict) else getattr(comp, "product_code", None),
                            "accessory_name": comp.get("product_name") if isinstance(comp, dict) else getattr(comp, "product_name", None),
                            "gia_ban": comp.get("amount") if isinstance(comp, dict) else getattr(comp, "amount", None),
                            "gia_si": comp.get("wholesale_price") if isinstance(comp, dict) else getattr(comp, "wholesale_price", None),
                            "trademark": comp.get("trademark") if isinstance(comp, dict) else getattr(comp, "trademark", None),
                            "guarantee": comp.get("guarantee") if isinstance(comp, dict) else getattr(comp, "guarantee", None),
                            "stock": comp.get("stock") if isinstance(comp, dict) else getattr(comp, "stock", None),
                            "description": comp.get("description") if isinstance(comp, dict) else getattr(comp, "description", None),
                            "product_photo": comp.get("product_photo") if isinstance(comp, dict) else getattr(comp, "product_photo", None),
                            "product_link": comp.get("product_link") if isinstance(comp, dict) else getattr(comp, "product_link", None),
                            "category": comp.get("category") if isinstance(comp, dict) else getattr(comp, "category", None),
                        })
                    if es_accessories:
                        es_bulk_upsert_accessories.delay(
                            customer_id=str(user.id),
                            accessories=es_accessories,
                            with_embedding=True,
                            api_key=getattr(user, "gemini_api_key", None),
                        )
                        logger.info(f"Triggered ES bulk upsert for {len(es_accessories)} accessories of user {user.id}")
                except Exception as es_err:
                    logger.warning(f"Failed to trigger ES sync in background_master_sync: {es_err}")

    except Exception as e:
        logger.error(f"Lỗi background task master sync: {e}")


async def background_export_and_sync_chatbots(user: User):
    """
    Background task để:
    1. Lấy tất cả linh kiện từ DB.
    2. Đồng bộ với Elasticsearch.
    """
    try:
        async with async_session() as db:
            # 1. Lấy tất cả linh kiện (đã được đồng bộ ở bước trước)
            all_components_response = await ProductComponentService.get_all_product_components(db, user_id=user.id, limit=100000)
            
            if all_components_response and all_components_response['data']:
                components_data = all_components_response['data']
                
                # 2. Đồng bộ với Elasticsearch via Celery task
                try:
                    es_accessories = []
                    for comp in components_data:
                        es_accessories.append({
                            "accessory_code": comp.get("product_code") if isinstance(comp, dict) else getattr(comp, "product_code", None),
                            "accessory_name": comp.get("product_name") if isinstance(comp, dict) else getattr(comp, "product_name", None),
                            "gia_ban": comp.get("amount") if isinstance(comp, dict) else getattr(comp, "amount", None),
                            "gia_si": comp.get("wholesale_price") if isinstance(comp, dict) else getattr(comp, "wholesale_price", None),
                            "trademark": comp.get("trademark") if isinstance(comp, dict) else getattr(comp, "trademark", None),
                            "guarantee": comp.get("guarantee") if isinstance(comp, dict) else getattr(comp, "guarantee", None),
                            "stock": comp.get("stock") if isinstance(comp, dict) else getattr(comp, "stock", None),
                            "description": comp.get("description") if isinstance(comp, dict) else getattr(comp, "description", None),
                            "product_photo": comp.get("product_photo") if isinstance(comp, dict) else getattr(comp, "product_photo", None),
                            "product_link": comp.get("product_link") if isinstance(comp, dict) else getattr(comp, "product_link", None),
                            "category": comp.get("category") if isinstance(comp, dict) else getattr(comp, "category", None),
                        })
                    if es_accessories:
                        es_bulk_upsert_accessories.delay(
                            customer_id=str(user.id),
                            accessories=es_accessories,
                            with_embedding=True,
                            api_key=getattr(user, "gemini_api_key", None),
                        )
                        logger.info(f"Triggered ES bulk upsert for {len(es_accessories)} accessories of user {user.id}")
                except Exception as es_err:
                    logger.warning(f"Failed to trigger ES sync: {es_err}")
    except Exception as e:
        logger.error(f"Lỗi background task export and sync chatbots: {e}")


@router.post("/sync-from-api", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="product_component.sync_from_api",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def sync_from_api_and_export(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ dữ liệu từ API vào hệ thống, sau đó chạy nền xuất Excel và đồng bộ chatbots.
    """
    try:
        # Bước 1: Đồng bộ dữ liệu vào DB (chờ cho xong)
        # BẮT BUỘC dùng URL đồng bộ riêng của người dùng
        user_sync = await UserSyncUrlRepository.get_by_user_id(db, current_user.id, only_active=True, type_url="component")
        if not user_sync or not user_sync.url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Vui lòng cấu hình Sync URL cá nhân trước khi đồng bộ."
            )
        api_url_override = user_sync.url
        sync_result = await ApiDataService.sync_product_components_from_api(
            db=db,
            user_id=current_user.id,
            background_tasks=background_tasks,
            current_user=current_user,
            is_today=False,
            sync_individually=False,
            api_url_override=api_url_override 
        )

        # Bước 2: Chạy nền phần export và đồng bộ chatbot (tạo session riêng trong task)
        background_tasks.add_task(background_export_and_sync_chatbots, current_user)

        return {
            "message": "Đồng bộ dữ liệu vào hệ thống thành công. Tác vụ xuất file và đồng bộ với chatbot đã được lên lịch.",
            "sync_details": sync_result
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi đồng bộ dữ liệu: {str(e)}"
        )


@router.post("/sync-now", status_code=status.HTTP_200_OK)
@with_log_context(
    event_action="product_component.sync_today_from_api",
    event_category="product_component",
    source_layer="controller",
    source_controller="product_component_controller",
)
async def sync_now_from_api_and_export(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Đồng bộ dữ liệu hôm nay từ API vào hệ thống, sau đó chạy nền xuất Excel và đồng bộ chatbots.
    """
    try:
        # Bước 1: Đồng bộ dữ liệu vào DB (chờ cho xong)
        # BẮT BUỘC dùng URL đồng bộ riêng của người dùng
        user_sync = await UserSyncUrlRepository.get_by_user_id(db, current_user.id, only_active=True, type_url="component")
        if not user_sync or not user_sync.url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Vui lòng cấu hình Sync URL cá nhân trước khi đồng bộ."
            )
        api_url_override = user_sync.url
        sync_result = await ApiDataService.sync_product_components_from_api(
            db=db,
            user_id=current_user.id,
            background_tasks=background_tasks,
            current_user=current_user,
            is_today=True,
            sync_individually=False,
            api_url_override=api_url_override
        )

        # Bước 2: Chạy nền phần export và đồng bộ chatbot (tạo session riêng trong task)
        background_tasks.add_task(background_export_and_sync_chatbots, current_user)

        return {
            "message": "Đồng bộ dữ liệu hôm nay vào hệ thống thành công. Tác vụ xuất file và đồng bộ với chatbot đã được lên lịch.",
            "sync_details": sync_result
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi đồng bộ dữ liệu: {str(e)}"
        )


