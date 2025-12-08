import httpx
import os
import json
import uuid
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import BackgroundTasks, HTTPException
import logging

from app.dto.product_component_dto import ProductComponentCreate
from app.services.product_component_service import ProductComponentService
from app.services.chatbot_service import ChatbotService
from app.models.user import User
from app.repositories.product_component_repository import ProductComponentRepository

logger = logging.getLogger(__name__)

class ApiDataService:
    """Service để xử lý đồng bộ dữ liệu từ API bên ngoài."""
    
    @staticmethod
    def _safe_str(value) -> str:
        """Chuyển đổi giá trị thành string một cách an toàn, xử lý UUID objects."""
        if value is None:
            return ""
        elif isinstance(value, uuid.UUID):
            return str(value)
        elif isinstance(value, str):
            return value.strip()
        else:
            return str(value).strip() if str(value) else ""
    
    @staticmethod
    def _safe_float(value, default=None):
        """Chuyển đổi giá trị thành float một cách an toàn."""
        if value is None:
            return default
        try:
            if isinstance(value, uuid.UUID):
                return default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def _safe_int(value, default=0):
        """Chuyển đổi giá trị thành int một cách an toàn."""
        if value is None:
            return default
        try:
            if isinstance(value, uuid.UUID):
                return default
            return int(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def _clean_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean data to ensure all values are JSON serializable and safe for string operations."""
        clean_data = {}
        for key, value in data.items():
            if value is None:
                clean_data[key] = None
            # Giữ nguyên UUID objects, không chuyển thành string
            elif isinstance(value, uuid.UUID):
                clean_data[key] = value
            elif isinstance(value, (str, int, float, bool)):
                clean_data[key] = value
            elif isinstance(value, list):
                clean_data[key] = value
            elif isinstance(value, dict):
                clean_data[key] = value
            else:
                # Convert any other types (including UUID) to string
                clean_data[key] = str(value)
        return clean_data
    
    @staticmethod
    async def sync_product_components_from_api(
        db: AsyncSession,
        user_id: str,
        background_tasks: BackgroundTasks,
        current_user: User,
        is_today: bool = False,
        sync_individually: bool = True,
        api_url_override: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Đồng bộ dữ liệu linh kiện từ API bên ngoài.
        
        Args:
            db: Database session
            user_id: ID của user hiện tại
            background_tasks: Background tasks để đồng bộ
            current_user: User hiện tại
            is_today: Chỉ đồng bộ sản phẩm hôm nay
            sync_individually: Đồng bộ từng sản phẩm với chatbot
            
        Returns:
            Dict chứa thống kê đồng bộ
        """
        # Lấy URL API từ environment hoặc override theo user
        api_url = api_url_override or os.getenv("API_DATA")
        if not api_url:
            raise HTTPException(
                status_code=500,
                detail="Không tìm thấy cấu hình API_DATA trong environment"
            )
        
        # Thêm param is_today nếu cần
        if is_today:
            api_url = f"{api_url}&is_today=true" if ("?" in api_url) else f"{api_url}?is_today=true"
            
        try:
            # Gọi API để lấy dữ liệu
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(api_url)
                response.raise_for_status()
                api_data = response.json()
            
            # Khởi tạo counters
            total_synced = 0
            total_created = 0
            total_updated = 0
            total_skipped = 0
            
            # Xử lý từng sản phẩm
            if isinstance(api_data, list):
                products = api_data
            else:
                products = api_data.get('data', []) if isinstance(api_data, dict) else []
            
            # Log bắt đầu sync với context
            logger.info(
                "Bắt đầu sync_product_components_from_api",
                extra={
                    "event.action": "sync.api_data.product_components",
                    "event.category": "sync",
                    "source.layer": "service",
                    "source.function": "sync_product_components_from_api",
                    "user.id": str(getattr(current_user, "id", None)) if current_user else None,
                    "api.url": api_url,
                },
            )

            for product_data in products:
                # Clean main product data first
                clean_product_data = ApiDataService._clean_data(product_data)
                
                # Xử lý sản phẩm chính
                main_result = await ApiDataService._process_single_product(
                    db=db,
                    product_data=clean_product_data,
                    user_id=user_id,
                    background_tasks=background_tasks,
                    current_user=current_user,
                    is_variant=False,
                    sync_individually=sync_individually
                )
                
                if main_result["action"] == "created":
                    total_created += 1
                elif main_result["action"] == "updated":
                    total_updated += 1
                elif main_result["action"] == "skipped":
                    total_skipped += 1
                
                total_synced += 1
                
                # Xử lý các biến thể trong list_extend
                list_extend = clean_product_data.get("list_extend", [])
                for variant_data in list_extend:
                    # Kết hợp thông tin từ sản phẩm chính (cleaned) và biến thể
                    combined_variant = {
                        **clean_product_data,  # Already cleaned main product data
                        **ApiDataService._clean_data(variant_data),  # Clean variant data
                        "parent_code": clean_product_data.get("code"),  # Lưu mã sản phẩm gốc
                        "parent_name": clean_product_data.get("name"),  # Lưu tên sản phẩm gốc
                    }
                    
                    variant_result = await ApiDataService._process_single_product(
                        db=db,
                        product_data=combined_variant,
                        user_id=user_id,
                        background_tasks=background_tasks,
                        current_user=current_user,
                        is_variant=True,
                        sync_individually=sync_individually
                    )
                    
                    if variant_result["action"] == "created":
                        total_created += 1
                    elif variant_result["action"] == "updated":
                        total_updated += 1
                    elif variant_result["action"] == "skipped":
                        total_skipped += 1
                    
                    total_synced += 1
            
            result = {
                "total_synced": total_synced,
                "total_created": total_created,
                "total_updated": total_updated,
                "total_skipped": total_skipped,
            }

            logger.info(
                "Hoàn thành sync_product_components_from_api",
                extra={
                    "event.action": "sync.api_data.product_components",
                    "event.category": "sync",
                    "source.layer": "service",
                    "source.function": "sync_product_components_from_api",
                    "user.id": str(getattr(current_user, "id", None)) if current_user else None,
                    "api.url": api_url,
                    "sync.total_synced": total_synced,
                    "sync.total_created": total_created,
                    "sync.total_updated": total_updated,
                    "sync.total_skipped": total_skipped,
                },
            )

            return result
            
        except httpx.HTTPError as e:
            logger.error(
                "Lỗi gọi API trong sync_product_components_from_api",
                exc_info=True,
                extra={
                    "event.action": "sync.api_data.product_components",
                    "event.category": "sync",
                    "source.layer": "service",
                    "source.function": "sync_product_components_from_api",
                    "user.id": str(getattr(current_user, "id", None)) if current_user else None,
                    "api.url": api_url,
                },
            )
            raise HTTPException(
                status_code=500,
                detail=f"Lỗi kết nối đến API: {str(e)}"
            )
        except Exception as e:
            logger.error(
                "Lỗi đồng bộ dữ liệu trong sync_product_components_from_api",
                exc_info=True,
                extra={
                    "event.action": "sync.api_data.product_components",
                    "event.category": "sync",
                    "source.layer": "service",
                    "source.function": "sync_product_components_from_api",
                    "user.id": str(getattr(current_user, "id", None)) if current_user else None,
                    "api.url": api_url,
                },
            )
            raise HTTPException(
                status_code=500,
                detail=f"Lỗi xử lý dữ liệu: {str(e)}"
            )
    
    @staticmethod
    async def _process_single_product(
        db: AsyncSession,
        product_data: Dict[str, Any],
        user_id: str,
        background_tasks: BackgroundTasks,
        current_user: User,
        is_variant: bool = False,
        sync_individually: bool = True
    ) -> Dict[str, str]:
        """
        Xử lý một sản phẩm đơn lẻ.
        
        Args:
            db: Database session
            product_data: Dữ liệu sản phẩm từ API
            user_id: ID của user
            background_tasks: Background tasks
            current_user: User hiện tại
            is_variant: Có phải là biến thể không
            sync_individually: Đồng bộ từng sản phẩm với chatbot
            
        Returns:
            Dict với action: "created", "updated", hoặc "skipped"
        """
        try:
            # Map dữ liệu từ API sang ProductComponentCreate
            component_data = await ApiDataService._map_api_data_to_component(
                product_data, user_id, db, is_variant
            )
            
            # Chỉ kiểm tra dựa trên product_code nếu có
            if component_data.product_code:
                # Có mã sản phẩm - kiểm tra xem đã tồn tại chưa để cập nhật
                safe_product_code = ApiDataService._safe_str(component_data.product_code)
                existing_component = await ProductComponentService.get_product_component_by_code(
                    db=db, 
                    product_code=safe_product_code, 
                    user_id=user_id
                )
                
                if existing_component:
                    # Cập nhật sản phẩm hiện có
                    from app.dto.product_component_dto import ProductComponentUpdate
                    update_data = ProductComponentUpdate(**component_data.dict(exclude_unset=True))
                    
                    updated_component = await ProductComponentService.update_product_component_with_sync(
                        db=db,
                        product_component_id=existing_component.id,
                        data=update_data,
                        user=current_user
                    )
                    
                    if sync_individually:
                        # Background tasks cho update
                        background_tasks.add_task(
                            ChatbotService.update_product_component, 
                            updated_component.id, 
                            current_user
                        )
                        background_tasks.add_task(
                            ChatbotService.update_product_component_in_custom, 
                            updated_component.id, 
                            current_user
                        )
                    
                    return {"action": "updated", "component_id": str(updated_component.id)}
            
            # Tạo sản phẩm mới (hoặc không có product_code, hoặc product_code chưa tồn tại)
            created_component = await ProductComponentService.create_product_component_with_sync(
                db=db, data=component_data, user=current_user
            )

            if sync_individually:
                # Background task cho create
                background_tasks.add_task(
                    ChatbotService.add_new_product_component, 
                    created_component.id, 
                    current_user
                )
                background_tasks.add_task(
                    ChatbotService.add_product_component_to_custom, 
                    created_component.id, 
                    current_user
                )
            
            return {"action": "created", "component_id": str(created_component.id)}
                
        except Exception as e:
            logger.error(f"Lỗi xử lý sản phẩm {product_data.get('code', 'unknown')}: {e}")
            return {"action": "skipped", "error": str(e)}
    
    @staticmethod
    async def _map_api_data_to_component(
        product_data: Dict[str, Any], 
        user_id,
        db: AsyncSession, # Thêm db session
        is_variant: bool = False
    ) -> ProductComponentCreate:
        """
        Map dữ liệu từ API sang ProductComponentCreate.
        
        Args:
            product_data: Dữ liệu từ API
            user_id: ID của user
            db: Database session
            is_variant: Có phải là biến thể không
            
        Returns:
            ProductComponentCreate object
        """
        # Use utility function to clean data
        product_data = ApiDataService._clean_data(product_data)
        # Xử lý category từ category_name
        category_name = ApiDataService._safe_str(product_data.get("category_name"))
        category = category_name.split(">>")[-1].strip() if category_name else "Khác"
        
        # Xử lý manufacturer từ manufactory_name
        manufacturer = ApiDataService._safe_str(product_data.get("manufactory_name")) or "Không xác định"
        
        # Xử lý tên sản phẩm
        product_name = ApiDataService._safe_str(product_data.get("name")) or "Sản phẩm"
        
        # Xử lý thuộc tính cho biến thể
        variant_properties = []
        if is_variant and "parent_code" in product_data:
            # Lấy tên sản phẩm gốc từ parent
            parent_name = product_data.get("parent_name", "")
            if parent_name:
                product_name = parent_name  # Sử dụng tên sản phẩm gốc
            
            # Chuyển variant name thành thuộc tính
            variant_name = ApiDataService._safe_str(product_data.get("name"))
            if ":" in variant_name:
                # Parse "MODEL:TD2 - D" thành thuộc tính
                parts = variant_name.split(":", 1)
                if len(parts) == 2:
                    # Đảm bảo property_key và property_value là string
                    property_key = str(parts[0]).strip() if parts[0] else ""
                    property_value = str(parts[1]).strip() if parts[1] else ""
                    variant_properties.append({
                        "key": property_key,
                        "values": [property_value]
                    })
            else:
                # Nếu không có ":", sử dụng toàn bộ làm thuộc tính "Variant"
                if variant_name:
                    variant_properties.append({
                        "key": "Biến thể",
                        "values": [variant_name]
                    })
        
        # Tạo properties cho database - theo format của ProductComponentsTab
        properties_list = variant_properties.copy() if variant_properties else []

        # Lấy user_id từ dữ liệu - xử lý an toàn
        if isinstance(user_id, uuid.UUID):
            user_id_uuid = user_id
        elif isinstance(user_id, str):
            user_id_uuid = user_id
        else:
            user_id_uuid = user_id
        
        # Store metadata riêng nếu cần
        if is_variant:
            description_extra = f" [Biến thể của {product_data.get('parent_code', '')}]"
        else:
            description_extra = ""
        
        # Xử lý các field có thể None
        content = ApiDataService._safe_str(product_data.get("content")) or "Không có mô tả"
        description = content + description_extra
        
        product_photo = ApiDataService._safe_str(product_data.get("image")) or None
        product_link = ApiDataService._safe_str(product_data.get("link")) or None
        guarantee = ApiDataService._safe_str(product_data.get("warranty")) or None
        
        # Kiểm tra xem linh kiện có bị trùng lặp không (dựa trên tên và thuộc tính)
        # Chỉ kiểm tra khi tạo mới (khi product_code không được cung cấp từ API)
        # TODO: Implement find_duplicate_by_name_and_properties method in ProductComponentRepository
        # if not product_data.get("code"):
        #     existing_duplicate = await ProductComponentRepository.find_duplicate_by_name_and_properties(
        #         db=db,
        #         name=product_name,
        #         properties=properties_list,
        #         user_id=user_id_uuid
        #     )
        #     if existing_duplicate:
        #         raise Exception(
        #             f"Linh kiện '{product_name}' với các thuộc tính tương tự đã tồn tại với mã sản phẩm {existing_duplicate.product_code}"
        #         )
                
        # Tạo đối tượng ProductComponentCreate
        component_data = ProductComponentCreate(
            user_id=user_id_uuid,
            product_code=ApiDataService._safe_str(product_data.get("code")) or "",
            product_name=product_name or "Sản phẩm",
            description=description,
            amount=ApiDataService._safe_float(product_data.get("price"), 0.0),
            wholesale_price=ApiDataService._safe_float(product_data.get("price_wholesale")),
            category=category,
            trademark=manufacturer,
            stock=ApiDataService._safe_int(product_data.get("quantity"), 0),
            product_photo=product_photo,
            product_link=product_link,
            guarantee=guarantee,
            properties=json.dumps(properties_list)
        )
        return component_data
