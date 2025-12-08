import httpx
import json
import logging
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.product_component import ProductComponent
from app.models.user import User
from app.repositories.product_component_repository import ProductComponentRepository
from app.configs.settings import settings

# Setup logger
logger = logging.getLogger(__name__)

class ChatbotSyncService:
    """
    Service để đồng bộ dữ liệu với ChatbotCustom
    """
    
    CHATBOT_CUSTOM_API_BASE_URL =  settings.CHATBOT_CUSTOM_API_BASE_URL
    
    @classmethod
    async def sync_product_component(cls, db: AsyncSession, component_id: str, user: User, action: str = "create"):
        """
        Đồng bộ product component với ChatbotCustom
        action: "create", "update", "delete"
        """
        try:
            if action in ["create", "update"]:
                # Lấy thông tin component kèm kiểm tra quyền sở hữu theo user
                component = await ProductComponentRepository.get_by_id_for_user(db, component_id, user.id)
                if not component:
                    return False
                
                # Chuyển đổi dữ liệu sang format phù hợp với ChatbotCustom
                component_data = cls._format_component_data(component)
                
                async with httpx.AsyncClient(timeout=10.0) as client:
                    url = f"{cls.CHATBOT_CUSTOM_API_BASE_URL}/insert-accessory-embed-row/{user.id}"
                    logger.info(f"Gọi API ChatbotCustom: {url}")
                    
                    response = await client.post(
                        url,
                        json=component_data,
                        headers={
                            "Content-Type": "application/json"
                        }
                    )
                    
                    logger.info(f"Response từ ChatbotCustom: {response.status_code}")
                    return response.status_code == 200
                    
            elif action == "delete":
                async with httpx.AsyncClient(timeout=10.0) as client:
                    url = f"{cls.CHATBOT_CUSTOM_API_BASE_URL}/product/{user.id}/{component_id}"
                    logger.info(f"Gọi API ChatbotCustom để xóa: {url}")
                    
                    response = await client.delete(url)
                    
                    logger.info(f"Response từ ChatbotCustom (xóa): {response.status_code}")
                    return response.status_code == 200
                    
        except Exception as e:
            logger.error(f"Lỗi đồng bộ với ChatbotCustom: {e}")
            return False
    
    @classmethod
    async def sync_all_user_components(cls, db: AsyncSession, user: User):
        """
        Đồng bộ toàn bộ product components của user với ChatbotCustom
        """
        try:
            # Lấy tất cả components của user
            components, _ = await ProductComponentRepository.get_all(db, skip=0, limit=10000, user_id=user.id)
            
            # Format dữ liệu
            components_data = [cls._format_component_data(comp) for comp in components]
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                url = f"{cls.CHATBOT_CUSTOM_API_BASE_URL}/products/bulk/{user.id}"
                logger.info(f"Gọi API ChatbotCustom bulk sync: {url}")
                
                response = await client.post(
                    url,
                    json=components_data
                )
                
                logger.info(f"Response từ ChatbotCustom bulk sync: {response.status_code}")
                return response.status_code == 200
                
        except Exception as e:
            logger.error(f"Lỗi đồng bộ bulk với ChatbotCustom: {e}")
            return False
    
    @classmethod
    async def sync_excel_import_to_chatbot(cls, db: AsyncSession, user: User, file_content: bytes):
        """
        Đồng bộ dữ liệu từ file Excel import với ChatbotCustom
        Sử dụng API /insert-product/{customer_id} để upload file Excel
        """
        try:
            logger.info(f"Bắt đầu đồng bộ Excel import với ChatbotCustom cho user: {user.email}")
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Sử dụng API insert-product để upload file Excel
                url = f"{cls.CHATBOT_CUSTOM_API_BASE_URL}/insert-product/{user.id}"
                logger.info(f"Gọi API ChatbotCustom Excel import: {url}")
                
                # Tạo form data với file Excel
                files = {"file": ("import.xlsx", file_content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                
                response = await client.post(
                    url,
                    files=files
                )
                
                logger.info(f"Response từ ChatbotCustom Excel import: {response.status_code}")
                
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(f"Excel import thành công: {response_data}")
                    return True, response_data
                else:
                    logger.error(f"Excel import thất bại: {response.status_code} - {response.text}")
                    return False, f"HTTP {response.status_code}: {response.text}"
                    
        except Exception as e:
            error_msg = f"Lỗi đồng bộ Excel import với ChatbotCustom: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
    
    @classmethod
    def _format_component_data(cls, component: ProductComponent) -> dict:
        """
        Chuyển đổi ProductComponent sang format cho ChatbotCustom
        """
        # Parse properties JSON nếu có
        properties_str = ""
        if component.properties:
            try:
                properties_data = json.loads(component.properties)
                if isinstance(properties_data, list):
                    properties_str = ", ".join(str(item) for item in properties_data)
                else:
                    properties_str = str(properties_data)
            except (json.JSONDecodeError, TypeError):
                properties_str = str(component.properties)
        
        # Format avatar_images thành string
        avatar_images_str = ""
        if component.product_photo:
            avatar_images_str = component.product_photo
        
        return {
            "product_code": component.product_code,
            "product_name": component.product_name,
            "category": component.category,
            "properties": properties_str,
            "lifecare_price": float(component.amount) if component.amount else None,
            "sale_price": float(component.wholesale_price) if component.wholesale_price else None,
            "trademark": component.trademark,
            "guarantee": None,  # Không có trong ProductComponent
            "inventory": component.stock or 0,
            "specifications": component.description,  # Map description vào specifications
            "avatar_images": avatar_images_str,
            "link_accessory": component.product_link
        } 