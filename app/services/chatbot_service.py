import httpx
from typing import Dict, Any, Optional
import logging
import traceback
from sqlalchemy.ext.asyncio import AsyncSession
import uuid
from fastapi import HTTPException, status

from app.models.user_device import UserDevice
from app.models.user import User
from app.models.brand import Brand
from app.repositories.device_color_repository import DeviceColorRepository
from app.repositories.device_storage_repository import DeviceStorageRepository
from app.repositories.user_device_repository import UserDeviceRepository
from app.repositories.brand_repository import BrandRepository
from app.database.database import async_session
from app.configs.settings import settings
from app.services.chatbot_sync_service import ChatbotSyncService
from app.services.elasticsearch_service import ElasticsearchService
from app.logging.logger import log_error, log_info


logger = logging.getLogger(__name__)

CHATBOT_API_BASE_URL = settings.CHATBOT_API_BASE_URL

def parse_battery_condition(condition: str) -> float:
    """
    Chuyển đổi tình trạng pin từ string (ví dụ "99%") sang float.
    """
    if condition:
        try:
            return float(condition.replace('%', '').strip())
        except (ValueError, AttributeError):
            return 0.0
    return 0.0

async def get_product_data(db: AsyncSession, user_device: UserDevice) -> Dict[str, Any]:
    """
    Chuyển đổi đối tượng UserDevice thành dictionary cho Chatbot API.
    Lấy thêm dữ liệu từ database nếu cần.
    """
    device_info = user_device.device_info
    
    available_colors = await DeviceColorRepository.get_colors_by_device_info_id(db, device_info.id)
    available_storages = await DeviceStorageRepository.get_by_device_info_id(db, device_info.id)

    return {
        "ma_san_pham": user_device.product_code,
        "model": device_info.model if device_info else None,
        "mau_sac": user_device.color.name if user_device.color else None,
        "dung_luong": str(user_device.device_storage.capacity) + "GB" if user_device.device_storage else None,
        "bao_hanh": user_device.warranty,
        "tinh_trang_may": user_device.device_condition,
        "loai_thiet_bi": user_device.device_type,
        "tinh_trang_pin": parse_battery_condition(user_device.battery_condition),
        "gia": user_device.price,
        "gia_buon": None,  # Không có trong UserDevice
        "ton_kho": user_device.inventory,
        "ghi_chu": user_device.notes,
        "ra_mat": device_info.release_date if device_info else None,
        "man_hinh": device_info.screen if device_info else None,
        "chip_ram": device_info.chip_ram if device_info else None,
        "camera": device_info.camera if device_info else None,
        "pin_mah": device_info.battery if device_info else None,
        "ket_noi_hdh": device_info.connectivity_os if device_info else None,
        "mau_sac_tieng_anh": device_info.color_english if device_info else None,
        "kich_thuoc_trong_luong": device_info.dimensions_weight if device_info else None,
        "mau_sac_available": ", ".join([c.name for c in available_colors]) if available_colors else None,
        "dung_luong_available": ", ".join([str(s.capacity) + "GB" for s in available_storages]) if available_storages else None,
    }

async def get_service_data(brand: Brand) -> Dict[str, Any]:
    """
    Chuyển đổi đối tượng Brand thành dictionary cho ServiceRow schema.
    """
    service = brand.service
    return {
        "ma_dich_vu": brand.service_code,
        "ten_dich_vu": service.name,
        "hang_san_pham": brand.device_brand.name if brand.device_brand else None,
        "ten_san_pham": brand.device_type,
        "hang_dich_vu": brand.name,
        "gia": float(brand.price) if brand.price else None,
        "gia_buon": None,  # Không có trong Brand
        "bao_hanh": brand.warranty,
        "mau_sac_san_pham": brand.color,
    }


class ChatbotService:
    @staticmethod
    async def add_product(user_device_id: uuid.UUID, current_user: User):
        """
        Thêm một sản phẩm mới vào Elasticsearch trực tiếp.
        Chạy trong background task với session riêng.
        """
        async with async_session() as db:
            user_device = await UserDeviceRepository.get_by_id_with_details(db, user_device_id)
            if not user_device:
                logger.error(f"Không tìm thấy UserDevice với ID: {user_device_id} trong background task.")
                return

            customer_id = str(current_user.id)
            product_data = await get_product_data(db, user_device)

            try:
                await ElasticsearchService.upsert_product(customer_id, product_data, api_key=current_user.gemini_api_key)
                logger.info(f"Thêm sản phẩm {user_device.product_code} vào Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi thêm sản phẩm vào Elasticsearch: {e}")

    @staticmethod
    async def update_product(user_device_id: uuid.UUID, current_user: User):
        """
        Cập nhật một sản phẩm trong Elasticsearch trực tiếp.
        Chạy trong background task với session riêng.
        """
        async with async_session() as db:
            user_device = await UserDeviceRepository.get_by_id_with_details(db, user_device_id)
            if not user_device or not user_device.product_code:
                logger.error(f"Không tìm thấy UserDevice hoặc product_code với ID: {user_device_id} trong background task.")
                return

            customer_id = str(current_user.id)
            product_data = await get_product_data(db, user_device)

            try:
                await ElasticsearchService.upsert_product(customer_id, product_data, api_key=current_user.gemini_api_key)
                logger.info(f"Cập nhật sản phẩm {user_device.product_code} trong Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi cập nhật sản phẩm trong Elasticsearch: {e}")

    @staticmethod
    async def delete_product(product_code: str, current_user: User):
        """
        Xóa một sản phẩm khỏi Elasticsearch trực tiếp.
        Sử dụng product_code làm ID sản phẩm.
        """
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.delete_product(customer_id, product_code)
            logger.info(f"Xóa sản phẩm {product_code} khỏi Elasticsearch thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa sản phẩm khỏi Elasticsearch: {e}")

    @staticmethod
    async def bulk_delete_products(product_codes: list[str], current_user: User):
        """
        Xóa hàng loạt sản phẩm khỏi Elasticsearch trực tiếp.
        """
        if not product_codes:
            return

        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.bulk_delete_products(customer_id, product_codes)
            logger.info(f"Xóa hàng loạt {len(product_codes)} sản phẩm khỏi Elasticsearch cho user {customer_id} thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa hàng loạt sản phẩm khỏi Elasticsearch: {e}")

    @staticmethod
    async def delete_all_products(current_user: User):
        """
        Xóa tất cả sản phẩm của một user khỏi Elasticsearch trực tiếp.
        """
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.delete_all_products(customer_id)
            logger.info(f"Xóa tất cả sản phẩm khỏi Elasticsearch cho user {customer_id} thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa tất cả sản phẩm khỏi Elasticsearch: {e}")

    @staticmethod
    async def add_all_products(user_devices: list, current_user: User):
        """
        Thêm tất cả sản phẩm vào Elasticsearch trực tiếp.
        Sử dụng cho việc restore tất cả thiết bị đã xóa.
        """
        if not user_devices:
            return

        async with async_session() as db:
            customer_id = str(current_user.id)

            payload = []
            for user_device in user_devices:
                try:
                    product_data = await get_product_data(db, user_device)
                    payload.append(product_data)
                except Exception as e:
                    logger.error(f"Lỗi khi chuẩn bị dữ liệu sản phẩm {getattr(user_device, 'product_code', None)} để đồng bộ bulk: {e}")

            if not payload:
                return

            try:
                await ElasticsearchService.bulk_upsert_products(customer_id, payload, api_key=current_user.gemini_api_key)
                logger.info(f"Đồng bộ bulk {len(payload)} sản phẩm vào Elasticsearch thành công cho user {customer_id}.")
            except Exception as e:
                logger.error(f"Lỗi khi đồng bộ bulk sản phẩm vào Elasticsearch: {e}")

    @staticmethod
    async def add_all_services(brands: list, current_user: User):
        """
        Thêm tất cả dịch vụ vào Elasticsearch trực tiếp.
        Sử dụng cho việc restore tất cả brands đã xóa.
        """
        if not brands:
            return

        customer_id = str(current_user.id)

        payload = []
        for brand in brands:
            try:
                service_data = await get_service_data(brand)
                payload.append(service_data)
            except Exception as e:
                logger.error(f"Lỗi khi chuẩn bị dữ liệu dịch vụ {getattr(brand, 'service_code', None)} để đồng bộ bulk: {e}")

        if not payload:
            return

        try:
            await ElasticsearchService.bulk_upsert_services(customer_id, payload, api_key=current_user.gemini_api_key)
            logger.info(f"Đồng bộ bulk {len(payload)} dịch vụ vào Elasticsearch thành công cho user {customer_id}.")
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ bulk dịch vụ vào Elasticsearch: {e}")

    @staticmethod
    async def stream_chat_with_bot(thread_id: str, query: str, customer_id: str, llm_provider: str, api_key: str, access: Optional[int] = None, scopes: list[str] = None, image_url: Optional[str] = None, image_urls: Optional[list[str]] = None, image_base64: Optional[str] = None):
        """
        Gửi yêu cầu chat đến chatbot backend và stream phản hồi.
        """
        access = access if access is not None else ChatbotService._convert_scopes_to_access(scopes)
        logger.info(f"Stream chat request - Thread: {thread_id}, Customer: {customer_id}, LLM: {llm_provider}, Access: {access}")
        
        # Thêm cờ stream=True vào payload
        url = f"{CHATBOT_API_BASE_URL}/chat/{thread_id}"
        payload = {
            "query": query,
            "customer_id": customer_id,
            "llm_provider": llm_provider,
            "api_key": api_key,
            "access": access,
            "stream": True  # Yêu cầu streaming
        }
        if image_url:
            payload["image_url"] = image_url
        if image_urls:
            payload["image_urls"] = image_urls
        if image_base64:
            payload["image_base64"] = image_base64

        async with httpx.AsyncClient(timeout=300.0) as client:
            try:
                async with client.stream("POST", url, json=payload) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes():
                        yield chunk
            except httpx.HTTPStatusError as e:
                log_error({
                    "message": f"Lỗi từ Chatbot backend (HTTP {e.response.status_code})",
                    "event.category": "chatbot",
                    "event.action": "chatbot.stream.http_error",
                    "source.layer": "service",
                    "source.function": "stream_chat_with_bot",
                    "error.type": "HTTPStatusError",
                    "http.status_code": e.response.status_code,
                    "error.response_text": str(e.response.text)[:500],
                })
                # Không thể raise HTTPException trong generator, client sẽ thấy kết nối bị cắt
            except Exception as e:
                log_error({
                    "message": f"Lỗi không xác định khi stream chat với bot: {e}",
                    "event.category": "chatbot",
                    "event.action": "chatbot.stream.error",
                    "source.layer": "service",
                    "source.function": "stream_chat_with_bot",
                    "error.type": e.__class__.__name__,
                    "error.stack_trace": traceback.format_exc(),
                })

    @staticmethod
    async def chat_with_bot(thread_id: str, query: str, customer_id: str, llm_provider: str, api_key: str, access: Optional[int] = None, scopes: list[str] = None, image_url: Optional[str] = None, image_urls: Optional[list[str]] = None, image_base64: Optional[str] = None, history: Optional[list] = None) -> Dict[str, Any]:
        """
        Gửi yêu cầu chat đến chatbot backend.
        """
        # Chuyển đổi scopes thành access code nếu chưa được cung cấp sẵn
        access = access if access is not None else ChatbotService._convert_scopes_to_access(scopes)
        logger.info(f"Chat request - Thread: {thread_id}, Customer: {customer_id}, LLM: {llm_provider}, Access: {access}")
        
        url = f"{CHATBOT_API_BASE_URL}/chat/{thread_id}"
        payload = {
            "query": query,
            "customer_id": customer_id,
            "llm_provider": llm_provider,
            "api_key": api_key,
            "access": access  # Sử dụng access thay vì scopes
        }
        if image_url:
            payload["image_url"] = image_url
        if image_urls:
            payload["image_urls"] = image_urls
        if image_base64:
            payload["image_base64"] = image_base64
        if history is not None:
            try:
                safe_history = []
                for h in (history or []):
                    role = None
                    message = None
                    if isinstance(h, dict):
                        role = h.get('role')
                        message = h.get('message')
                    else:
                        role = getattr(h, 'role', None)
                        message = getattr(h, 'message', None)
                    if role and message is not None:
                        safe_history.append({ 'role': str(role), 'message': str(message) })
                payload["history"] = safe_history
            except Exception:
                pass
        
        logger.info(f"Payload gửi đến chatbot: {payload}")

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, timeout = None)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Lỗi từ Chatbot backend (HTTP {e.response.status_code}): {e.response.text}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Có lỗi xảy ra khi kết nối đến dịch vụ chatbot. Vui lòng thử lại sau."
                )
            except Exception as e:
                logger.error(f"Lỗi không xác định khi chat với bot: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Lỗi hệ thống không xác định khi xử lý yêu cầu chat."
                )

    @staticmethod
    async def get_chat_history(customer_id: str, thread_id: str, limit: int = 20) -> list[dict]:
        try:
            url = f"{CHATBOT_API_BASE_URL}/chat-history/{customer_id}/{thread_id}"
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(url)
                    if resp.status_code == 404:
                        return []
                    resp.raise_for_status()
                except httpx.HTTPStatusError as e:
                    logger.warning(f"get_chat_history failed HTTP {e.response.status_code}: {e.response.text}")
                    return []
                except Exception as e:
                    logger.warning(f"get_chat_history request error: {e}")
                    return []

                data = []
                try:
                    data = resp.json()
                except Exception:
                    return []

                if not isinstance(data, list):
                    return []

                newest = data[:limit] if limit and limit > 0 else data
                ordered = list(reversed(newest))
                out = []
                for item in ordered:
                    try:
                        role = str(item.get('role')) if isinstance(item, dict) else str(getattr(item, 'role', ''))
                        message = item.get('message') if isinstance(item, dict) else getattr(item, 'message', '')
                        if role and message is not None:
                            out.append({ 'role': role, 'message': str(message) })
                    except Exception:
                        continue
                return out
        except Exception as e:
            logger.warning(f"get_chat_history unexpected error: {e}")
            return []

    @staticmethod
    def _convert_scopes_to_access(scopes: list[str] = None) -> int:
        """
        Chuyển đổi scopes thành access code cho chatbot API.
        Dựa trên 3 combo plans thực tế:
        
        📋 COMBO PLANS:
        - Combo Toàn diện 4: Tất cả dịch vụ (access = 123)
        - Combo Tiết kiệm 2: Sửa chữa + Bán điện thoại (access = 12)  
        - Combo Nâng cao 3: Sửa chữa + Bán điện thoại + Bán linh kiện (access = 123)
        
        🔢 ACCESS CODES:
        - 0: Không có quyền
        - 1: Chỉ sản phẩm (Bán điện thoại)
        - 2: Chỉ dịch vụ (Sửa chữa)
        - 3: Sản phẩm + Dịch vụ (1 | 2 = 3)
        - 12: Dịch vụ + Sản phẩm (2 | 1 = 3, nhưng logic đặc biệt)
        - 123: Tất cả (Sản phẩm + Dịch vụ + Phụ kiện)
        """
        if not scopes:
            logger.info("Không có scopes, trả về access = 0")
            return 0  # Không có quyền
        
        # Nếu có "*" nghĩa là có tất cả quyền (Combo Toàn diện 4)
        if "*" in scopes:
            logger.info("Có scope '*' (Combo Toàn diện 4), trả về access = 123")
            return 123  # Tất cả: sản phẩm + dịch vụ + phụ kiện
        
        logger.info(f"Chuyển đổi scopes {scopes} thành access code")
        
        # Chuẩn hóa chuỗi và nhận diện theo ID quan hệ hoặc tên dịch vụ
        scope_strs = [str(s).strip().lower() for s in scopes]
        REPAIR_ID = "154519e0-9043-44f4-b67b-fb3d6f901658"
        PRODUCT_ID = "9b1ad1bc-629c-46a9-9503-bd8c985b2407"
        ACCESSORY_ID = "b807488e-b95e-4e17-bae6-ed7ffd03d8f3"

        # Theo ID (quan hệ 3 bảng)
        has_repair_id = any(REPAIR_ID in s for s in scope_strs)
        has_sales_id = any(PRODUCT_ID in s for s in scope_strs)
        has_accessory_id = any(ACCESSORY_ID in s for s in scope_strs)

        # Theo từ khóa tên (fallback)
        has_repair_kw = any(s and any(k in s for k in ["sửa chữa", "dịch vụ sửa chữa", "repair", "service"]) for s in scope_strs)
        has_sales_kw = any(s and any(k in s for k in ["bán điện thoại", "sản phẩm", "product", "phone"]) for s in scope_strs)
        has_accessory_kw = any(s and any(k in s for k in ["bán linh kiện", "linh kiện", "phụ kiện", "accessory", "component"]) for s in scope_strs)

        has_repair = has_repair_id or has_repair_kw
        has_sales = has_sales_id or has_sales_kw
        has_accessory = has_accessory_id or has_accessory_kw

        # Ưu tiên cao nhất: nếu có linh kiện => Combo Nâng cao 3 (123)
        if has_accessory:
            logger.info("Phát hiện scope liên quan linh kiện -> access = 123 (Combo Nâng cao 3)")
            return 123

        # Combo Tiết kiệm 2: có cả sửa chữa và bán điện thoại -> 12
        if has_repair and has_sales:
            logger.info("Phát hiện Combo Tiết kiệm 2: Sửa chữa + Bán điện thoại -> access = 12")
            return 12

        # Đơn lẻ
        if has_repair:
            return 2
        if has_sales:
            return 1

        logger.warning(f"Không thể map scopes {scopes} thành access code, sử dụng 0")
        return 0

    @staticmethod
    async def add_service(brand_id: uuid.UUID, current_user: User):
        async with async_session() as db:
            brand = await BrandRepository.get_by_id_with_details(db, brand_id)
            if not brand:
                logger.error(f"Không tìm thấy Brand với ID: {brand_id} trong background task.")
                return

            customer_id = str(current_user.id)
            service_data = await get_service_data(brand)

            try:
                await ElasticsearchService.upsert_service(customer_id, service_data, api_key=current_user.gemini_api_key)
                logger.info(f"Thêm dịch vụ {service_data.get('ma_dich_vu')} vào Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi thêm dịch vụ vào Elasticsearch: {e}")

    @staticmethod
    async def update_service(brand_id: uuid.UUID, current_user: User):
        async with async_session() as db:
            brand = await BrandRepository.get_by_id_with_details(db, brand_id)
            if not brand or not brand.service_code:
                logger.error(f"Không tìm thấy Brand hoặc service_code với ID: {brand_id} trong background task.")
                return

            customer_id = str(current_user.id)
            service_data = await get_service_data(brand)

            try:
                await ElasticsearchService.upsert_service(customer_id, service_data, api_key=current_user.gemini_api_key)
                logger.info(f"Cập nhật dịch vụ {brand.service_code} trong Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi cập nhật dịch vụ trong Elasticsearch: {e}")

    @staticmethod
    async def delete_service(service_code: str, current_user: User):
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.delete_service(customer_id, service_code)
            logger.info(f"Xóa dịch vụ {service_code} khỏi Elasticsearch thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa dịch vụ khỏi Elasticsearch: {e}")

    @staticmethod
    async def add_product_component(product_component_id: uuid.UUID, current_user: User):
        """
        Thêm một linh kiện mới vào Elasticsearch trực tiếp.
        Chạy trong background task với session riêng.
        """
        async with async_session() as db:
            from app.repositories.product_component_repository import ProductComponentRepository
            # User-scoped fetch to ensure ownership
            product_component = await ProductComponentRepository.get_by_id_for_user(db, product_component_id, current_user.id)
            if not product_component:
                logger.error(f"Không tìm thấy ProductComponent với ID: {product_component_id} trong background task.")
                return

            customer_id = str(current_user.id)
            
            # Chuyển đổi dữ liệu linh kiện thành format phù hợp cho Elasticsearch
            accessory_data = {
                "accessory_code": product_component.product_code,
                "accessory_name": product_component.product_name,
                "lifecare_price": float(product_component.amount) if product_component.amount else None,
                "sale_price": float(product_component.wholesale_price) if product_component.wholesale_price else None,
                "trademark": product_component.trademark,
                "guarantee": product_component.guarantee,
                "inventory": product_component.stock,
                "specifications": product_component.description,
                "avatar_images": product_component.product_photo,
                "link_accessory": product_component.product_link,
                "category": product_component.category,
                "properties": product_component.properties
            }

            try:
                await ElasticsearchService.upsert_accessory(customer_id, accessory_data, api_key=current_user.gemini_api_key)
                logger.info(f"Thêm linh kiện {product_component.product_code} vào Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi thêm linh kiện vào Elasticsearch: {e}")

    @staticmethod
    async def update_product_component(product_component_id: uuid.UUID, current_user: User):
        """
        Cập nhật một linh kiện trong Elasticsearch trực tiếp.
        Chạy trong background task với session riêng.
        """
        async with async_session() as db:
            from app.repositories.product_component_repository import ProductComponentRepository
            product_component = await ProductComponentRepository.get_by_id(db, product_component_id)
            if not product_component or not product_component.product_code:
                logger.error(f"Không tìm thấy ProductComponent hoặc product_code với ID: {product_component_id} trong background task.")
                return

            customer_id = str(current_user.id)
            
            # Chuyển đổi dữ liệu linh kiện thành format phù hợp cho Elasticsearch
            accessory_data = {
                "accessory_code": product_component.product_code,
                "accessory_name": product_component.product_name,
                "lifecare_price": float(product_component.amount) if product_component.amount else None,
                "sale_price": float(product_component.wholesale_price) if product_component.wholesale_price else None,
                "trademark": product_component.trademark,
                "guarantee": product_component.guarantee,
                "inventory": product_component.stock,
                "specifications": product_component.description,
                "avatar_images": product_component.product_photo,
                "link_accessory": product_component.product_link,
                "category": product_component.category,
                "properties": product_component.properties
            }

            try:
                await ElasticsearchService.upsert_accessory(customer_id, accessory_data, api_key=current_user.gemini_api_key)
                logger.info(f"Cập nhật linh kiện {product_component.product_code} trong Elasticsearch thành công.")
            except Exception as e:
                logger.error(f"Lỗi khi cập nhật linh kiện trong Elasticsearch: {e}")

    @staticmethod
    async def delete_product_component(product_code: str, current_user: User):
        """
        Xóa một linh kiện khỏi Elasticsearch trực tiếp.
        Sử dụng product_code làm ID linh kiện.
        """
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.delete_accessory(customer_id, product_code)
            logger.info(f"Xóa linh kiện {product_code} khỏi Elasticsearch thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa linh kiện khỏi Elasticsearch: {e}")

    @staticmethod
    async def bulk_delete_product_components(product_codes: list[str], current_user: User):
        """Xóa hàng loạt linh kiện sản phẩm khỏi Elasticsearch trực tiếp."""
        if not product_codes:
            return
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.bulk_delete_accessories(customer_id, product_codes)
            logger.info(f"Xóa hàng loạt {len(product_codes)} linh kiện khỏi Elasticsearch cho user {customer_id} thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa hàng loạt linh kiện khỏi Elasticsearch: {e}")

    @staticmethod
    async def delete_all_product_components(current_user: User):
        """Xóa tất cả linh kiện của một user khỏi Elasticsearch trực tiếp."""
        customer_id = str(current_user.id)

        try:
            await ElasticsearchService.delete_all_accessories(customer_id)
            logger.info(f"Xóa tất cả linh kiện khỏi Elasticsearch cho user {customer_id} thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa tất cả linh kiện khỏi Elasticsearch: {e}")

    # Methods for ChatbotCustom (Linh Kiện Hoàng Mai) integration
    @staticmethod
    async def add_product_component_to_custom(component_id: uuid.UUID, user: User):
        """
        Thêm linh kiện vào ChatbotCustom (Hoàng Mai)
        """
        try:
            async with async_session() as db:
                success = await ChatbotSyncService.sync_product_component(
                    db, str(component_id), user, "create"
                )
                if success:
                    logger.info(f"Thêm linh kiện {component_id} vào ChatbotCustom thành công.")
                else:
                    logger.error(f"Lỗi khi thêm linh kiện {component_id} vào ChatbotCustom.")
        except Exception as e:
            logger.error(f"Lỗi khi thêm linh kiện vào ChatbotCustom: {e}")

    @staticmethod
    async def update_product_component_in_custom(component_id: uuid.UUID, user: User):
        """
        Cập nhật linh kiện trong ChatbotCustom
        """
        try:
            async with async_session() as db:
                success = await ChatbotSyncService.sync_product_component(
                    db, str(component_id), user, "update"
                )
                if success:
                    logger.info(f"Cập nhật linh kiện {component_id} trong ChatbotCustom thành công.")
                else:
                    logger.error(f"Lỗi khi cập nhật linh kiện {component_id} trong ChatbotCustom.")
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật linh kiện trong ChatbotCustom: {e}")

    @staticmethod
    async def delete_product_component_from_custom(component_id: str, user: User):
        """
        Xóa linh kiện khỏi ChatbotCustom
        """
        try:
            async with async_session() as db:
                success = await ChatbotSyncService.sync_product_component(
                    db, component_id, user, "delete"
                )
                if success:
                    logger.info(f"Xóa linh kiện {component_id} khỏi ChatbotCustom thành công.")
                else:
                    logger.error(f"Lỗi khi xóa linh kiện {component_id} khỏi ChatbotCustom.")
        except Exception as e:
            logger.error(f"Lỗi khi xóa linh kiện khỏi ChatbotCustom: {e}")

    @staticmethod
    async def sync_all_user_components_to_custom(user: User):
        """
        Đồng bộ toàn bộ linh kiện của user với ChatbotCustom
        """
        try:
            async with async_session() as db:
                success = await ChatbotSyncService.sync_all_user_components(db, user)
                if success:
                    logger.info(f"Đồng bộ toàn bộ linh kiện của user {user.id} với ChatbotCustom thành công.")
                else:
                    logger.error(f"Lỗi khi đồng bộ toàn bộ linh kiện của user {user.id} với ChatbotCustom.")
                return success
        except Exception as e:
            logger.error(f"Lỗi khi đồng bộ toàn bộ linh kiện với ChatbotCustom: {e}")
            return False

    @staticmethod
    async def sync_excel_import_to_mobile_store(file_content: bytes, user: User):
        """
        Đồng bộ dữ liệu từ file Excel import với ChatbotMobileStore
        Sử dụng API /insert-accessory/{customer_id} để upload file Excel
        """
        try:
            logger.info(f"Bắt đầu đồng bộ Excel import với ChatbotMobileStore cho user: {user.email}")
            
            customer_id = str(user.id)
            url = f"{CHATBOT_API_BASE_URL}/upload-accessory-embed/{customer_id}"
            logger.info(f"Gọi API ChatbotMobileStore Excel import: {url}")
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Tạo form data với file Excel
                files = {"file": ("import.xlsx", file_content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                
                response = await client.post(url, files=files)
                
                logger.info(f"Response từ ChatbotMobileStore Excel import: {response.status_code}")
                
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(f"Excel import thành công: {response_data}")
                    return True, response_data
                else:
                    logger.error(f"Excel import thất bại: {response.status_code} - {response.text}")
                    return False, f"HTTP {response.status_code}: {response.text}"
                    
        except Exception as e:
            error_msg = f"Lỗi đồng bộ Excel import với ChatbotMobileStore: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    @staticmethod
    async def bulk_sync_products_from_file(file_content: bytes, user: User):
        """
        Đồng bộ dữ liệu sản phẩm từ file Excel import với ChatbotMobileStore.
        Sử dụng API /insert-product/{customer_id} để upload file Excel.
        """
        try:
            logger.info(f"Bắt đầu đồng bộ sản phẩm từ Excel với ChatbotMobileStore cho user: {user.email}")
            
            customer_id = str(user.id)
            url = f"{CHATBOT_API_BASE_URL}/upload-product-embed/{customer_id}"
            logger.info(f"Gọi API ChatbotMobileStore để đồng bộ sản phẩm từ Excel: {url}")
            
            async with httpx.AsyncClient(timeout=300.0) as client:
                files = {"file": ("products_import.xlsx", file_content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                
                response = await client.post(url, files=files)
                
                logger.info(f"Response từ ChatbotMobileStore (đồng bộ sản phẩm): {response.status_code}")
                
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(f"Đồng bộ sản phẩm từ Excel thành công: {response_data}")
                    return True, response_data
                else:
                    logger.error(f"Đồng bộ sản phẩm từ Excel thất bại: {response.status_code} - {response.text}")
                    return False, f"HTTP {response.status_code}: {response.text}"
                    
        except Exception as e:
            error_msg = f"Lỗi khi đồng bộ sản phẩm từ Excel với ChatbotMobileStore: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    @staticmethod
    async def bulk_sync_services_from_file(file_content: bytes, user: User):
        """
        Đồng bộ dữ liệu dịch vụ từ file Excel import với ChatbotMobileStore.
        Sử dụng API /insert-service/{customer_id} để upload file Excel.
        """
        try:
            logger.info(f"Bắt đầu đồng bộ dịch vụ từ Excel với ChatbotMobileStore cho user: {user.email}")
            
            customer_id = str(user.id)
            url = f"{CHATBOT_API_BASE_URL}/upload-service-embed/{customer_id}"
            logger.info(f"Gọi API ChatbotMobileStore để đồng bộ dịch vụ từ Excel: {url}")
            
            async with httpx.AsyncClient(timeout=300.0) as client:
                files = {"file": ("services_import.xlsx", file_content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                
                response = await client.post(url, files=files)
                
                logger.info(f"Response từ ChatbotMobileStore (đồng bộ dịch vụ): {response.status_code}")
                
                if response.status_code == 200:
                    response_data = response.json()
                    logger.info(f"Đồng bộ dịch vụ từ Excel thành công: {response_data}")
                    return True, response_data
                else:
                    logger.error(f"Đồng bộ dịch vụ từ Excel thất bại: {response.status_code} - {response.text}")
                    return False, f"HTTP {response.status_code}: {response.text}"
                    
        except Exception as e:
            error_msg = f"Lỗi khi đồng bộ dịch vụ từ Excel với ChatbotMobileStore: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg

    @staticmethod
    async def clear_history_chat(user: User):
        """
        Xóa lịch sử chat của một user
        """
        try:
            url = f"{CHATBOT_API_BASE_URL}/chat-history-clear/{user.id}"
            async with httpx.AsyncClient() as client:
                response = await client.post(url)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Lỗi khi xóa lịch sử chat của user {user.id}: {e}")
            return False