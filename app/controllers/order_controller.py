from fastapi import APIRouter, HTTPException, Depends
from app.middlewares.auth_middleware import get_current_user

from app.models.user import User
import httpx
import os
from typing import Dict, Any

from app.logging.decorators import with_log_context

router = APIRouter()

# URL của ChatbotMobileStore API
CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL")

async def call_chatbot_api(
    endpoint: str,
    method: str = "GET",
    data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Gọi API từ ChatbotMobileStore
    """
    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}{endpoint}"
            headers = {}
            
            if method == "GET":
                response = await client.get(url, headers=headers, timeout=None)
            elif method == "POST":
                response = await client.post(url, json=data, headers=headers, timeout=None)
            elif method == "PUT":
                response = await client.put(url, json=data, headers=headers, timeout=None)
            elif method == "DELETE":
                response = await client.delete(url, headers=headers, timeout=None)
            
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type:
                    return response.json()
                else:
                    return {"content": response.text}
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotMobileStore: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi ChatbotMobileStore API: {str(e)}")

@router.get("/orders/product-order")
@with_log_context(
    event_action="order.product.list",
    event_category="order",
    source_layer="controller",
    source_controller="order_controller",
)
async def get_product_order(
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách đơn hàng điện thoại.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        result = await call_chatbot_api(endpoint=f"/orders/{user_id}/products", method="GET")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orders/service-order")
@with_log_context(
    event_action="order.service.list",
    event_category="order",
    source_layer="controller",
    source_controller="order_controller",
)
async def get_service_order(
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách đơn hàng dịch vụ.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        result = await call_chatbot_api(endpoint=f"/orders/{user_id}/services", method="GET")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/orders/accessory-order")
@with_log_context(
    event_action="order.accessory.list",
    event_category="order",
    source_layer="controller",
    source_controller="order_controller",
)
async def get_accessory_order(
    current_user: User = Depends(get_current_user)
):
    """
    Lấy danh sách đơn hàng phụ kiện.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        result = await call_chatbot_api(endpoint=f"/orders/{user_id}/accessories", method="GET")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/orders/status")
@with_log_context(
    event_action="order.status.update",
    event_category="order",
    source_layer="controller",
    source_controller="order_controller",
)
async def update_order_status(
    order_id: str,
    thread_id: str,
    status: str,
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật trạng thái đơn hàng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        result = await call_chatbot_api(endpoint=f"/orders/{user_id}/{thread_id}/{order_id}", method="PUT", data={"status": status})
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    