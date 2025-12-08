from fastapi import APIRouter, HTTPException, Depends
from app.middlewares.auth_middleware import get_current_user
from pydantic import BaseModel
from app.models.user import User
import httpx
import os
from typing import Dict, Any, Optional
from app.logging.decorators import with_log_context

router = APIRouter()

class ChatbotJsSettingsRequest(BaseModel):
    chatbot_icon_url: Optional[str] = None
    chatbot_message_default: Optional[str] = None
    chatbot_callout: Optional[str] = None
    chatbot_name: Optional[str] = None
    
# URL của ChatbotMobileStore API
CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL")

async def call_chatbot_api(
    endpoint: str,
    method: str = "GET",
    data: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Gọi API từ ChatbotCustom
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
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi ChatbotCustom API: {str(e)}")

@router.get("/chatbot-js-agent/settings")
@with_log_context(
    event_action="chatbot_js.settings.get",
    event_category="chatbot_js",
    source_layer="controller",
    source_controller="chatbot_js_settings_controller",
)
async def get_js_chatbot_agent(
    current_user: User = Depends(get_current_user)
):
    """
    Lấy cài đặt khung chat cho người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        result = await call_chatbot_api(endpoint=f"/settings/{user_id}", method="GET")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/chatbot-js-agent/settings")
@with_log_context(
    event_action="chatbot_js.settings.update",
    event_category="chatbot_js",
    source_layer="controller",
    source_controller="chatbot_js_settings_controller",
)
async def update_js_chatbot_agent(
    request: ChatbotJsSettingsRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Cập nhật cài đặt khung chat cho người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        
        result = await call_chatbot_api(endpoint=f"/settings/{user_id}", method="PUT", data={"chatbot_icon_url": request.chatbot_icon_url, "chatbot_callout": request.chatbot_callout, "chatbot_message_default": request.chatbot_message_default, "chatbot_name": request.chatbot_name})
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
