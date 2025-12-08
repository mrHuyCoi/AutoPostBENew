from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
import httpx
import os
from typing import Dict, Any
from app.repositories.user_bot_control_repository import UserBotControlRepository
from app.models.user_bot_control import UserBotControl
from app.logging.decorators import with_log_context

router = APIRouter()

# ----------------------- Per-platform Bot Control -----------------------
async def _get_platform_controls_map(db: AsyncSession, user_id: str) -> Dict[str, bool]:
    platforms = ["zalo", "zalo_oa", "messenger"]
    out: Dict[str, bool] = {}
    for p in platforms:
        try:
            out[p] = await UserBotControlRepository.is_enabled(db, user_id, p)
        except Exception:
            # default to True if cannot determine
            out[p] = True
    return out

@router.get("/chatbot-control/platforms")
@with_log_context(
    event_action="user_config.chatbot_control_platforms.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_platform_controls(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    user_id = str(current_user.id)
    return await _get_platform_controls_map(db, user_id)

@router.put("/chatbot-control/platforms")
@with_log_context(
    event_action="user_config.chatbot_control_platforms.set",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def set_platform_control(
    payload: Dict[str, Any],
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    platform = (payload or {}).get("platform")
    enabled = (payload or {}).get("enabled")
    if platform not in ("zalo", "zalo_oa", "messenger"):
        raise HTTPException(status_code=400, detail="platform phải là 'zalo' | 'zalo_oa' | 'messenger'")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="enabled phải là boolean")

    stmt = select(UserBotControl).where(
        UserBotControl.user_id == current_user.id,
        UserBotControl.platform == platform,
    )
    res = await db.execute(stmt)
    rec: UserBotControl | None = res.scalars().first()
    if not rec:
        rec = UserBotControl(user_id=current_user.id, platform=platform, enabled=bool(enabled))
        db.add(rec)
    else:
        rec.enabled = bool(enabled)
    await db.commit()
    return await _get_platform_controls_map(db, str(current_user.id))

# URL của ChatbotMobileStore API
CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL", "http://localhost:8001")
CHATBOT_CUSTOM_API_BASE_URL = os.getenv("CHATBOT_CUSTOM_API_BASE_URL", "http://localhost:8002")

async def call_chatbot_custom_api(
    endpoint: str, 
    method: str = "GET", 
    data: Dict[str, Any] = None,
    file: UploadFile = None,
    user_id: str = None
) -> Dict[str, Any]:
    """
    Gọi API từ ChatbotCustom
    """
    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_CUSTOM_API_BASE_URL}{endpoint}"
            headers = {"Content-Type": "application/json"}
            
            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "PUT":
                response = await client.put(url, json=data, headers=headers)
            elif method == "DELETE":
                response = await client.delete(url, headers=headers)
            elif method == "POST":
                if file:
                    # Upload file
                    files = {"file": (file.filename, file.file, file.content_type)}
                    response = await client.post(url, files=files)
                else:
                    # Upload text
                    response = await client.post(url, json=data, headers=headers)
            else:
                raise HTTPException(status_code=400, detail="Method không được hỗ trợ")
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotCustom: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi ChatbotCustom API: {str(e)}")

async def call_chatbot_api(
    endpoint: str, 
    method: str = "GET", 
    data: Dict[str, Any] = None,
    file: UploadFile = None,
    user_id: str = None
) -> Dict[str, Any]:
    """
    Gọi API từ ChatbotMobileStore
    """
    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}{endpoint}"
            headers = {"Content-Type": "application/json"}
            
            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "PUT":
                response = await client.put(url, json=data, headers=headers)
            elif method == "DELETE":
                response = await client.delete(url, headers=headers)
            elif method == "POST":
                if file:
                    # Upload file
                    files = {"file": (file.filename, file.file, file.content_type)}
                    response = await client.post(url, files=files)
                else:
                    # Upload text
                    response = await client.post(url, json=data, headers=headers)
            else:
                raise HTTPException(status_code=400, detail="Method không được hỗ trợ")
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(status_code=response.status_code, detail=f"Lỗi từ ChatbotMobileStore: {response.text}")
                
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi gọi ChatbotMobileStore API: {str(e)}")

# ----------------------- Chatbot Power Control (Proxy) -----------------------
@router.get("/chatbot-control/mobile/status")
@with_log_context(
    event_action="user_config.chatbot_mobile.status.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_mobile_bot_status(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}/customer/status/{user_id}"
            res = await client.get(url)
            return res.json() if res.status_code == 200 else HTTPException(status_code=res.status_code, detail=res.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")


@router.post("/chatbot-control/mobile/stop")
@with_log_context(
    event_action="user_config.chatbot_mobile.stop",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def stop_mobile_bot(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}/customer/stop/{user_id}"
            res = await client.post(url)
            return res.json() if res.status_code == 200 else HTTPException(status_code=res.status_code, detail=res.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")


@router.post("/chatbot-control/mobile/start")
@with_log_context(
    event_action="user_config.chatbot_mobile.start",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def start_mobile_bot(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}/customer/start/{user_id}"
            res = await client.post(url)
            return res.json() if res.status_code == 200 else HTTPException(status_code=res.status_code, detail=res.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotMobileStore: {str(e)}")


@router.get("/chatbot-control/custom/status")
@with_log_context(
    event_action="user_config.chatbot_custom.status.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_custom_bot_status(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_CUSTOM_API_BASE_URL}/bot-status/{user_id}"
            res = await client.get(url)
            return res.json() if res.status_code == 200 else HTTPException(status_code=res.status_code, detail=res.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")


@router.post("/chatbot-control/custom")
@with_log_context(
    event_action="user_config.chatbot_custom.control",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def control_custom_bot(
    payload: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    try:
        user_id = str(current_user.id)
        command = payload.get("command")
        if command not in ("start", "stop", "status"):
            raise HTTPException(status_code=400, detail="command phải là 'start' | 'stop' | 'status'")
        async with httpx.AsyncClient() as client:
            if command == "status":
                url = f"{CHATBOT_CUSTOM_API_BASE_URL}/bot-status/{user_id}"
                res = await client.get(url)
            else:
                url = f"{CHATBOT_CUSTOM_API_BASE_URL}/power-off-bot/{user_id}"
                res = await client.post(url, json={"command": command})
            return res.json() if res.status_code == 200 else HTTPException(status_code=res.status_code, detail=res.text)
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Không thể kết nối đến ChatbotCustom: {str(e)}")

@router.put("/user-config/persona")
@with_log_context(
    event_action="user_config.persona.update",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def set_persona_config(
    config: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Cấu hình hoặc cập nhật vai trò và tên cho chatbot AI của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi API config/persona từ ChatbotMobileStore
        result = await call_chatbot_api(
            endpoint=f"/config/persona/{user_id}",
            method="PUT",
            data=config,
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-config/persona")
@with_log_context(
    event_action="user_config.persona.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_persona_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy cấu hình vai trò và tên của chatbot AI cho một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/persona/{user_id}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/user-config/persona")
@with_log_context(
    event_action="user_config.persona.delete",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def delete_persona_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Xóa cấu hình vai trò và tên của chatbot AI, quay về mặc định.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/persona/{user_id}",
            method="DELETE",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/user-config/prompt")
@with_log_context(
    event_action="user_config.prompt.update",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def set_prompt_config(
    config: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Thêm hoặc cập nhật system prompt tùy chỉnh cho người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/prompt/{user_id}",
            method="PUT",
            data=config,
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-config/prompt")
@with_log_context(
    event_action="user_config.prompt.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_prompt_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy system prompt tùy chỉnh của một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/prompt/{user_id}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/user-config/prompt")
@with_log_context(
    event_action="user_config.prompt.delete",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def delete_prompt_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Xóa system prompt tùy chỉnh của một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/prompt/{user_id}",
            method="DELETE",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/user-config/service-feature")
@with_log_context(
    event_action="user_config.service_feature.update",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def set_service_feature_config(
    config: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Bật hoặc tắt chức năng tư vấn dịch vụ cho chatbot của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/service-feature/{user_id}",
            method="PUT",
            data=config,
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-config/service-feature")
@with_log_context(
    event_action="user_config.service_feature.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_service_feature_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy trạng thái bật/tắt chức năng tư vấn dịch vụ của một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/service-feature/{user_id}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/user-config/accessory-feature")
@with_log_context(
    event_action="user_config.accessory_feature.update",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def set_accessory_feature_config(
    config: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Bật hoặc tắt chức năng tư vấn phụ kiện cho chatbot của người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/accessory-feature/{user_id}",
            method="PUT",
            data=config,
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-config/accessory-feature")
@with_log_context(
    event_action="user_config.accessory_feature.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_accessory_feature_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy trạng thái bật/tắt chức năng tư vấn phụ kiện của một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/config/accessory-feature/{user_id}",
            method="GET",
            user_id=user_id
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-config/all")
@with_log_context(
    event_action="user_config.all.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_all_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy tất cả cấu hình của một người dùng.
    Gọi các API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        # Gọi tất cả các API config
        persona_config = await call_chatbot_api(f"/config/persona/{user_id}", "GET", user_id=user_id)
        prompt_config = await call_chatbot_api(f"/config/prompt/{user_id}", "GET", user_id=user_id)
        service_config = await call_chatbot_api(f"/config/service-feature/{user_id}", "GET", user_id=user_id)
        accessory_config = await call_chatbot_api(f"/config/accessory-feature/{user_id}", "GET", user_id=user_id)
        
        # Tổng hợp kết quả
        return {
            "ai_name": persona_config.get("ai_name"),
            "ai_role": persona_config.get("ai_role"),
            "custom_prompt": prompt_config.get("custom_prompt"),
            "service_feature_enabled": service_config.get("enabled"),
            "accessory_feature_enabled": accessory_config.get("enabled")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/chatbot-js-agent/create")
@with_log_context(
    event_action="user_config.chatbot_js_agent.create",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def create_chatbot_js_agent(
    chatbot_callout: str,
    chatbot_message_default: str,
    chatbot_icon_url: str,
    chatbot_name: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Tạo chatbot JS cho một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/settings",
            method="POST",
            data={
                "chatbot_callout": chatbot_callout,
                "chatbot_message_default": chatbot_message_default,
                "chatbot_icon_url": chatbot_icon_url,
                "chatbot_name": chatbot_name,
                "customer_id": user_id
            },
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chatbot-js-agent/get")
@with_log_context(
    event_action="user_config.chatbot_js_agent.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_chatbot_js_agent(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy chatbot JS cho một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/settings/{user_id}",
            method="GET",
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/chatbot-js-agent/update")
@with_log_context(
    event_action="user_config.chatbot_js_agent.update",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def update_chatbot_js_agent(
    chatbot_callout: str,
    chatbot_message_default: str,
    chatbot_icon_url: str,
    chatbot_name: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Cập nhật chatbot JS cho một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/settings/{user_id}",
            method="PUT",
            data={
                "chatbot_callout": chatbot_callout,
                "chatbot_message_default": chatbot_message_default,
                "chatbot_icon_url": chatbot_icon_url,
                "chatbot_name": chatbot_name
            },
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/chatbot-js-agent/upload-icon")
@with_log_context(
    event_action="user_config.chatbot_js_agent.upload_icon",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def upload_chatbot_js_agent_icon(
    icon: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Tải lên icon cho chatbot JS của một người dùng.
    Gọi API từ ChatbotMobileStore.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_api(
            endpoint=f"/settings/{user_id}/upload-icon",
            method="POST",
            file=icon,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/chatbot-js-custom/create")
@with_log_context(
    event_action="user_config.chatbot_js_custom.create",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def create_chatbot_js_custom(
    chatbot_callout: str,
    chatbot_message_default: str,
    chatbot_icon_url: str,
    chatbot_name: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Tạo chatbot JS Custom cho một người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_custom_api(
            endpoint=f"/settings/{user_id}",
            method="POST",
            data={
                "chatbot_callout": chatbot_callout,
                "chatbot_message_default": chatbot_message_default,
                "chatbot_icon_url": chatbot_icon_url,
                "chatbot_name": chatbot_name,
            },
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/chatbot-js-custom/get")
@with_log_context(
    event_action="user_config.chatbot_js_custom.get",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def get_chatbot_js_custom(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy chatbot JS Custom cho một người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_custom_api(
            endpoint=f"/settings/{user_id}",
            method="GET",
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/chatbot-js-custom/upload-icon")
@with_log_context(
    event_action="user_config.chatbot_js_custom.upload_icon",
    event_category="user_config",
    source_layer="controller",
    source_controller="user_config_controller",
)
async def upload_chatbot_js_custom_icon(
    icon: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Tải lên icon cho chatbot JS Custom của một người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        # Lấy user_id từ current_user (đã được giải mã từ token)
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
            
        result = await call_chatbot_custom_api(
            endpoint=f"/settings/{user_id}/upload-icon",
            method="POST",
            file=icon,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


