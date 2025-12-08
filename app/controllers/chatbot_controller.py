from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import os
import re
import unicodedata
import httpx

from app.database.database import get_db
from app.configs.settings import settings

try:
    import redis as redis_lib
except Exception:
    redis_lib = None
from app.middlewares.api_key_middleware import get_user_for_chatbot
from app.middlewares.auth_middleware import get_current_active_superuser
from app.models.user import User
from app.dto.chatbot_dto import ChatRequest
from app.dto.response import ResponseModel
from app.services.chatbot_service import ChatbotService
from app.services.chatbot_subscription_service import ChatbotSubscriptionService
from app.repositories.user_bot_control_repository import UserBotControlRepository
from app.tasks.chatbot_tasks import chatbot_chat
from app.logging.decorators import with_log_context

logger = logging.getLogger(__name__)
CHATBOT_API_BASE_URL = os.getenv("CHATBOT_API_BASE_URL", "http://localhost:8001")
router = APIRouter(tags=["Chatbot"])


# ---------------- Zalo OA Bot-sent marking ----------------
def _norm_text_for_zalo(t: str) -> str:
    """Normalize text for reliable comparison."""
    try:
        s = unicodedata.normalize("NFC", str(t))
        s = "".join(ch for ch in s if unicodedata.category(ch) not in ("Cf", "Cc", "Cs"))
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(t).strip() if t else ""


def _mark_zalo_oa_bot_sent(owner_user_id: str, partner_id: str, text: str, ttl_seconds: int = 60) -> None:
    """Mark that bot just replied to this partner (timestamp-based, TTL 60s)."""
    if not redis_lib:
        return
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        # Simple key: mark that bot replied to this partner recently
        key = f"zalo:oa:bot_replied:{owner_user_id}:{partner_id}"
        import time
        r.set(key, str(int(time.time())), ex=ttl_seconds)
        logger.info("[ZaloOABotSent] Marked bot replied key=%s ttl=%s", key, ttl_seconds)
    except Exception as e:
        logger.warning("[ZaloOABotSent] Redis error: %s", e)

@router.post("/chat")
@with_log_context(
    event_action="chatbot.chat",
    event_category="chatbot",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def chat_endpoint(
    request: ChatRequest,
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot),
    http_request: Request = None,
):
    try:
        current_user, scopes = auth_result
        platform = getattr(request, 'platform', None)
        subscription = None
        try:
            x_key = http_request.headers.get("x-api-key") if http_request else None
        except Exception:
            x_key = None
        if x_key:
            if not platform:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Thiếu tham số platform")
            enabled = await UserBotControlRepository.is_enabled(db, current_user.id, platform)
            if not enabled:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Nền tảng này đang tắt."
                )
            subscription = await ChatbotSubscriptionService.get_my_active_subscription(db, current_user)
            await ChatbotSubscriptionService.check_and_increment_usage(db, current_user)
        
        if subscription is None:
            subscription = await ChatbotSubscriptionService.get_my_active_subscription(db, current_user)
        
        access_override = None
        if subscription and hasattr(subscription, "max_api_calls") and subscription.max_api_calls is not None and subscription.max_api_calls > 0:
            access_override = 123

        use_env_gemini = False
        needs_decrypt = True
        api_key = request.api_key
        if (
            subscription
            and hasattr(subscription, "max_api_calls")
            and subscription.max_api_calls is not None
            and subscription.max_api_calls > 0
            and request.llm_provider == "google_genai"
        ):
            use_env_gemini = True
        if use_env_gemini:
            api_key = settings.GEMINI_API_KEY
            needs_decrypt = False
        else:
            if not api_key:
                if request.llm_provider == "google_genai":
                    api_key = current_user.gemini_api_key
                elif request.llm_provider == "openai":
                    api_key = current_user.openai_api_key
        
        if api_key and needs_decrypt:
            try:
                from app.utils.crypto import token_encryption
                api_key = token_encryption.decrypt(api_key)
            except Exception as e:
                logger.error(f"Error decrypting API key: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Lỗi khi giải mã API key."
                )
        
        if not api_key:
            if use_env_gemini:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Hệ thống chưa được cấu hình GEMINI_API_KEY."
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Bạn chưa thêm API key bên trang cấu hình."
            )
        
        # Prefer provided thread_id (e.g., from Zalo thread) if available; otherwise use user_id
        thread_id = str(request.thread_id) if getattr(request, 'thread_id', None) else str(current_user.id)
        customer_id = str(current_user.id)
        
        logger.info(f"Chatbot request - User: {current_user.id}, Scopes: {scopes}")
        
        # Async enqueue mode via query param ?async=1 (only for non-stream)
        try:
            async_flag = http_request.query_params.get("async") if http_request else None
        except Exception:
            async_flag = None
        is_async = str(async_flag).lower() in ("1", "true", "yes") if async_flag is not None else False

        if is_async and not request.stream:
            res = chatbot_chat.delay(
                thread_id=thread_id,
                query=request.query,
                customer_id=customer_id,
                llm_provider=request.llm_provider,
                api_key=api_key,
                access=access_override,
                scopes=scopes,
                image_url=getattr(request, 'image_url', None),
                image_urls=getattr(request, 'image_urls', None),
                image_base64=getattr(request, 'image_base64', None),
                history=getattr(request, 'history', None),
            )
            return ResponseModel.success(data={"task_id": res.id, "status": "queued"}, message="Chat enqueued", status_code=202)

        if request.stream:
            return StreamingResponse(
                ChatbotService.stream_chat_with_bot(
                    thread_id=thread_id,
                    query=request.query,
                    customer_id=customer_id,
                    llm_provider=request.llm_provider,
                    api_key=api_key,
                    access=access_override,
                    scopes=scopes,
                    image_url=getattr(request, 'image_url', None),
                    image_urls=getattr(request, 'image_urls', None),
                    image_base64=getattr(request, 'image_base64', None),
                ),
                media_type="text/event-stream"
            )
        else:
            response = await ChatbotService.chat_with_bot(
                thread_id=thread_id,
                query=request.query,
                customer_id=customer_id,
                llm_provider=request.llm_provider,
                api_key=api_key,
                access=access_override,
                scopes=scopes,
                image_url=getattr(request, 'image_url', None),
                image_urls=getattr(request, 'image_urls', None),
                image_base64=getattr(request, 'image_base64', None),
                history=getattr(request, 'history', None),
            )
            
            # Mark bot-sent for Zalo OA to prevent pause on webhook echo
            if platform == "zalo_oa" and response:
                try:
                    reply_text = response.get("response") or response.get("reply") or response.get("message")
                    if reply_text and isinstance(reply_text, str):
                        _mark_zalo_oa_bot_sent(customer_id, thread_id, reply_text)
                        logger.info("[ChatbotController] Marked Zalo OA bot-sent for thread=%s", thread_id)
                except Exception as e:
                    logger.warning("[ChatbotController] Failed to mark bot-sent: %s", e)
            
            return ResponseModel.success(data=response, message="Chatbot response")
    except HTTPException:
        # Re-raise HTTP exceptions without modification
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/chat-history/{thread_id}")
@with_log_context(
    event_action="chatbot.chat_history.get",
    event_category="chatbot",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def get_chat_history_endpoint(
    thread_id: str,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    try:
        current_user, _ = auth_result
        customer_id = str(current_user.id)
        data = await ChatbotService.get_chat_history(customer_id=customer_id, thread_id=thread_id, limit=limit)
        return ResponseModel.success(data=data, message="Chat history")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.post("/clear-history-chat")
@with_log_context(
    event_action="chatbot.chat_history.clear",
    event_category="chatbot",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def clear_history_chat(
    db: AsyncSession = Depends(get_db),
    auth_result: tuple[User, list[str]] = Depends(get_user_for_chatbot)
):
    try:
        current_user, scopes = auth_result
        await ChatbotService.clear_history_chat(current_user)
        return ResponseModel.success(message="Lịch sử chat đã được xóa")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

async def _call_chatbot_api(endpoint: str, method: str = "GET", data: dict | None = None):
    try:
        async with httpx.AsyncClient() as client:
            url = f"{CHATBOT_API_BASE_URL}{endpoint}"
            headers = {"Content-Type": "application/json"}
            if method == "GET":
                resp = await client.get(url)
            elif method == "PUT":
                resp = await client.put(url, json=data, headers=headers)
            elif method == "POST":
                resp = await client.post(url, json=data, headers=headers)
            elif method == "DELETE":
                resp = await client.delete(url)
            else:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Method not supported")
            if resp.status_code >= 400:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@router.get("/admin/instructions")
@with_log_context(
    event_action="chatbot.admin.instructions.list",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_get_instructions(current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api("/instructions", "GET")

@router.get("/admin/instructions/{key}")
@with_log_context(
    event_action="chatbot.admin.instructions.get",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_get_instruction_by_key(key: str, current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api(f"/instructions/{key}", "GET")

@router.post("/admin/instructions")
@with_log_context(
    event_action="chatbot.admin.instructions.create",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_create_instruction(item: dict, current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api("/instructions", "POST", data=item)

@router.put("/admin/instructions")
@with_log_context(
    event_action="chatbot.admin.instructions.update_all",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_update_instructions(update_data: dict, current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api("/instructions", "PUT", data=update_data)

@router.put("/admin/instructions/{key}")
@with_log_context(
    event_action="chatbot.admin.instructions.upsert",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_upsert_instruction(key: str, item: dict, current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api(f"/instructions/{key}", "PUT", data=item)

@router.delete("/admin/instructions/{key}")
@with_log_context(
    event_action="chatbot.admin.instructions.delete",
    event_category="chatbot_admin",
    source_layer="controller",
    source_controller="chatbot_controller",
)
async def admin_delete_instruction(key: str, current_user: User = Depends(get_current_active_superuser)):
    return await _call_chatbot_api(f"/instructions/{key}", "DELETE")