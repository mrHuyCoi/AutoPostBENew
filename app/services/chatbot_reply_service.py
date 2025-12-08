from typing import Optional
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, and_, desc

from app.models.messenger_bot_config import MessengerBotConfig
from app.models.user import User as UserModel
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository
from app.repositories.user_bot_control_repository import UserBotControlRepository
from app.services.chatbot_service import ChatbotService
from app.utils.crypto import token_encryption
from app.configs.settings import settings as cfg
from app.models.messenger_message import MessengerMessage


async def _get_access_from_subscription(db: AsyncSession, user_id) -> int:
    sub = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, user_id)
    if not sub or not sub.plan or not sub.plan.services:
        return 123
    ids = {str(s.id) for s in sub.plan.services}
    has_repair = "154519e0-9043-44f4-b67b-fb3d6f901658" in ids
    has_product = "9b1ad1bc-629c-46a9-9503-bd8c985b2407" in ids
    has_accessory = "b807488e-b95e-4e17-bae6-ed7ffd03d8f3" in ids
    if has_accessory:
        return 123
    if has_repair and has_product:
        return 12
    if has_repair:
        return 2
    if has_product:
        return 1
    return 0


async def _choose_chatbot(db: AsyncSession, user_id, page_id: str) -> str:
    stmt = select(MessengerBotConfig).where(
        MessengerBotConfig.user_id == user_id,
        MessengerBotConfig.page_id == page_id,
    )
    res = await db.execute(stmt)
    cfg_row = res.scalars().first()

    mobile_on = True
    custom_on = False
    if cfg_row:
        mobile_on = bool(cfg_row.mobile_enabled)
        custom_on = bool(cfg_row.custom_enabled)

    if mobile_on and custom_on:
        return "mobile"
    if custom_on:
        return "custom"
    return "mobile"


async def generate_reply_for_messenger(
    db: AsyncSession,
    user_id,
    page_id: str,
    psid: str,
    message_text: str,
) -> Optional[str]:
    try:
        enabled = await UserBotControlRepository.is_enabled(db, user_id, "messenger")
        if not enabled:
            return None
    except Exception:
        pass

    chatbot = await _choose_chatbot(db, user_id, page_id)

    user: UserModel = await db.get(UserModel, user_id)
    api_key_enc = user.gemini_api_key if user and user.gemini_api_key else user.openai_api_key if user else None
    if not api_key_enc:
        return None
    try:
        api_key = token_encryption.decrypt(api_key_enc)
    except Exception:
        return None

    customer_id = str(user_id)
    thread_id = psid

    # Build recent chat history from stored Messenger messages (last 10), chronological
    history: Optional[list[dict]] = None
    try:
        stmt = (
            select(MessengerMessage)
            .where(
                MessengerMessage.user_id == user_id,
                MessengerMessage.page_id == page_id,
                or_(
                    and_(MessengerMessage.direction == "in", MessengerMessage.sender_id == psid),
                    and_(MessengerMessage.direction == "out", MessengerMessage.recipient_id == psid),
                ),
            )
            .order_by(desc(MessengerMessage.timestamp_ms))
            .limit(10)
        )
        res = await db.execute(stmt)
        rows = list(res.scalars().all())
        rows.reverse()  # chronological (oldest -> newest)
        hist: list[dict] = []
        for m in rows:
            try:
                txt = m.message_text if getattr(m, "message_text", None) else None
                if not txt:
                    continue
                role = "user" if m.direction == "in" else "assistant"
                hist.append({"role": role, "message": str(txt)})
            except Exception:
                continue
        if hist:
            history = hist
    except Exception:
        history = None

    if chatbot == "mobile":
        try:
            access = await _get_access_from_subscription(db, user_id)
            res = await ChatbotService.chat_with_bot(
                thread_id=thread_id,
                query=message_text,
                customer_id=customer_id,
                llm_provider="google_genai",
                api_key=api_key,
                access=access,
                history=history,
            )
            return res.get("response") if isinstance(res, dict) else None
        except Exception:
            return None


async def generate_reply_for_facebook_comment(
    db: AsyncSession,
    user_id,
    page_id: str,
    comment_id: str,
    comment_text: str,
    post_text: Optional[str] = None,
    post_image_urls: Optional[list[str]] = None,
) -> Optional[str]:
    try:
        enabled = await UserBotControlRepository.is_enabled(db, user_id, "messenger")
        if not enabled:
            return None
    except Exception:
        pass

    chatbot = await _choose_chatbot(db, user_id, page_id)

    user: UserModel = await db.get(UserModel, user_id)
    api_key_enc = user.gemini_api_key if user and user.gemini_api_key else user.openai_api_key if user else None
    if not api_key_enc:
        return None
    try:
        api_key = token_encryption.decrypt(api_key_enc)
    except Exception:
        return None

    customer_id = str(user_id)
    thread_id = f"fb_comment:{page_id}:{comment_id}"

    # Build lightweight history from post content so chatbot hiểu bối cảnh bài đăng
    history: Optional[list[dict]] = None
    if post_text:
        try:
            history = [{"role": "user", "message": f"Nội dung bài đăng: {post_text}"}]
        except Exception:
            history = None

    if chatbot == "mobile":
        try:
            access = await _get_access_from_subscription(db, user_id)
            res = await ChatbotService.chat_with_bot(
                thread_id=thread_id,
                query=comment_text,
                customer_id=customer_id,
                llm_provider="google_genai",
                api_key=api_key,
                access=access,
                image_urls=post_image_urls if post_image_urls else None,
                history=history,
            )
            return res.get("response") if isinstance(res, dict) else None
        except Exception:
            return None
    else:
        try:
            # Với chatbot custom, gom bối cảnh bài đăng vào cùng một message
            parts: list[str] = []
            if post_text:
                parts.append(f"Nội dung bài đăng: {post_text}")
            if post_image_urls:
                try:
                    urls_joined = "\n".join(post_image_urls)
                    parts.append(f"Ảnh bài đăng (URL):\n{urls_joined}")
                except Exception:
                    pass
            parts.append(f"Bình luận của khách: {comment_text}")
            merged_message = "\n\n".join(parts)

            url = f"{cfg.CHATBOT_CUSTOM_API_BASE_URL}/chat/{customer_id}"
            form_data = {
                "message": merged_message,
                "model_choice": "gemini",
                "api_key": api_key,
                "session_id": thread_id,
            }
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, data=form_data)
                if resp.status_code != 200:
                    return None
                data = resp.json()
                return data.get("reply") if isinstance(data, dict) else None
        except Exception:
            return None
        
