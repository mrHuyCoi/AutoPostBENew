from __future__ import annotations

import hmac
import hashlib
import json
from typing import Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import logging
from fastapi import APIRouter, Depends, HTTPException, Request, status, UploadFile, File, Form
from fastapi.responses import PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import httpx

from app.database.database import get_db
from app.configs.settings import settings
from app.models.social_account import SocialAccount
from app.models.messenger_message import MessengerMessage
from app.models.messenger_bot_config import MessengerBotConfig
from app.models.messenger_conversation_state import MessengerConversationState
from app.models.user import User
from app.services.chatbot_service import ChatbotService
from app.utils.crypto import token_encryption
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository
from app.repositories.user_bot_control_repository import UserBotControlRepository
from app.models.chatbot_plan import ChatbotPlan  # for typing/loading services
from app.middlewares.subscription_middleware import check_active_subscription
from app.tasks.messenger_tasks import generate_and_send_messenger_reply, send_facebook_comment_reply
from app.logging.decorators import with_log_context

logger = logging.getLogger(__name__)
router = APIRouter()

FACEBOOK_API_BASE_URL = settings.FACEBOOK_API_BASE_URL
VERIFY_TOKEN: Optional[str] = getattr(settings, "FACEBOOK_VERIFY_TOKEN", None)
APP_SECRET: Optional[str] = getattr(settings, "FACEBOOK_APP_SECRET", None)


def _now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


async def _get_pause_ttl_minutes(db: AsyncSession, user_id, page_id: str) -> int:
    """
    Lấy TTL tạm dừng (phút) từ cấu hình bot theo từng Page. Mặc định 10 nếu không có cấu hình.
    TTL = 0 nghĩa là tắt tính năng auto-pause.
    """
    stmt = select(MessengerBotConfig).where(
        MessengerBotConfig.user_id == user_id,
        MessengerBotConfig.page_id == page_id,
    )
    res = await db.execute(stmt)
    cfg = res.scalars().first()
    if not cfg:
        return 10
    # If value is None (unset), default to 10. If explicitly 0, keep 0 (feature off).
    try:
        raw = getattr(cfg, "pause_ttl_minutes", None)
        ttl = int(raw) if raw is not None else 10
    except Exception:
        ttl = 10
    return max(ttl, 0)


async def _set_paused(db: AsyncSession, user_id, page_id: str, psid: str, ttl_minutes: int, reason: str = "human_takeover") -> None:
    """
    Đặt paused_until = now + ttl phút cho hội thoại (user_id, page_id, psid).
    Nếu ttl_minutes <= 0 thì không làm gì.
    """
    if ttl_minutes <= 0:
        return
    now = _now_vn_naive()
    paused_until = now + timedelta(minutes=ttl_minutes)
    # upsert
    stmt = select(MessengerConversationState).where(
        MessengerConversationState.user_id == user_id,
        MessengerConversationState.page_id == page_id,
        MessengerConversationState.psid == psid,
    )
    res = await db.execute(stmt)
    st = res.scalars().first()
    if not st:
        st = MessengerConversationState(
            user_id=user_id,
            page_id=page_id,
            psid=psid,
            paused_until=paused_until,
            reason=reason,
        )
        db.add(st)
    else:
        st.paused_until = paused_until
        st.reason = reason
    await db.commit()


async def _is_paused(db: AsyncSession, user_id, page_id: str, psid: str) -> bool:
    """
    Kiểm tra hội thoại có đang paused không; nếu hết hạn thì tự clear.
    """
    now = _now_vn_naive()
    stmt = select(MessengerConversationState).where(
        MessengerConversationState.user_id == user_id,
        MessengerConversationState.page_id == page_id,
        MessengerConversationState.psid == psid,
    )
    res = await db.execute(stmt)
    st = res.scalars().first()
    if not st or not st.paused_until:
        return False
    if st.paused_until > now:
        return True
    # Hết hạn: clear
    st.paused_until = None
    await db.commit()
    return False

def _extract_psid_from_event(event) -> str | None:
    """
    Lấy PSID từ event một cách an toàn.
    - Echo (page -> user): PSID nằm ở recipient.id
    - Inbound (user -> page): PSID nằm ở sender.id
    """
    try:
        msg = event.get("message", {}) or {}
        if msg.get("is_echo"):
            return event.get("recipient", {}).get("id")
        return event.get("sender", {}).get("id")
    except Exception:
        return None


def _verify_signature(raw_body: bytes, signature_header: Optional[str]) -> bool:
    if not signature_header or not APP_SECRET:
        # If no signature or secret configured, skip verification
        return True
    try:
        method, signature_hash = signature_header.split("=", 1)
        if method != "sha256":
            return False
        expected = hmac.new(APP_SECRET.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature_hash, expected)
    except Exception:
        return False


async def _get_user_and_page_token_by_page_id(db: AsyncSession, page_id: str) -> tuple[User | None, str | None, SocialAccount | None]:
    stmt = select(SocialAccount).where(
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    result = await db.execute(stmt)
    acc: SocialAccount | None = result.scalars().first()
    if not acc:
        return None, None, None
    # Return persisted User object from DB
    user: User | None = await db.get(User, acc.user_id)
    return user, token_encryption.decrypt(acc.access_token) if acc.access_token else None, acc


async def _get_scopes_from_subscription(db: AsyncSession, user_id) -> list[str]:
    sub = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, user_id)
    if not sub or not sub.plan or not sub.plan.services:
        return ["*"]  # fallback to full access if unknown
    return [srv.name for srv in sub.plan.services]


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
    # Decide which chatbot to use based on toggles
    cfg = None
    stmt = select(MessengerBotConfig).where(
        MessengerBotConfig.user_id == user_id,
        MessengerBotConfig.page_id == page_id,
    )
    res = await db.execute(stmt)
    cfg = res.scalars().first()

    # Defaults: mobile on, custom off
    mobile_on = True
    custom_on = False
    if cfg:
        mobile_on = bool(cfg.mobile_enabled)
        custom_on = bool(cfg.custom_enabled)

    if mobile_on and custom_on:
        return "mobile"  # default priority when both enabled
    if custom_on:
        return "custom"
    # else
    return "mobile"


async def _generate_reply(
    db: AsyncSession,
    user_id,
    page_id: str,
    psid: str,
    message_text: str,
) -> Optional[str]:
    # Platform gating: if Messenger platform is disabled for this user, skip auto-reply
    try:
        enabled = await UserBotControlRepository.is_enabled(db, user_id, "messenger")
        if not enabled:
            return None
    except Exception:
        # On error checking control, be safe and allow processing
        pass
    chatbot = await _choose_chatbot(db, user_id, page_id)

    # Fetch user API key (prefer Gemini)
    # We only have a lightweight User; load full if needed
    # Use a direct query to get user api keys
    from app.models.user import User as UserModel
    user: UserModel = await db.get(UserModel, user_id)
    api_key_enc = user.gemini_api_key if user and user.gemini_api_key else user.openai_api_key if user else None
    if not api_key_enc:
        return None
    try:
        api_key = token_encryption.decrypt(api_key_enc)
    except Exception:
        return None

    customer_id = str(user_id)
    thread_id = psid  # use PSID as thread/session id

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
            )
            return res.get("response") if isinstance(res, dict) else None
        except Exception:
            return None
    else:
        # custom (linh kiện)
        try:
            from app.configs.settings import settings as cfg
            url = f"{cfg.CHATBOT_CUSTOM_API_BASE_URL}/chat/{customer_id}"
            form_data = {
                "message": message_text,
                "model_choice": "gemini",
                "api_key": api_key,
                "session_id": thread_id,
            }
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, data=form_data)
                if resp.status_code != 200:
                    return None
                data = resp.json()
                # ChatbotCustom returns ChatResponse schema with 'reply'
                return data.get("reply") if isinstance(data, dict) else None
        except Exception:
            return None


async def _send_message_to_psid(page_access_token: str, psid: str, text: str) -> None:
    if not text:
        return
    url = f"{FACEBOOK_API_BASE_URL}/me/messages"
    payload = {
        "recipient": {"id": psid},
        "message": {"text": text},
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(url, params={"access_token": page_access_token}, json=payload)
        r.raise_for_status()


@router.get("/webhook")
@with_log_context(
    event_action="messenger.webhook.verify",
    event_category="messenger_webhook",
    source_layer="controller",
    source_controller="messenger_webhook_controller",
)
async def verify_webhook(
    request: Request,
):
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")

    if mode and token and mode == "subscribe" and token == VERIFY_TOKEN:
        # Return raw challenge as plain text per FB requirements
        ch = challenge or ""
        return PlainTextResponse(content=str(ch), status_code=200)
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Verification failed")


async def _process_feed_changes(db: AsyncSession, page_id: str | None, entry: dict) -> None:
    # Handle feed comment changes (Page comments)
    changes = entry.get("changes", [])
    for change in changes:
        try:
            if change.get("field") != "feed":
                continue
            value = change.get("value") or {}
            if (value.get("item") or "") != "comment":
                continue
            verb = value.get("verb") or ""
            if verb not in ("add", "created"):
                # Ignore edits/deletes
                continue

            comment_id = value.get("comment_id") or value.get("id")
            message = value.get("message") or ""
            from_obj = value.get("from") or {}
            author_id = str(from_obj.get("id") or "")
            author_name = from_obj.get("name")
            # ID của bài đăng gốc mà comment này thuộc về (nếu có)
            post_id = value.get("post_id") or value.get("parent_id")

            # Skip if comment is from the Page itself to avoid loops
            if author_id and page_id and author_id == page_id:
                continue

            if not comment_id or not message:
                continue

            # Resolve account by Page ID from entry
            user_obj, _, _ = await _get_user_and_page_token_by_page_id(db, page_id or "")
            if not user_obj:
                continue

            dedupe_id = str(comment_id)
            try:
                logger.info(
                    "[FB_COMMENT] enqueue reply task user_id=%s page_id=%s comment_id=%s dedupe_id=%s message=%s",
                    getattr(user_obj, "id", None),
                    page_id,
                    comment_id,
                    dedupe_id,
                    (message[:200] if message else ""),
                )
                send_facebook_comment_reply.delay(
                    str(user_obj.id),
                    str(page_id or ""),
                    str(comment_id),
                    str(message),
                    dedupe_id,
                    str(author_name) if author_name is not None else None,
                    str(post_id) if post_id is not None else None,
                )
                logger.info(
                    "[FB_COMMENT] enqueue send_facebook_comment_reply OK for comment_id=%s",
                    comment_id,
                )
            except Exception as e:
                # best-effort, do not break webhook
                try:
                    logger.exception(
                        "[FB_COMMENT] failed to enqueue send_facebook_comment_reply for comment_id=%s error=%s",
                        comment_id,
                        e,
                    )
                except Exception:
                    pass
        except Exception:
            # Ignore single change failure
            continue


async def _process_single_messaging_event(db: AsyncSession, page_id: str | None, event: dict) -> None:
    try:
        sender_id = event.get("sender", {}).get("id")
        recipient_id = event.get("recipient", {}).get("id")
        timestamp_ms = event.get("timestamp")


        # Handle delivery/read receipts first (they do not include 'message')
        delivery_obj = event.get("delivery")
        if delivery_obj:
            # Resolve account by Page ID from entry
            user_obj, _, _ = await _get_user_and_page_token_by_page_id(db, page_id or "")
            if user_obj:
                mids = delivery_obj.get("mids") or []
                if mids:
                    from sqlalchemy import select as _select
                    for d_mid in mids:
                        existing = await db.execute(
                            _select(MessengerMessage).where(
                                MessengerMessage.message_mid == d_mid,
                                MessengerMessage.direction == "out",
                                MessengerMessage.user_id == user_obj.id,
                            )
                        )
                        mm = existing.scalars().first()
                        if mm:
                            if mm.status != "read":  # keep 'read' as highest state
                                mm.status = "delivered"
                        else:
                            # No echo/outbound persisted for this MID -> likely human-sent from page tools
                            psid = sender_id or _extract_psid_from_event(event)
                            if psid:
                                try:
                                    ttl = await _get_pause_ttl_minutes(db, user_obj.id, page_id or "")
                                    logger.info(
                                        "Auto-pause decision: user_id=%s page_id=%s psid=%s ttl=%s reason=human_delivery",
                                        user_obj.id,
                                        page_id,
                                        psid,
                                        ttl,
                                    )
                                except Exception:
                                    ttl = await _get_pause_ttl_minutes(db, user_obj.id, page_id or "")
                                await _set_paused(db, user_obj.id, page_id or "", psid, ttl, reason="human_delivery")
                    await db.commit()
            return

        read_obj = event.get("read")
        if read_obj:
            # Resolve account by Page ID from entry
            user_obj, _, _ = await _get_user_and_page_token_by_page_id(db, page_id or "")
            if user_obj:
                watermark = read_obj.get("watermark")
                # For read: sender.id is PSID
                psid = sender_id or _extract_psid_from_event(event)
                if watermark and psid:
                    from sqlalchemy import select as _select
                    res = await db.execute(
                        _select(MessengerMessage).where(
                            MessengerMessage.user_id == user_obj.id,
                            MessengerMessage.page_id == page_id,
                            MessengerMessage.direction == "out",
                            MessengerMessage.recipient_id == psid,
                            MessengerMessage.timestamp_ms <= watermark,
                        )
                    )
                    for mm in res.scalars().all():
                        mm.status = "read"
                    await db.commit()
            return

        # Process message events: handle echo first (logging + auto-pause), then inbound text
        message_obj = event.get("message")
        if not message_obj:
            return
        if message_obj.get("is_echo"):
            # Store echo as outgoing for record; if echo không phải của app => auto-pause TTL
            # Quan trọng: tra cứu theo PAGE_ID (entry.id), không dùng recipient_id (PSID)
            user_obj, page_token, acc = await _get_user_and_page_token_by_page_id(db, page_id or "")
            if user_obj:
                # Avoid duplicate by mid
                mid = message_obj.get("mid")
                if mid:
                    from sqlalchemy import select as _select
                    existing = await db.execute(_select(MessengerMessage).where(MessengerMessage.message_mid == mid))
                    already = existing.scalars().first() is not None
                else:
                    already = False
                # Lưu echo out nếu chưa có
                if not already:
                    msg = MessengerMessage(
                        user_id=user_obj.id,
                        page_id=page_id,  # always store Page ID here
                        sender_id=sender_id,
                        recipient_id=recipient_id or page_id,
                        message_mid=mid,
                        message_text=message_obj.get("text"),
                        attachments=message_obj.get("attachments"),
                        timestamp_ms=timestamp_ms,
                        direction="out",
                        status="replied",
                    )
                    db.add(msg)
                    await db.commit()

                # Phát hiện echo từ người thật (không phải app hiện tại)
                app_id = message_obj.get("app_id")
                is_human_echo = (app_id is None) or (str(app_id) != str(settings.FACEBOOK_APP_ID))
                if is_human_echo:
                    ttl = await _get_pause_ttl_minutes(db, user_obj.id, page_id)
                    # PSID trong echo là recipient.id
                    psid = _extract_psid_from_event(event) or recipient_id or ""
                    if psid:
                        try:
                            logger.info(
                                "Auto-pause decision: user_id=%s page_id=%s psid=%s ttl=%s reason=human_echo",
                                user_obj.id,
                                page_id,
                                psid,
                                ttl,
                            )
                        except Exception:
                            pass
                        await _set_paused(db, user_obj.id, page_id, psid, ttl, reason="human_echo")
            return

        text = message_obj.get("text")
        if not text:
            # Optionally ignore non-text attachments for now
            return

        # Find user and page access token
        user_obj, page_token, acc = await _get_user_and_page_token_by_page_id(db, recipient_id or page_id or "")
        if not user_obj or not page_token:
            # Cannot process without mapping or token; skip persisting to avoid orphaned records
            return

        # Save inbound message
        # Avoid duplicate inbound by mid
        mid = message_obj.get("mid")
        if mid:
            from sqlalchemy import select as _select
            existing_in = await db.execute(_select(MessengerMessage).where(MessengerMessage.message_mid == mid))
            ex = existing_in.scalars().first()
            if ex:
                return

        inbound = MessengerMessage(
            user_id=user_obj.id,
            page_id=recipient_id or page_id,
            sender_id=sender_id,
            recipient_id=recipient_id or page_id,
            message_mid=mid,
            message_text=text,
            attachments=message_obj.get("attachments"),
            timestamp_ms=timestamp_ms,
            direction="in",
            status="received",
        )
        db.add(inbound)
        await db.commit()

        # Check paused state before generating reply
        page_for_conv = (recipient_id or page_id)
        paused = await _is_paused(db, user_obj.id, page_for_conv, sender_id)
        if paused:
            try:
                logger.info(
                    "Conversation paused: page_id=%s psid=%s - skip auto-reply",
                    page_for_conv,
                    sender_id,
                )
            except Exception:
                pass
            # Đang tạm dừng: không trả lời
            return

        # Generate reply via chatbot
        try:
            generate_and_send_messenger_reply.delay(
                str(user_obj.id),
                str(page_for_conv),
                str(sender_id),
                str(text),
                mid,
            )
        except Exception:
            pass
        # Send reply back to PSID
        # Don't insert outbound here; rely on echo event to record official send
    except Exception as e:
        # Best-effort: do not block webhook on single event failure
        try:
            if "recipient_id" in locals() or "page_id" in locals():
                # If we already have a record with the same mid, update it instead of inserting a duplicate
                mid_existing = event.get("message", {}).get("mid") if event.get("message") else None
                if mid_existing:
                    from sqlalchemy import select as _select
                    result = await db.execute(_select(MessengerMessage).where(MessengerMessage.message_mid == mid_existing))
                    existing_msg = result.scalars().first()
                    if existing_msg:
                        existing_msg.status = "error"
                        existing_msg.error = str(e)
                        await db.commit()
                        return

                fail = MessengerMessage(
                    user_id=user_obj.id if "user_obj" in locals() and user_obj else None,
                    page_id=recipient_id or page_id,
                    sender_id=sender_id,
                    recipient_id=recipient_id or page_id,
                    message_mid=mid_existing,
                    message_text=event.get("message", {}).get("text") if event.get("message") else None,
                    attachments=event.get("message", {}).get("attachments") if event.get("message") else None,
                    timestamp_ms=event.get("timestamp"),
                    direction="in",
                    status="error",
                    error=str(e),
                )
                db.add(fail)
                await db.commit()
        except Exception:
            pass


async def _process_messaging_events(db: AsyncSession, page_id: str | None, entry: dict) -> None:
    messaging_events = entry.get("messaging", [])
    for event in messaging_events:
        await _process_single_messaging_event(db, page_id, event)


@router.post("/webhook")
async def receive_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Facebook Messenger Webhook receiver.
    
    This endpoint receives webhook events from Facebook Messenger.
    Must return 200 quickly to acknowledge receipt.
    """
    try:
        raw = await request.body()
    except Exception as e:
        logger.error(f"Failed to read request body: {e}")
        return {"status": "EVENT_RECEIVED"}
    
    sig = request.headers.get("x-hub-signature-256")
    if not _verify_signature(raw, sig):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid signature")

    try:
        body = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception as e:
        logger.error(f"Failed to parse JSON body: {e}")
        return {"status": "EVENT_RECEIVED"}

    if body.get("object") != "page":
        # Not a page subscription - still return 200 to avoid Facebook retries
        return {"status": "EVENT_RECEIVED"}

    # Process entries
    entries = body.get("entry", [])
    for entry in entries:
        page_id = str(entry.get("id")) if entry.get("id") else None
        await _process_feed_changes(db, page_id, entry)
        await _process_messaging_events(db, page_id, entry)

    return {"status": "EVENT_RECEIVED"}


# ---------------- Bot config (toggle) endpoints ----------------
@router.get("/bot-config/{page_id}")
@with_log_context(
    event_action="messenger.bot_config.get",
    event_category="messenger_webhook",
    source_layer="controller",
    source_controller="messenger_webhook_controller",
)
async def get_bot_config(
    page_id: str,
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(MessengerBotConfig).where(
        MessengerBotConfig.user_id == current_user.id,
        MessengerBotConfig.page_id == page_id,
    )
    res = await db.execute(stmt)
    cfg = res.scalars().first()
    if not cfg:
        return {"page_id": page_id, "mobile_enabled": True, "custom_enabled": False, "pause_ttl_minutes": 10}
    return {
        "page_id": page_id,
        "mobile_enabled": bool(cfg.mobile_enabled),
        "custom_enabled": bool(cfg.custom_enabled),
        # If None (unset), show 10; if explicitly 0, keep 0
        "pause_ttl_minutes": (int(getattr(cfg, "pause_ttl_minutes", 0)) if getattr(cfg, "pause_ttl_minutes", None) is not None else 10),
    }


@router.put("/bot-config/{page_id}")
@with_log_context(
    event_action="messenger.bot_config.set",
    event_category="messenger_webhook",
    source_layer="controller",
    source_controller="messenger_webhook_controller",
)
async def set_bot_config(
    page_id: str,
    payload: dict,
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
    db: AsyncSession = Depends(get_db),
):
    mobile_enabled = payload.get("mobile_enabled")
    custom_enabled = payload.get("custom_enabled")
    pause_ttl_minutes = payload.get("pause_ttl_minutes")
    if mobile_enabled is None and custom_enabled is None and pause_ttl_minutes is None:
        raise HTTPException(status_code=400, detail="Cần cung cấp ít nhất một trong các trường: mobile_enabled, custom_enabled, pause_ttl_minutes")

    stmt = select(MessengerBotConfig).where(
        MessengerBotConfig.user_id == current_user.id,
        MessengerBotConfig.page_id == page_id,
    )
    res = await db.execute(stmt)
    cfg = res.scalars().first()

    if not cfg:
        cfg = MessengerBotConfig(
            user_id=current_user.id,
            page_id=page_id,
            mobile_enabled=bool(mobile_enabled) if mobile_enabled is not None else True,
            custom_enabled=bool(custom_enabled) if custom_enabled is not None else False,
            pause_ttl_minutes=int(pause_ttl_minutes) if pause_ttl_minutes is not None else 10,
        )
        db.add(cfg)
    else:
        if mobile_enabled is not None:
            cfg.mobile_enabled = bool(mobile_enabled)
        if custom_enabled is not None:
            cfg.custom_enabled = bool(custom_enabled)
        if pause_ttl_minutes is not None:
            try:
                cfg.pause_ttl_minutes = max(int(pause_ttl_minutes), 0)
            except Exception:
                raise HTTPException(status_code=400, detail="pause_ttl_minutes phải là số nguyên không âm")

    await db.commit()

    return {
        "page_id": page_id,
        "mobile_enabled": bool(cfg.mobile_enabled),
        "custom_enabled": bool(cfg.custom_enabled),
        "pause_ttl_minutes": int(getattr(cfg, "pause_ttl_minutes", 10) or 0),
    }


@router.post("/messages/send")
@with_log_context(
    event_action="messenger.messages.send",
    event_category="messenger_webhook",
    source_layer="controller",
    source_controller="messenger_webhook_controller",
)
async def send_message_to_psid(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
):
    page_id = (payload or {}).get("page_id")
    psid = (payload or {}).get("psid")
    text = (payload or {}).get("text")
    image_url = (payload or {}).get("image_url")
    attachment_id = (payload or {}).get("attachment_id")

    if not page_id or not psid:
        raise HTTPException(status_code=400, detail="page_id và psid là bắt buộc")

    provided = [x for x in [text, image_url, attachment_id] if x]
    if len(provided) != 1:
        raise HTTPException(status_code=400, detail="Cần cung cấp đúng một trong: text, image_url, attachment_id")

    # Tra cứu Page thuộc user và giải mã PAT
    stmt = select(SocialAccount).where(
        SocialAccount.user_id == current_user.id,
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    res = await db.execute(stmt)
    acc: SocialAccount | None = res.scalars().first()
    if not acc:
        raise HTTPException(status_code=404, detail="Không tìm thấy Page hoặc Page không hoạt động")

    try:
        pat = token_encryption.decrypt(acc.access_token)
    except Exception:
        raise HTTPException(status_code=400, detail="Token Page không hợp lệ")

    url = f"{FACEBOOK_API_BASE_URL}/me/messages"
    msg_payload: dict
    if text:
        msg_payload = {
            "messaging_type": "RESPONSE",
            "recipient": {"id": psid},
            "message": {"text": str(text)},
        }
    elif image_url:
        msg_payload = {
            "messaging_type": "RESPONSE",
            "recipient": {"id": psid},
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"url": str(image_url), "is_reusable": True},
                }
            },
        }
    else:  # attachment_id
        msg_payload = {
            "messaging_type": "RESPONSE",
            "recipient": {"id": psid},
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"attachment_id": str(attachment_id)},
                }
            },
        }

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(url, params={"access_token": pat}, json=msg_payload)
        if r.status_code != 200:
            try:
                err = r.json()
            except Exception:
                err = {"error": {"message": r.text}}
            raise HTTPException(status_code=r.status_code, detail=err)
        return r.json()


@router.post("/messages/send-image")
@with_log_context(
    event_action="messenger.messages.send_image",
    event_category="messenger_webhook",
    source_layer="controller",
    source_controller="messenger_webhook_controller",
)
async def send_image_message(
    page_id: str = Form(...),
    psid: str = Form(...),
    file: UploadFile = File(...),
    is_reusable: bool = Form(True),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(check_active_subscription(required_max_social_accounts=1)),
):
    stmt = select(SocialAccount).where(
        SocialAccount.user_id == current_user.id,
        SocialAccount.platform == "facebook",
        SocialAccount.account_id == page_id,
        SocialAccount.is_active == True,
    )
    res = await db.execute(stmt)
    acc: SocialAccount | None = res.scalars().first()
    if not acc:
        raise HTTPException(status_code=404, detail="Không tìm thấy Page hoặc Page không hoạt động")

    try:
        pat = token_encryption.decrypt(acc.access_token)
    except Exception:
        raise HTTPException(status_code=400, detail="Token Page không hợp lệ")

    upload_url = f"{FACEBOOK_API_BASE_URL}/{page_id}/message_attachments"
    message_obj = {"attachment": {"type": "image", "payload": {"is_reusable": bool(is_reusable)}}}
    try:
        content = await file.read()
    except Exception:
        raise HTTPException(status_code=400, detail="Không đọc được file")

    files = {
        "filedata": (
            file.filename or "image",
            content,
            file.content_type or "application/octet-stream",
        )
    }
    data = {"message": json.dumps(message_obj)}

    async with httpx.AsyncClient(timeout=30.0) as client:
        ur = await client.post(upload_url, params={"access_token": pat}, data=data, files=files)
        if ur.status_code != 200:
            try:
                err_up = ur.json()
            except Exception:
                err_up = {"error": {"message": ur.text}}
            raise HTTPException(status_code=ur.status_code, detail=err_up)

        up_json = ur.json()
        attachment_id = up_json.get("attachment_id") if isinstance(up_json, dict) else None
        if not attachment_id:
            raise HTTPException(status_code=500, detail="Không lấy được attachment_id")

        send_url = f"{FACEBOOK_API_BASE_URL}/me/messages"
        body = {
            "messaging_type": "RESPONSE",
            "recipient": {"id": psid},
            "message": {
                "attachment": {
                    "type": "image",
                    "payload": {"attachment_id": attachment_id},
                }
            },
        }
        sr = await client.post(send_url, params={"access_token": pat}, json=body)
        if sr.status_code != 200:
            try:
                err_send = sr.json()
            except Exception:
                err_send = {"error": {"message": sr.text}}
            raise HTTPException(status_code=sr.status_code, detail=err_send)

        out = sr.json()
        if isinstance(out, dict):
            out["attachment_id"] = attachment_id
        return out
