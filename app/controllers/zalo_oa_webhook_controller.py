from __future__ import annotations
import hmac
import hashlib
import json
import logging
from typing import Optional, Dict, Any
import re
import unicodedata

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import httpx

from app.database.database import get_db
from app.models.oa_webhook_event import OaWebhookEvent
from app.configs.settings import settings
from app.models.oa_account import OaAccount
from app.models.oa_token import OaToken
from app.models.oa_blocked_user import OaBlockedUser
from app.repositories.user_api_key_repository import UserApiKeyRepository
from app.services.zalo_oa_service import ZaloOAService
from app.tasks.zalo_oa_tasks import generate_and_send_zalo_oa_reply
from app.logging.decorators import with_log_context

try:
    import redis as redis_lib
except Exception:  # pragma: no cover
    redis_lib = None

router = APIRouter()

logger = logging.getLogger(__name__)


def verify_signature(body_bytes: bytes, signature: Optional[str], headers: Optional[Dict[str, Any]] = None) -> bool:
    # Prefer OA secret key per Zalo spec; fallback to webhook secret if provided
    secret = settings.ZALO_OA_WEBHOOK_SECRET
    if not secret:
        return True  # Signature not configured, skip
    if not signature:
        return False

    # Extract signature value (Zalo sends like: "mac=<hex>")
    sig_value = signature.strip()
    if sig_value.lower().startswith("mac="):
        sig_value = sig_value.split("=", 1)[1].strip()
    sig_value = sig_value.lower()

    # Parse payload to get app_id and timestamp
    # Zalo formula: sha256(appId + data + timestamp + OAsecretKey)
    try:
        data_str = body_bytes.decode("utf-8")
        payload = json.loads(data_str) if data_str else {}

        # app_id from payload, fallback to configured app id
        app_id = str(payload.get("app_id") or settings.ZALO_OA_APP_ID or "")

        # timestamp variations that Zalo might use (body + potential headers)
        body_ts_val = (
            payload.get("timestamp")
            or payload.get("timeStamp")
            or payload.get("ts")
            or payload.get("time_stamp")
            or payload.get("timestamp_ms")
            or None
        )
        header_ts_val = None
        ts_source = "body"
        if headers:
            try:
                h_lc = {str(k).lower(): v for k, v in headers.items()}
                for hk in [
                    "x-zevent-timestamp",
                    "x-request-timestamp",
                    "x-zrequest-timestamp",
                    "x-time",
                    "x-timestamp",
                ]:
                    if hk in h_lc and h_lc[hk]:
                        header_ts_val = h_lc[hk]
                        break
            except Exception:
                header_ts_val = None
        ts_val = body_ts_val or header_ts_val or ""
        ts_source = "body" if body_ts_val is not None else ("header" if header_ts_val is not None else "none")
        timestamp = str(ts_val)

        # Compute expected from raw body
        to_hash_raw = app_id + data_str + timestamp + secret
        expected_raw = hashlib.sha256(to_hash_raw.encode("utf-8")).hexdigest()

        if hmac.compare_digest(expected_raw, sig_value):
            return True

        # Try with minified JSON (no spaces) in case of body formatting differences
        try:
            data_min = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            to_hash_min = app_id + data_min + timestamp + secret
            expected_min = hashlib.sha256(to_hash_min.encode("utf-8")).hexdigest()
        except Exception:
            data_min = None
            expected_min = None

        matched_min = expected_min is not None and hmac.compare_digest(expected_min, sig_value)
        if not matched_min:
            logger.warning(
                "Zalo signature mismatch: app_id=%s timestamp=%s (src=%s) secret=%s body_len=%d provided=%s expected_raw=%s expected_min=%s",
                app_id,
                timestamp,
                ts_source,
                "ZALO_OA_SECRET_KEY" if settings.ZALO_OA_SECRET_KEY else "ZALO_OA_WEBHOOK_SECRET",
                len(body_bytes),
                sig_value[:16] if sig_value else None,
                (expected_raw[:16] if expected_raw else None),
                (expected_min[:16] if expected_min else None),
            )
        return matched_min
    except Exception:
        logger.exception("verify_signature exception")
        return False


@router.post("/webhook")
async def zalo_oa_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Zalo OA Webhook receiver.
    
    This endpoint receives webhook events from Zalo OA.
    Must return 200 quickly to acknowledge receipt.
    """
    try:
        body_bytes = await request.body()
    except Exception as e:
        logger.error(f"Failed to read request body: {e}")
        return {"error": 0, "message": "success"}
    
    # Get signature from headers (Zalo uses various header names)
    headers_dict = dict(request.headers)
    x_zevent_signature = (
        headers_dict.get("x-zevent-signature")
        or headers_dict.get("X-ZEvent-Signature") 
        or headers_dict.get("mac")
        or None
    )
    
    if not verify_signature(body_bytes, x_zevent_signature, headers=headers_dict):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")

    try:
        payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
    except Exception as e:
        logger.error(f"Failed to parse JSON body: {e}")
        return {"error": 0, "message": "success"}

    # Dedup key best-effort
    dedupe_id = None
    try:
        dedupe_id = (
            payload.get("event_id")
            or payload.get("message", {}).get("msg_id")
            or payload.get("msg_id")
        )
    except Exception:
        pass

    event = OaWebhookEvent(
        event_type=((payload.get("event_name") or payload.get("event")) if isinstance(payload, dict) else None),
        dedupe_id=str(dedupe_id) if dedupe_id else None,
        payload=payload,
    )
    # Dedupe: if dedupe_id exists, skip insert
    if event.dedupe_id:
        try:
            res = await db.execute(select(OaWebhookEvent.id).where(OaWebhookEvent.dedupe_id == event.dedupe_id))
            if res.scalar() is not None:
                return {"ok": True, "deduped": True}
        except Exception:
            # best-effort dedupe; continue to insert
            pass

    db.add(event)
    # Flush to catch unique violations here (so we can rollback and return 200)
    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        return {"ok": True, "deduped": True}

    # Business logic: route events
    try:
        evt_name = (payload.get("event_name") or payload.get("event") or "") if isinstance(payload, dict) else ""
        sender_id = str((payload.get("sender") or {}).get("id")) if isinstance(payload, dict) else None
        recipient_id = str((payload.get("recipient") or {}).get("id")) if isinstance(payload, dict) else None
        msg_obj = (payload.get("message") or {}) if isinstance(payload, dict) else {}
        text = _extract_message_text(payload)
        msg_id = _extract_message_id(payload)
        
        # Debug: log every webhook event
        logger.info(
            "ZaloOA webhook: EVENT received; evt_name=%s sender_id=%s recipient_id=%s msg_id=%s text=%s",
            evt_name, sender_id, recipient_id, msg_id, repr(text[:50]) if text else None,
        )

        # Resolve OA account and partner_id based on event
        oa_id: Optional[str] = None
        partner_id: Optional[str] = None
        if evt_name == "user_send_text":
            # Inbound from user -> OA
            oa_id = recipient_id
            partner_id = sender_id
        elif evt_name == "user_send_image":
            # Inbound image from user -> OA
            oa_id = recipient_id
            partner_id = sender_id
        elif evt_name == "user_received_message":
            # Outbound from OA -> user (self-sent message or echo)
            oa_id = sender_id
            partner_id = recipient_id
        else:
            # Unhandled event types are acknowledged
            return {"ok": True, "ignored": True}

        if not oa_id or not partner_id:
            return {"ok": True, "ignored": True}

        account = await _get_owner_by_oa_id(db, oa_id)
        if not account:
            return {"ok": True, "unknown_oa": True}

        owner_user_id = str(account.owner_user_id)

        # Check if partner is blocked for this OA account
        try:
            blk = await db.execute(
                select(OaBlockedUser.id).where(
                    (OaBlockedUser.oa_account_id == account.id) & (OaBlockedUser.blocked_user_id == partner_id)
                )
            )
            if blk.scalar() is not None:
                logger.info(
                    "ZaloOA webhook: blocked partner -> skip; owner_user_id=%s oa_id=%s partner_id=%s",
                    owner_user_id, oa_id, partner_id,
                )
                return {"ok": True, "blocked": True}
        except Exception:
            # On error checking blocked list, proceed to avoid blocking webhook
            pass

        # Outbound (OA -> user): pause only if NOT sent by our chatbot recently
        if evt_name == "user_received_message":
            # Debug: log full message object for outbound
            logger.info(
                "ZaloOA webhook: OUTBOUND received; owner_user_id=%s oa_id=%s partner_id=%s msg_id=%s text=%s payload_message=%s",
                owner_user_id, oa_id, partner_id, msg_id, repr(text), repr(msg_obj),
            )
            # Try message-id based match first (more reliable), then text-based
            hit_id = bool(msg_id and _bot_is_recently_sent_id(owner_user_id, oa_id, partner_id, str(msg_id)))
            logger.info(
                "ZaloOA webhook: bot-sent ID check; msg_id=%s hit_id=%s",
                msg_id, hit_id,
            )
            if hit_id:
                logger.info(
                    "ZaloOA webhook: outbound from bot (id-match) -> no pause; owner_user_id=%s oa_id=%s partner_id=%s msg_id=%s",
                    owner_user_id, oa_id, partner_id, msg_id,
                )
                return {"ok": True, "from_bot": True, "match": "id"}
            hit_text = bool(text and _bot_is_recently_sent(owner_user_id, oa_id, partner_id, str(text)))
            logger.info(
                "ZaloOA webhook: bot-sent TEXT check; text=%s hit_text=%s",
                repr(text[:100]) if text else None, hit_text,
            )
            if hit_text:
                # Outbound echo of our chatbot message -> do not pause
                logger.info(
                    "ZaloOA webhook: outbound from bot (text-match) -> no pause; owner_user_id=%s oa_id=%s partner_id=%s",
                    owner_user_id, oa_id, partner_id,
                )
                return {"ok": True, "from_bot": True, "match": "text"}
            
            # Check if bot replied to this partner recently (timestamp-based)
            hit_replied = _bot_replied_recently(owner_user_id, partner_id)
            if hit_replied:
                logger.info(
                    "ZaloOA webhook: outbound from bot (replied-recently) -> no pause; owner_user_id=%s oa_id=%s partner_id=%s",
                    owner_user_id, oa_id, partner_id,
                )
                return {"ok": True, "from_bot": True, "match": "replied_recently"}
            
            # Extra debug to understand mismatches
            try:
                logger.info(
                    "ZaloOA webhook: outbound not matched; owner_user_id=%s oa_id=%s partner_id=%s msg_id=%s text_norm_snippet=%s",
                    owner_user_id, oa_id, partner_id, (msg_id or None), (_norm_text(text)[:120] if text else None),
                )
            except Exception:
                pass
            # Pause due to outbound likely from human owner; record until
            until = _now_vn_naive() + timedelta(minutes=10)
            _set_paused(owner_user_id, oa_id, partner_id, minutes=10)
            logger.info(
                "ZaloOA webhook: paused due to outbound; owner_user_id=%s oa_id=%s partner_id=%s until=%s",
                owner_user_id, oa_id, partner_id, until.isoformat(),
            )
            return {"ok": True, "paused": 10, "until": until.isoformat()}

        # Inbound user message (text)
        if evt_name == "user_send_text":
            if not text or not isinstance(text, str) or not text.strip():
                return {"ok": True}

            # Check paused state
            if _is_paused(owner_user_id, oa_id, partner_id):
                pu = _get_pause_until(owner_user_id, oa_id, partner_id)
                logger.info(
                    "ZaloOA webhook: inbound skipped due to paused; owner_user_id=%s oa_id=%s partner_id=%s until=%s",
                    owner_user_id, oa_id, partner_id, (pu.isoformat() if pu else None),
                )
                return {"ok": True, "paused": True, "until": (pu.isoformat() if pu else None)}

            # Get user's API key for X-API-Key auth
            api_key = await _get_user_api_key(db, account.owner_user_id)
            if not api_key:
                return {"ok": True, "no_api_key": True}

            # Decide which chatbot to use: check mobile first, then custom
            use_mobile = True
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    mobile_url = f"{settings.CHATBOT_API_BASE_URL}/customer/status/{owner_user_id}"
                    r1 = await client.get(mobile_url)
                    mobile_status = r1.json() if r1.status_code == 200 else None
                    if _is_stopped_status(mobile_status):
                        use_mobile = False
            except Exception:
                # If status check fails, default to mobile
                use_mobile = True

            # Build base URL of this server to call internal chatbot endpoints with X-API-Key
            try:
                host = (request.headers.get("host") or (
                    f"{request.url.hostname}:{request.url.port}" if getattr(request.url, "port", None) else str(request.url.hostname)
                ))
                base_url = f"{request.url.scheme}://{host}"
            except Exception:
                base_url = ""
            # Enqueue Celery task to generate and send reply asynchronously
            try:
                logger.info(
                    "ZaloOA webhook: ENQUEUE Celery task; owner_user_id=%s oa_id=%s partner_id=%s text=%s base_url=%s",
                    owner_user_id, oa_id, partner_id, repr(text.strip()[:50]) if text else None, base_url,
                )
                result = generate_and_send_zalo_oa_reply.delay(
                    owner_user_id,
                    oa_id,
                    partner_id,
                    text.strip(),
                    False,
                    None,
                    base_url,
                    dedupe_id,
                )
                logger.info("ZaloOA webhook: Celery task enqueued; task_id=%s", result.id if result else None)
            except Exception as e:
                logger.exception("ZaloOA webhook: Failed to enqueue Celery task: %s", e)

        # Inbound user message (image)
        elif evt_name == "user_send_image":
            img_url = _extract_first_image_url(payload)
            # If image URL not present, nothing to do
            if not img_url:
                return {"ok": True}

            # Check paused state
            if _is_paused(owner_user_id, oa_id, partner_id):
                pu = _get_pause_until(owner_user_id, oa_id, partner_id)
                try:
                    logger.info(
                        "ZaloOA webhook: inbound image skipped due to paused; owner_user_id=%s oa_id=%s partner_id=%s until=%s",
                        owner_user_id, oa_id, partner_id, (pu.isoformat() if pu else None),
                    )
                except Exception:
                    pass
                return {"ok": True, "paused": True, "until": (pu.isoformat() if pu else None)}

            # Get user's API key for X-API-Key auth
            api_key = await _get_user_api_key(db, account.owner_user_id)
            if not api_key:
                return {"ok": True, "no_api_key": True}

            # Decide which chatbot to use: check mobile first, then custom
            use_mobile = True
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    mobile_url = f"{settings.CHATBOT_API_BASE_URL}/customer/status/{owner_user_id}"
                    r1 = await client.get(mobile_url)
                    mobile_status = r1.json() if r1.status_code == 200 else None
                    if _is_stopped_status(mobile_status):
                        use_mobile = False
            except Exception:
                use_mobile = True

            # Build base URL
            try:
                host = (request.headers.get("host") or (
                    f"{request.url.hostname}:{request.url.port}" if getattr(request.url, "port", None) else str(request.url.hostname)
                ))
                base_url = f"{request.url.scheme}://{host}"
            except Exception:
                base_url = ""

            msg_text = (text.strip() if isinstance(text, str) else "") or ""
            # Enqueue Celery task to generate and send reply asynchronously (image mode)
            try:
                generate_and_send_zalo_oa_reply.delay(
                    owner_user_id,
                    oa_id,
                    partner_id,
                    msg_text,
                    True,
                    img_url,
                    base_url,
                    dedupe_id,
                )
            except Exception:
                pass

    except Exception as e:
        logger.exception(f"Failed to handle Zalo webhook event: {e}")
        # best-effort: never break webhook contract
        pass

    return {"ok": True}


# ---------------- Conversation pause (in-memory TTL) ----------------
_PAUSE_MAP: dict[tuple[str, str, str], datetime] = {}

def _now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)

def _pause_key(owner_user_id: str, oa_id: str, partner_id: str) -> tuple[str, str, str]:
    return (str(owner_user_id), str(oa_id), str(partner_id))

def _set_paused(owner_user_id: str, oa_id: str, partner_id: str, minutes: int = 10):
    if minutes <= 0:
        return
    _PAUSE_MAP[_pause_key(owner_user_id, oa_id, partner_id)] = _now_vn_naive() + timedelta(minutes=minutes)

def _is_paused(owner_user_id: str, oa_id: str, partner_id: str) -> bool:
    key = _pause_key(owner_user_id, oa_id, partner_id)
    until = _PAUSE_MAP.get(key)
    if not until:
        return False
    now = _now_vn_naive()
    if until > now:
        return True
    # expired -> clear
    _PAUSE_MAP.pop(key, None)
    return False

def _get_pause_until(owner_user_id: str, oa_id: str, partner_id: str) -> datetime | None:
    key = _pause_key(owner_user_id, oa_id, partner_id)
    until = _PAUSE_MAP.get(key)
    if not until:
        return None
    now = _now_vn_naive()
    if until > now:
        return until
    # expired -> clear
    _PAUSE_MAP.pop(key, None)
    return None


# ---------------- Bot-sent messages cache (Redis-based, shared with tasks) ----------------
def _norm_text(t: str) -> str:
    """Normalize text for reliable comparison across send/echo.
    - Unicode normalize (NFC)
    - Remove control/format characters (e.g., zero-width)
    - Collapse whitespace and trim
    """
    try:
        s = unicodedata.normalize("NFC", str(t))
        s = "".join(ch for ch in s if unicodedata.category(ch) not in ("Cf", "Cc", "Cs"))
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        try:
            return str(t).strip()
        except Exception:
            return ""


def _bot_mark_sent(owner_user_id: str, oa_id: str, partner_id: str, text: str, ttl_minutes: int = 5) -> None:
    """Mark message as sent by bot in Redis so webhook can recognize echo."""
    if not redis_lib or not text:
        return
    norm = _norm_text(text)
    if not norm:
        return
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent:{owner_user_id}:{oa_id}:{partner_id}:{hash(norm)}"
        r.set(key, norm[:200], ex=ttl_minutes * 60)
    except Exception:
        pass


def _bot_is_recently_sent(owner_user_id: str, oa_id: str, partner_id: str, text: str) -> bool:
    """Check if message was recently sent by bot (from Redis)."""
    if not redis_lib:
        logger.warning("[BotSentCheck] redis_lib not available")
        return False
    norm = _norm_text(text)
    if not norm:
        logger.warning("[BotSentCheck] norm text is empty, original text=%s", repr(text[:50]) if text else None)
        return False
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent:{owner_user_id}:{oa_id}:{partner_id}:{hash(norm)}"
        exists = r.exists(key) > 0
        logger.info("[BotSentCheck] TEXT key=%s exists=%s", key, exists)
        return exists
    except Exception as e:
        logger.exception("[BotSentCheck] Redis error: %s", e)
        return False


def _bot_mark_sent_id(owner_user_id: str, oa_id: str, partner_id: str, msg_id: str, ttl_minutes: int = 5) -> None:
    """Mark message ID as sent by bot in Redis."""
    if not redis_lib or not msg_id:
        return
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent_id:{owner_user_id}:{oa_id}:{partner_id}:{msg_id}"
        r.set(key, "1", ex=ttl_minutes * 60)
    except Exception:
        pass


def _bot_is_recently_sent_id(owner_user_id: str, oa_id: str, partner_id: str, msg_id: str) -> bool:
    """Check if message ID was recently sent by bot (from Redis)."""
    if not redis_lib:
        logger.warning("[BotSentCheck] redis_lib not available for ID check")
        return False
    if not msg_id:
        logger.warning("[BotSentCheck] msg_id is empty")
        return False
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent_id:{owner_user_id}:{oa_id}:{partner_id}:{msg_id}"
        exists = r.exists(key) > 0
        logger.info("[BotSentCheck] ID key=%s exists=%s", key, exists)
        return exists
    except Exception as e:
        logger.exception("[BotSentCheck] Redis ID error: %s", e)
        return False


def _bot_replied_recently(owner_user_id: str, partner_id: str) -> bool:
    """Check if bot replied to this partner recently (within TTL window)."""
    if not redis_lib:
        return False
    try:
        r = redis_lib.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_replied:{owner_user_id}:{partner_id}"
        exists = r.exists(key) > 0
        logger.info("[BotSentCheck] bot_replied key=%s exists=%s", key, exists)
        return exists
    except Exception as e:
        logger.warning("[BotSentCheck] bot_replied check error: %s", e)
        return False


async def _get_owner_by_oa_id(db: AsyncSession, oa_id: str) -> OaAccount | None:
    res = await db.execute(select(OaAccount).where(OaAccount.oa_id == str(oa_id)))
    return res.scalars().first()


def _is_stopped_status(obj: Any) -> bool:
    try:
        if obj is None:
            return False
        # Common fields: is_stopped / stopped / status contains 'stop'
        if isinstance(obj, dict):
            for k in ["is_stopped", "stopped", "stop", "paused"]:
                if k in obj and bool(obj.get(k)):
                    return True
            st = obj.get("status") or obj.get("state") or obj.get("message")
            if isinstance(st, str) and ("stop" in st.lower() or "pause" in st.lower()):
                return True
        return False
    except Exception:
        return False


def _extract_message_text(payload: Any) -> str | None:
    try:
        if not isinstance(payload, dict):
            return None
        m = payload.get("message") or {}
        candidates = [
            (m.get("text") if isinstance(m, dict) else None),
            (m.get("msg_text") if isinstance(m, dict) else None),
            payload.get("text"),
            payload.get("content"),
            (m.get("message") if isinstance(m, dict) else None),
            (m.get("content") if isinstance(m, dict) else None),
        ]
        for c in candidates:
            if isinstance(c, str) and c.strip():
                return c
        return None
    except Exception:
        return None

def _extract_first_image_url(payload: Any) -> str | None:
    try:
        if not isinstance(payload, dict):
            return None
        m = payload.get("message") or {}
        atts = m.get("attachments") if isinstance(m, dict) else None
        if not isinstance(atts, list):
            return None
        for att in atts:
            if not isinstance(att, dict):
                continue
            t = str(att.get("type") or "").lower()
            if t != "image":
                continue
            p = att.get("payload") or {}
            if isinstance(p, dict):
                url = (
                    p.get("url")
                    or p.get("href")
                    or p.get("thumbnail")
                    or p.get("thumb")
                )
                if isinstance(url, str) and url.strip():
                    return url
        return None
    except Exception:
        return None

def _extract_message_id(payload: Any) -> str | None:
    try:
        if not isinstance(payload, dict):
            return None
        m = payload.get("message") or {}
        for k in [
            "msg_id", "message_id", "msgId", "messageId",
        ]:
            v = None
            if isinstance(m, dict):
                v = m.get(k)
            if not v:
                v = payload.get(k)
            if isinstance(v, (str, int)):
                return str(v)
        return None
    except Exception:
        return None


async def _call_mobile_chatbot(self_base_url: str, api_key: str, thread_id: str, text: str, image_url: Optional[str] = None, image_base64: Optional[str] = None) -> str | None:
    url = f"{self_base_url}/api/v1/chatbot/chat"
    payload = {
        "query": text,
        "llm_provider": "google_genai",
        "thread_id": thread_id,
        "platform": "zalo_oa",
    }
    if image_url:
        payload["image_url"] = image_url
    if image_base64:
        payload["image_base64"] = image_base64
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(url, json=payload, headers=headers)
            if r.status_code != 200:
                return None
            data = r.json()
            # ResponseModel.success wraps data
            body = data.get("data") if isinstance(data, dict) else None
            if isinstance(body, dict):
                # ChatbotMobile typically returns {"response": "..."}
                return body.get("response") or body.get("reply") or body.get("message")
            return None
    except Exception:
        return None


async def _call_custom_chatbot(self_base_url: str, api_key: str, thread_id: str, text: str, image_url: Optional[str] = None) -> str | None:
    url = f"{self_base_url}/api/v1/chatbot-linhkien/chat"
    form = {
        "message": text,
        "model_choice": "gemini",
        "session_id": thread_id,
        "platform": "zalo_oa",
    }
    if image_url:
        form["image_url"] = image_url
    headers = {"X-API-Key": api_key}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(url, data=form, headers=headers)
            if r.status_code != 200:
                return None
            data = r.json()
            # ChatbotCustom returns raw JSON, prefer 'reply'
            if isinstance(data, dict):
                return data.get("reply") or data.get("response") or data.get("message")
            return None
    except Exception:
        return None


async def _send_zalo_reply(db: AsyncSession, account: OaAccount, owner_user_id: str, oa_id: str, to_user_id: str, text: str) -> None:
    if not text:
        return
    res = await db.execute(select(OaToken).where(OaToken.oa_account_id == account.id).order_by(desc(OaToken.created_at)))
    token = res.scalars().first()
    if not token:
        return
    try:
        # Pre-mark to handle extremely fast webhook echo
        _bot_mark_sent(owner_user_id, oa_id, to_user_id, text, ttl_minutes=5)
        result = await ZaloOAService.send_text_message(token.access_token, to_user_id, text)
        # Mark this message as sent by chatbot to avoid pausing when webhook echoes it back
        _bot_mark_sent(owner_user_id, oa_id, to_user_id, text, ttl_minutes=5)
        # Try to record message id from API response if available
        try:
            msg_id = None
            if isinstance(result, dict):
                msg_id = result.get("message_id") or result.get("msg_id")
                if not msg_id and isinstance(result.get("data"), dict):
                    msg_id = result.get("data", {}).get("message_id") or result.get("data", {}).get("msg_id")
            if msg_id:
                _bot_mark_sent_id(owner_user_id, oa_id, to_user_id, str(msg_id), ttl_minutes=5)
        except Exception:
            pass
    except Exception:
        # best-effort
        pass


async def _get_user_api_key(db: AsyncSession, user_id) -> str | None:
    try:
        rec = await UserApiKeyRepository.get_by_user_id(db, user_id)
        return rec.api_key if rec and getattr(rec, "is_active", True) else None
    except Exception:
        return None
