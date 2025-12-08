import asyncio
import logging
import json
import re
import unicodedata
from typing import Optional

import httpx
from sqlalchemy import select, desc

from app.celery_app import celery_app
from app.configs.settings import settings
from app.database.database import async_session
from app.models.oa_account import OaAccount
from app.models.oa_token import OaToken
from app.repositories.user_api_key_repository import UserApiKeyRepository
from app.services.zalo_oa_service import ZaloOAService

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)


# ---------------- Bot-sent marking (Redis-based, shared with webhook) ----------------
def _norm_text(t: str) -> str:
    """Normalize text for reliable comparison across send/echo."""
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


def _bot_mark_sent_redis(owner_user_id: str, oa_id: str, partner_id: str, text: str, ttl_seconds: int = 300) -> None:
    """Mark message as sent by bot in Redis so webhook can recognize echo."""
    if not redis or not text:
        logger.warning("[BotMarkSent] Skipped: redis=%s text=%s", bool(redis), bool(text))
        return
    norm = _norm_text(text)
    if not norm:
        logger.warning("[BotMarkSent] Skipped: norm is empty, original=%s", repr(text[:50]) if text else None)
        return
    try:
        r = redis.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent:{owner_user_id}:{oa_id}:{partner_id}:{hash(norm)}"
        r.set(key, norm[:200], ex=ttl_seconds)
        logger.info("[BotMarkSent] TEXT marked key=%s ttl=%s", key, ttl_seconds)
    except Exception as e:
        logger.exception("[BotMarkSent] Redis error: %s", e)


def _bot_mark_sent_id_redis(owner_user_id: str, oa_id: str, partner_id: str, msg_id: str, ttl_seconds: int = 300) -> None:
    """Mark message ID as sent by bot in Redis."""
    if not redis or not msg_id:
        logger.warning("[BotMarkSent] ID Skipped: redis=%s msg_id=%s", bool(redis), msg_id)
        return
    try:
        r = redis.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_sent_id:{owner_user_id}:{oa_id}:{partner_id}:{msg_id}"
        r.set(key, "1", ex=ttl_seconds)
        logger.info("[BotMarkSent] ID marked key=%s ttl=%s", key, ttl_seconds)
    except Exception as e:
        logger.exception("[BotMarkSent] Redis ID error: %s", e)


def _bot_mark_replied_redis(owner_user_id: str, partner_id: str, ttl_seconds: int = 60) -> None:
    """Mark that bot just replied to this partner (timestamp-based)."""
    if not redis:
        return
    try:
        r = redis.from_url(settings.CELERY_BROKER_URL)
        key = f"zalo:oa:bot_replied:{owner_user_id}:{partner_id}"
        import time
        r.set(key, str(int(time.time())), ex=ttl_seconds)
        logger.info("[BotMarkSent] bot_replied marked key=%s ttl=%s", key, ttl_seconds)
    except Exception as e:
        logger.warning("[BotMarkSent] bot_replied error: %s", e)


def _get_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


async def _async_generate_and_send(
    owner_user_id: str,
    oa_id: str,
    partner_id: str,
    text: str,
    is_image: bool = False,
    image_url: Optional[str] = None,
    base_url: str = "",
) -> dict:
    r = None
    conv_key = None
    lock_key = None
    is_leader = True
    if redis:
        try:
            r = redis.from_url(settings.CELERY_BROKER_URL)
        except Exception:
            r = None
    if r:
        is_leader = False
        try:
            conv_key = f"zalo:oa:agg:{owner_user_id}:{oa_id}:{partner_id}"
            lock_key = f"{conv_key}:lock"
            item = {
                "text": str(text) if isinstance(text, str) else (str(text) if text is not None else ""),
                "is_image": bool(is_image),
                "image_url": str(image_url) if image_url else None,
            }
            r.rpush(conv_key, json.dumps(item))
            r.expire(conv_key, 120)
            got_lock = r.set(lock_key, "1", nx=True, ex=120)
            is_leader = bool(got_lock)
        except Exception:
            is_leader = True
    if not is_leader:
        return {"status": "queued"}

    await asyncio.sleep(30)

    aggregated_items: list[dict] = []
    if r and conv_key:
        try:
            raw_items = r.lrange(conv_key, 0, -1)
            r.delete(conv_key)
        except Exception:
            raw_items = None
        if raw_items:
            for v in raw_items:
                try:
                    if isinstance(v, bytes):
                        v = v.decode("utf-8")
                    obj = json.loads(v)
                    if isinstance(obj, dict):
                        aggregated_items.append(obj)
                except Exception:
                    continue
    if not aggregated_items:
        aggregated_items.append(
            {
                "text": str(text) if isinstance(text, str) else (str(text) if text is not None else ""),
                "is_image": bool(is_image),
                "image_url": str(image_url) if image_url else None,
            }
        )

    parts: list[str] = []
    for it in aggregated_items:
        t = it.get("text")
        if isinstance(t, str) and t.strip():
            parts.append(t.strip())
    if parts:
        text = "\n".join(parts)

    final_image = None
    for it in reversed(aggregated_items):
        u = it.get("image_url")
        if isinstance(u, str) and u.strip():
            final_image = u.strip()
            break
    if final_image:
        image_url = final_image

    if r and lock_key:
        try:
            r.delete(lock_key)
        except Exception:
            pass

    async with async_session() as db:
        # Get X-API-Key
        try:
            rec = await UserApiKeyRepository.get_by_user_id(db, owner_user_id)
            api_key = rec.api_key if rec and getattr(rec, "is_active", True) else None
            if not api_key:
                return {"status": "skipped", "reason": "no_api_key"}
        except Exception:
            return {"status": "skipped", "reason": "api_key_lookup_failed"}

        # Decide which chatbot to use
        use_mobile = True
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                mobile_url = f"{settings.CHATBOT_API_BASE_URL}/customer/status/{owner_user_id}"
                r1 = await client.get(mobile_url)
                mobile_status = r1.json() if r1.status_code == 200 else None
                # If stopped -> fallback to custom
                if isinstance(mobile_status, dict) and (
                    mobile_status.get("is_stopped")
                    or mobile_status.get("stopped")
                    or (isinstance(mobile_status.get("status"), str) and "stop" in mobile_status.get("status").lower())
                ):
                    use_mobile = False
        except Exception:
            use_mobile = True

        reply: Optional[str] = None
        history: Optional[list[dict]] = None
        try:
            res_acc = await db.execute(select(OaAccount).where(OaAccount.oa_id == str(oa_id)))
            account: OaAccount | None = res_acc.scalars().first()
            logger.info("[ZaloOA] Fetching history: oa_id=%s partner_id=%s account_found=%s", oa_id, partner_id, account is not None)
            if account:
                res_tok = await db.execute(
                    select(OaToken).where(OaToken.oa_account_id == account.id).order_by(desc(OaToken.created_at))
                )
                token: OaToken | None = res_tok.scalars().first()
                logger.info("[ZaloOA] Token found=%s has_access_token=%s", token is not None, bool(token and token.access_token))
                if token and token.access_token:
                    try:
                        conv = await ZaloOAService.get_conversation(
                            token.access_token,
                            user_id=str(partner_id),
                            offset=0,
                            count=10,
                        )
                        logger.info("[ZaloOA] get_conversation response type=%s keys=%s", type(conv).__name__, list(conv.keys()) if isinstance(conv, dict) else None)
                        msgs_raw = []
                        if isinstance(conv, dict):
                            data_root = conv.get("data")
                            logger.info("[ZaloOA] data_root type=%s", type(data_root).__name__ if data_root else None)
                            # Zalo OA /conversation direct response: {"data": [...], "error": 0, "message": "Success"}
                            # data_root is the message array directly
                            if isinstance(data_root, list):
                                msgs_raw = data_root
                                logger.info("[ZaloOA] Found msgs in data (list), count=%d", len(msgs_raw))
                            # Internal API wrapper shape: {"data": {"data": [...], ...}}
                            elif isinstance(data_root, dict) and isinstance(data_root.get("data"), list):
                                msgs_raw = data_root.get("data") or []
                                logger.info("[ZaloOA] Found msgs in data.data, count=%d", len(msgs_raw))
                            # Fallbacks for other shapes
                            elif isinstance(data_root, dict):
                                for k in ("messages", "message_list", "items"):
                                    if isinstance(data_root.get(k), list):
                                        msgs_raw = data_root.get(k)
                                        logger.info("[ZaloOA] Found msgs in data.%s, count=%d", k, len(msgs_raw))
                                        break
                        extracted: list[tuple[int | None, str, str]] = []
                        for it in (msgs_raw or []):
                            try:
                                if not isinstance(it, dict):
                                    continue
                                # Text content: Zalo OA /conversation trả về field "message" cho tin nhắn text
                                cand = [
                                    it.get("message"),
                                    it.get("text"),
                                    it.get("msg_text"),
                                    it.get("content"),
                                ]
                                msg_txt = next((c for c in cand if isinstance(c, str) and c.strip()), None)
                                if not msg_txt:
                                    continue
                                # Map role giống listenerManager.js: user (khách) -> "user", OA (bot/staff) -> "assistant"
                                sender_id = it.get("from_id")
                                src_val = it.get("src")
                                role = "assistant"
                                if sender_id is not None and str(sender_id) == str(partner_id):
                                    role = "user"
                                elif src_val == 1:
                                    role = "user"
                                elif src_val == 0:
                                    role = "assistant"
                                ts_candidates = [
                                    it.get("time"),
                                    it.get("timestamp"),
                                ]
                                ts_val = None
                                for t in ts_candidates:
                                    try:
                                        if t is None:
                                            continue
                                        ts_val = int(t)
                                        break
                                    except Exception:
                                        continue
                                extracted.append((ts_val, role, str(msg_txt)))
                            except Exception:
                                continue
                        if extracted:
                            extracted.sort(key=lambda x: (x[0] is None, x[0]))
                            history = [{"role": r, "message": m} for (_, r, m) in extracted]
                        logger.info("[ZaloOA] Extracted history count=%d", len(history) if history else 0)
                    except Exception as e:
                        logger.exception("[ZaloOA] Error fetching conversation: %s", e)
                        history = None
        except Exception as e:
            logger.exception("[ZaloOA] Error in history block: %s", e)
            history = None
        # Call internal endpoints with X-API-Key so that platform gating and LLM key handling are centralized
        if use_mobile:
            url = f"{base_url.rstrip('/')}/api/v1/chatbot/chat"
            payload = {
                "query": text.strip() if isinstance(text, str) else "",
                "llm_provider": "google_genai",
                "thread_id": str(partner_id),
                "platform": "zalo_oa",
            }
            if image_url:
                payload["image_url"] = image_url
            if history is not None:
                payload["history"] = history
            headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
            logger.info("[ZaloOA] Calling chatbot API: url=%s payload_keys=%s history_count=%s", url, list(payload.keys()), len(history) if history else 0)
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    r = await client.post(url, json=payload, headers=headers)
                    logger.info("[ZaloOA] Chatbot API response: status=%s", r.status_code)
                    if r.status_code == 200:
                        resp_json = r.json()
                        logger.info("[ZaloOA] Chatbot API response body keys=%s", list(resp_json.keys()) if isinstance(resp_json, dict) else type(resp_json).__name__)
                        body = resp_json.get("data") if isinstance(resp_json, dict) else None
                        if isinstance(body, dict):
                            reply = body.get("response") or body.get("reply") or body.get("message")
                            logger.info("[ZaloOA] Got reply from chatbot: %s", repr(reply[:100]) if reply else None)
                        else:
                            logger.warning("[ZaloOA] Chatbot response data is not dict: %s", type(body).__name__)
                    else:
                        logger.error("[ZaloOA] Chatbot API error: status=%s body=%s", r.status_code, r.text[:500])
            except Exception as e:
                logger.exception("[ZaloOA] Chatbot API call failed: %s", e)
        else:
            # Check custom status before calling
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    custom_url = f"{settings.CHATBOT_CUSTOM_API_BASE_URL}/bot-status/{owner_user_id}"
                    r2 = await client.get(custom_url)
                    custom_status = r2.json() if r2.status_code == 200 else None
                    stopped = False
                    if isinstance(custom_status, dict):
                        stopped = bool(
                            custom_status.get("is_stopped")
                            or custom_status.get("stopped")
                            or (
                                isinstance(custom_status.get("status"), str)
                                and ("stop" in custom_status.get("status").lower() or "pause" in custom_status.get("status").lower())
                            )
                        )
                    if not stopped:
                        url = f"{base_url.rstrip('/')}/api/v1/chatbot-linhkien/chat"
                        form = {
                            "message": text.strip() if isinstance(text, str) else "",
                            "model_choice": "gemini",
                            "session_id": str(partner_id),
                            "platform": "zalo_oa",
                        }
                        if image_url:
                            form["image_url"] = image_url
                        headers = {"X-API-Key": api_key}
                        async with httpx.AsyncClient(timeout=30.0) as client2:
                            r = await client2.post(url, data=form, headers=headers)
                            if r.status_code == 200 and isinstance(r.json(), dict):
                                data = r.json()
                                reply = data.get("reply") or data.get("response") or data.get("message")
            except Exception:
                pass

        if not reply:
            logger.warning("[ZaloOA] No reply from chatbot, skipping send")
            return {"status": "skipped", "reason": "no_reply"}

        # Find OA token and send
        logger.info("[ZaloOA] Finding OA account to send message: oa_id=%s", oa_id)
        res = await db.execute(select(OaAccount).where(OaAccount.oa_id == str(oa_id)))
        account: OaAccount | None = res.scalars().first()
        if not account:
            logger.error("[ZaloOA] OA account not found: oa_id=%s", oa_id)
            return {"status": "error", "reason": "oa_not_found"}
        res2 = await db.execute(
            select(OaToken).where(OaToken.oa_account_id == account.id).order_by(desc(OaToken.created_at))
        )
        token: OaToken | None = res2.scalars().first()
        if not token:
            logger.error("[ZaloOA] Token not found for account: account_id=%s", account.id)
            return {"status": "error", "reason": "token_missing"}

        logger.info("[ZaloOA] Sending message to Zalo: partner_id=%s reply_length=%d", partner_id, len(reply) if reply else 0)
        try:
            # Pre-mark to handle fast webhook echo
            _bot_mark_sent_redis(owner_user_id, oa_id, partner_id, reply, ttl_seconds=300)
            _bot_mark_replied_redis(owner_user_id, partner_id, ttl_seconds=60)
            result = await ZaloOAService.send_text_message(token.access_token, str(partner_id), reply)
            logger.info("[ZaloOA] send_text_message result: %s", result)
            # Mark again after send
            _bot_mark_sent_redis(owner_user_id, oa_id, partner_id, reply, ttl_seconds=300)
            _bot_mark_replied_redis(owner_user_id, partner_id, ttl_seconds=60)
            # Try to record message id from API response
            try:
                msg_id = None
                if isinstance(result, dict):
                    msg_id = result.get("message_id") or result.get("msg_id")
                    if not msg_id and isinstance(result.get("data"), dict):
                        msg_id = result.get("data", {}).get("message_id") or result.get("data", {}).get("msg_id")
                if msg_id:
                    _bot_mark_sent_id_redis(owner_user_id, oa_id, partner_id, str(msg_id), ttl_seconds=300)
                    logger.info("[ZaloOA] Message sent successfully: msg_id=%s", msg_id)
            except Exception as e:
                logger.warning("[ZaloOA] Error extracting msg_id: %s", e)
        except Exception as e:
            logger.exception("[ZaloOA] send_text_message FAILED: %s", e)
            return {"status": "error", "reason": "send_failed"}

        logger.info("[ZaloOA] Message sent successfully to partner_id=%s", partner_id)
        return {"status": "sent"}


@celery_app.task(
    name="generate_and_send_zalo_oa_reply",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def generate_and_send_zalo_oa_reply(
    self,
    owner_user_id: str,
    oa_id: str,
    partner_id: str,
    text: str,
    is_image: bool = False,
    image_url: Optional[str] = None,
    base_url: str = "",
    dedupe_id: Optional[str] = None,
):
    logger.info(
        "[ZaloOA Task] STARTED; owner_user_id=%s oa_id=%s partner_id=%s text=%s is_image=%s base_url=%s dedupe_id=%s",
        owner_user_id, oa_id, partner_id, repr(text[:50]) if text else None, is_image, base_url, dedupe_id,
    )
    # Best-effort idempotency using Redis
    if redis and dedupe_id:
        try:
            r = redis.from_url(settings.CELERY_BROKER_URL)
            key = f"zalo:oa:{oa_id}:{partner_id}:{dedupe_id}"
            ok = r.set(key, "1", nx=True, ex=60 * 60 * 24 * 2)
            if not ok:
                return {"status": "skipped", "reason": "duplicate"}
        except Exception:
            pass

    loop = _get_loop()
    return loop.run_until_complete(
        _async_generate_and_send(
            owner_user_id=str(owner_user_id),
            oa_id=str(oa_id),
            partner_id=str(partner_id),
            text=str(text),
            is_image=bool(is_image),
            image_url=str(image_url) if image_url else None,
            base_url=str(base_url or ""),
        )
    )
