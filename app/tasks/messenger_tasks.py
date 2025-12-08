import asyncio
import logging
import json
from typing import Optional

from app.celery_app import celery_app
from app.configs.settings import settings
from app.database.database import async_session
from app.models.social_account import SocialAccount
from app.services.chatbot_reply_service import generate_reply_for_messenger, generate_reply_for_facebook_comment
from app.utils.crypto import token_encryption
import httpx
from sqlalchemy import select

try:
    import redis
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)


def _get_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


async def _async_generate_and_send(user_id: str, page_id: str, psid: str, message_text: str) -> dict:
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
            conv_key = f"messenger:agg:{user_id}:{page_id}:{psid}"
            lock_key = f"{conv_key}:lock"
            item = {
                "text": str(message_text) if isinstance(message_text, str) else (str(message_text) if message_text is not None else ""),
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
                "text": str(message_text) if isinstance(message_text, str) else (str(message_text) if message_text is not None else ""),
            }
        )

    parts: list[str] = []
    for it in aggregated_items:
        t = it.get("text")
        if isinstance(t, str) and t.strip():
            parts.append(t.strip())
    if parts:
        message_text = "\n".join(parts)

    if r and lock_key:
        try:
            r.delete(lock_key)
        except Exception:
            pass

    async with async_session() as db:
        # Generate reply via shared service
        reply: Optional[str] = await generate_reply_for_messenger(
            db=db,
            user_id=user_id,
            page_id=page_id,
            psid=psid,
            message_text=message_text,
        )
        if not reply:
            return {"status": "skipped", "reason": "no_reply"}

        # Resolve Page Access Token by page_id
        stmt = select(SocialAccount).where(
            SocialAccount.platform == "facebook",
            SocialAccount.account_id == page_id,
            SocialAccount.is_active == True,
        )
        res = await db.execute(stmt)
        acc: SocialAccount | None = res.scalars().first()
        if not acc or not acc.access_token:
            return {"status": "error", "reason": "page_token_missing"}
        try:
            page_token = token_encryption.decrypt(acc.access_token)
        except Exception:
            return {"status": "error", "reason": "page_token_decrypt_failed"}

        # Send reply to PSID
        url = f"{settings.FACEBOOK_API_BASE_URL}/me/messages"
        payload = {
            "recipient": {"id": psid},
            "message": {"text": reply},
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(url, params={"access_token": page_token}, json=payload)
            r.raise_for_status()

        return {"status": "sent"}


@celery_app.task(name="generate_and_send_messenger_reply", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={"max_retries": 3})
def generate_and_send_messenger_reply(self, user_id: str, page_id: str, psid: str, message_text: str, mid: Optional[str] = None):
    # Idempotency with Redis SETNX(mid)
    release_key = None
    if redis and mid:
        try:
            r = redis.from_url(settings.CELERY_BROKER_URL)
            key = f"messenger:mid:{mid}"
            ok = r.set(key, "1", nx=True, ex=60 * 60 * 24 * 2)  # 48h TTL
            if not ok:
                return {"status": "skipped", "reason": "duplicate_mid"}
            release_key = key
        except Exception:
            pass

    loop = _get_loop()
    try:
        result = loop.run_until_complete(_async_generate_and_send(user_id, page_id, psid, message_text))
        return result
    finally:
        # Do not delete key to keep dedup window
        pass


async def _async_send_comment_reply(
    user_id: str,
    page_id: str,
    comment_id: str,
    comment_text: str,
    author_name: Optional[str] = None,
    post_id: Optional[str] = None,
) -> dict:
    async with async_session() as db:
        logger.info(
            "[FB_COMMENT] _async_send_comment_reply start user_id=%s page_id=%s comment_id=%s text=%s post_id=%s",
            user_id,
            page_id,
            comment_id,
            (comment_text[:200] if comment_text else ""),
            post_id,
        )
        if not comment_text:
            logger.info("[FB_COMMENT] skip empty comment_text for comment_id=%s", comment_id)
            return {"status": "skipped", "reason": "empty_comment_text"}

        # Resolve Page Access Token by page_id
        stmt = select(SocialAccount).where(
            SocialAccount.platform == "facebook",
            SocialAccount.account_id == page_id,
            SocialAccount.is_active == True,
        )
        res = await db.execute(stmt)
        acc: SocialAccount | None = res.scalars().first()
        if not acc or not acc.access_token:
            logger.warning(
                "[FB_COMMENT] page_token_missing for page_id=%s comment_id=%s",
                page_id,
                comment_id,
            )
            return {"status": "error", "reason": "page_token_missing"}

        try:
            page_token = token_encryption.decrypt(acc.access_token)
        except Exception:
            logger.exception("[FB_COMMENT] page_token_decrypt_failed for page_id=%s comment_id=%s", page_id, comment_id)
            return {"status": "error", "reason": "page_token_decrypt_failed"}

        # Fetch post content (text + images) as context for chatbot if post_id is available
        post_text: Optional[str] = None
        post_image_urls: Optional[list[str]] = None
        if post_id:
            try:
                post_url = f"{settings.FACEBOOK_API_BASE_URL}/{post_id}"
                fields = "message,attachments{media_type,media,url,subattachments{media_type,media,url}}"
                async with httpx.AsyncClient(timeout=20.0) as client:
                    r_post = await client.get(
                        post_url,
                        params={
                            "access_token": page_token,
                            "fields": fields,
                        },
                    )
                    logger.info(
                        "[FB_COMMENT] fetched post context status=%s body=%s",
                        r_post.status_code,
                        r_post.text[:300],
                    )
                    r_post.raise_for_status()
                    data = r_post.json() if r_post.text else {}

                post_text = data.get("message") or None

                attachments = (data.get("attachments") or {}).get("data") or []
                images: list[str] = []
                for att in attachments:
                    try:
                        media = att.get("media") or {}
                        image = media.get("image") or {}
                        src = image.get("src") or att.get("url")
                        if src:
                            images.append(str(src))
                        subatts = (att.get("subattachments") or {}).get("data") or []
                        for sub in subatts:
                            media_s = (sub.get("media") or {}).get("image") or {}
                            src_s = media_s.get("src") or sub.get("url")
                            if src_s:
                                images.append(str(src_s))
                    except Exception:
                        continue
                if images:
                    post_image_urls = images

                logger.info(
                    "[FB_COMMENT] post context extracted for post_id=%s text_len=%s image_count=%s",
                    post_id,
                    len(post_text or ""),
                    len(post_image_urls or []),
                )
            except Exception:
                # Best-effort: if post fetch fails, still continue with just comment_text
                logger.exception("[FB_COMMENT] failed to fetch post context for post_id=%s", post_id)

        reply: Optional[str] = await generate_reply_for_facebook_comment(
            db=db,
            user_id=user_id,
            page_id=page_id,
            comment_id=comment_id,
            comment_text=comment_text,
            post_text=post_text,
            post_image_urls=post_image_urls,
        )
        if not reply:
            logger.info(
                "[FB_COMMENT] no reply generated by chatbot for user_id=%s page_id=%s comment_id=%s",
                user_id,
                page_id,
                comment_id,
            )
            return {"status": "skipped", "reason": "no_reply"}

        url = f"{settings.FACEBOOK_API_BASE_URL}/{comment_id}/comments"

        reply_to_send = reply
        if author_name:
            try:
                name_clean = str(author_name).strip()
            except Exception:
                name_clean = ""
            if name_clean:
                # Prefix reply with @Tên để giống tag tên trong comment
                reply_to_send = f"@{name_clean} {reply}"

        payload = {"message": reply_to_send}

        logger.info(
            "[FB_COMMENT] sending reply to Facebook comment_id=%s page_id=%s url=%s",
            comment_id,
            page_id,
            url,
        )
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(url, params={"access_token": page_token}, json=payload)
            logger.info(
                "[FB_COMMENT] Facebook reply response status=%s body=%s",
                r.status_code,
                r.text[:500],
            )
            r.raise_for_status()

        logger.info(
            "[FB_COMMENT] reply sent successfully for user_id=%s page_id=%s comment_id=%s",
            user_id,
            page_id,
            comment_id,
        )
        return {"status": "sent"}


@celery_app.task(
    name="send_facebook_comment_reply",
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def send_facebook_comment_reply(
    self,
    user_id: str,
    page_id: str,
    comment_id: str,
    comment_text: str,
    dedupe_id: Optional[str] = None,
    author_name: Optional[str] = None,
    post_id: Optional[str] = None,
):
    logger.info(
        "[FB_COMMENT] send_facebook_comment_reply task received user_id=%s page_id=%s comment_id=%s dedupe_id=%s author_name=%s post_id=%s",
        user_id,
        page_id,
        comment_id,
        dedupe_id,
        author_name,
        post_id,
    )
    release_key = None
    if redis and dedupe_id:
        try:
            r = redis.from_url(settings.CELERY_BROKER_URL)
            key = f"facebook:comment:{dedupe_id}"
            ok = r.set(key, "1", nx=True, ex=60 * 60 * 24 * 2)
            if not ok:
                logger.info("[FB_COMMENT] duplicate_comment detected for dedupe_id=%s", dedupe_id)
                return {"status": "skipped", "reason": "duplicate_comment"}
            release_key = key
        except Exception:
            pass

    loop = _get_loop()
    try:
        result = loop.run_until_complete(
            _async_send_comment_reply(
                user_id=str(user_id),
                page_id=str(page_id),
                comment_id=str(comment_id),
                comment_text=str(comment_text),
                author_name=str(author_name) if author_name is not None else None,
                post_id=str(post_id) if post_id is not None else None,
            )
        )
        logger.info(
            "[FB_COMMENT] send_facebook_comment_reply finished for comment_id=%s result=%s",
            comment_id,
            result,
        )
        return result
    finally:
        pass
