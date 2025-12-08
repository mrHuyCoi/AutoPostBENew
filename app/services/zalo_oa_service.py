from __future__ import annotations
import httpx
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import mimetypes

from app.configs.settings import settings

logger = logging.getLogger(__name__)


class ZaloOAService:
    @staticmethod
    def build_permission_url(app_id: str, redirect_uri: str, code_challenge: str, state: str) -> str:
        base = settings.ZALO_OAUTH_BASE_URL.rstrip("/")
        return (
            f"{base}/v4/oa/permission?app_id={app_id}"
            f"&redirect_uri={httpx.QueryParams({'redirect_uri': redirect_uri})['redirect_uri']}"
            f"&code_challenge={code_challenge}&code_challenge_method=S256&state={state}"
        )

    @staticmethod
    async def exchange_token(code: str, code_verifier: str) -> Dict[str, Any]:
        url = f"{settings.ZALO_OAUTH_BASE_URL.rstrip('/')}/v4/oa/access_token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "secret_key": settings.ZALO_OA_SECRET_KEY or "",
        }
        data = {
            "code": code,
            "app_id": settings.ZALO_OA_APP_ID or "",
            "grant_type": "authorization_code",
            "code_verifier": code_verifier,
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(url, data=data, headers=headers)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def refresh_token(refresh_token: str) -> Dict[str, Any]:
        url = f"{settings.ZALO_OAUTH_BASE_URL.rstrip('/')}/v4/oa/access_token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "secret_key": settings.ZALO_OA_SECRET_KEY or "",
        }
        data = {
            "refresh_token": refresh_token,
            "app_id": settings.ZALO_OA_APP_ID or "",
            "grant_type": "refresh_token",
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(url, data=data, headers=headers)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def get_me(access_token: str) -> Dict[str, Any]:
        url = f"{settings.ZALO_GRAPH_BASE_URL.rstrip('/')}/me"
        headers = {
            "access_token": access_token,
        }
        params = {"fields": "id,name,picture"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def get_oa_info(access_token: str) -> Dict[str, Any]:
        """
        Lấy thông tin Zalo Official Account theo OpenAPI.
        Endpoint: GET /v2.0/oa/getoa
        Header: access_token
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v2.0/oa/getoa"
        headers = {"access_token": access_token}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def list_recent_chat(access_token: str, offset: int = 0, count: int = 5) -> Dict[str, Any]:
        """
        Call Zalo OA OpenAPI to fetch recent chat messages across users.
        Endpoint: GET /v2.0/oa/listrecentchat
        Header: access_token
        Query: data={"offset": <int>, "count": <int>} (max count 10)
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v2.0/oa/listrecentchat"
        headers = {"access_token": access_token}
        # Ensure count does not exceed 10 per docs
        count = max(1, min(int(count), 10))
        params = {"data": json.dumps({"offset": int(offset), "count": count}, separators=(",", ":"))}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def get_conversation(access_token: str, user_id: str, offset: int = 0, count: int = 5) -> Dict[str, Any]:
        """
        Call Zalo OA OpenAPI to fetch messages in a specific conversation with a user.
        Endpoint: GET /v2.0/oa/conversation
        Header: access_token
        Query: data={"user_id": <long>, "offset": <int>, "count": <int>} (max count 10)
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v2.0/oa/conversation"
        headers = {"access_token": access_token}
        # Clamp count to 10 per docs
        count = max(1, min(int(count), 10))
        # Attempt to send user_id as number if possible to match API expectation
        try:
            uid: Any = int(user_id)
        except Exception:
            uid = user_id
        params = {"data": json.dumps({"user_id": uid, "offset": int(offset), "count": count}, separators=(",", ":"))}
        logger.info("[ZaloOAService] get_conversation: url=%s user_id=%s offset=%s count=%s", url, user_id, offset, count)
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers=headers, params=params)
            logger.info("[ZaloOAService] get_conversation response: status=%s", resp.status_code)
            if resp.status_code != 200:
                logger.error("[ZaloOAService] get_conversation error: status=%s body=%s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
            result = resp.json()
            logger.info("[ZaloOAService] get_conversation result keys=%s error=%s", list(result.keys()) if isinstance(result, dict) else None, result.get("error") if isinstance(result, dict) else None)
            return result

    @staticmethod
    async def send_text_message(access_token: str, to_user_id: str, text: str) -> Dict[str, Any]:
        """
        Send CS text message according to Zalo OA OpenAPI v3.0.
        Endpoint: POST /v3.0/oa/message/cs
        Header: access_token
        Body: {"recipient": {"user_id": "..."}, "message": {"text": "..."}}
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v3.0/oa/message/cs"
        headers = {
            "Content-Type": "application/json",
            "access_token": access_token,
        }
        payload: Dict[str, Any] = {
            "recipient": {"user_id": to_user_id},
            "message": {"text": text},
        }
        logger.info("[ZaloOAService] send_text_message: url=%s to_user_id=%s text_length=%d", url, to_user_id, len(text) if text else 0)
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            logger.info("[ZaloOAService] send_text_message response: status=%s", resp.status_code)
            if resp.status_code != 200:
                logger.error("[ZaloOAService] send_text_message error: status=%s body=%s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
            # API responds with JSON (sometimes labeled text/json)
            try:
                result = resp.json()
                logger.info("[ZaloOAService] send_text_message result: %s", result)
                return result
            except Exception as e:
                logger.warning("[ZaloOAService] send_text_message JSON parse error: %s, raw=%s", e, resp.text[:200])
                return {"raw": resp.text}

    @staticmethod
    async def upload_image(access_token: str, file_bytes: bytes, filename: str, content_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Upload image to Zalo OA to obtain attachment_id.
        Endpoint: POST /v2.0/oa/upload/image (multipart/form-data with field name 'file')
        Header: access_token
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v2.0/oa/upload/image"
        headers = {"access_token": access_token}
        if not content_type:
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"file": (filename or "image", file_bytes, content_type)}
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=headers, files=files)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}

    @staticmethod
    async def send_image_message(access_token: str, to_user_id: str, attachment_id: str) -> Dict[str, Any]:
        """
        Send image message using media template with a previously uploaded attachment_id.
        Endpoint: POST /v3.0/oa/message/cs
        Header: access_token, Content-Type: application/json
        """
        url = f"{settings.ZALO_OA_OPENAPI_BASE_URL.rstrip('/')}/v3.0/oa/message/cs"
        headers = {
            "Content-Type": "application/json",
            "access_token": access_token,
        }
        payload: Dict[str, Any] = {
            "recipient": {"user_id": to_user_id},
            "message": {
                "attachment": {
                    "type": "template",
                    "payload": {
                        "template_type": "media",
                        "elements": [
                            {
                                "media_type": "image",
                                "attachment_id": attachment_id,
                            }
                        ],
                    },
                }
            },
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}
