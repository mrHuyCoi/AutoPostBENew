from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
from typing import Optional, Dict, Any

from app.database.database import get_db
from app.middlewares.api_key_middleware import validate_api_key
from app.models.user import User
from app.configs.settings import settings
from app.logging.decorators import with_log_context

router = APIRouter()

ZALO_API_BASE_URL = settings.ZALO_API_BASE_URL

@router.get("/staffzalo")
@with_log_context(
    event_action="zalo_staff.list",
    event_category="zalo_staff",
    source_layer="controller",
    source_controller="staffzalo_controller",
)
async def list_staff(
    includeInactive: bool = False,
    limit: int = 50,
    offset: int = 0,
    account_id: str | None = None,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """Danh sách staff, chỉ trả về các staff thuộc session của user (theo token).
    Quy ước: staff có associated_session_keys chứa session_key (user_id) hoặc rỗng (global) sẽ được hiển thị.
    """
    user, scopes = auth_details
    session_key = str(user.id)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            params = {
                "includeInactive": str(includeInactive).lower(),
                "limit": max(1, min(200, limit)),
                "offset": max(0, offset),
                "session_key": session_key,
            }
            if account_id:
                params["account_id"] = account_id
            url = f"{ZALO_API_BASE_URL}/api/staff"
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            data = resp.json()
            items = data.get("data") or data.get("items") or []
            # Filter by associated_session_keys
            filtered = []
            for it in items:
                keys = it.get("associated_session_keys")
                if not keys:
                    filtered.append(it)  # global staff (visible to all)
                else:
                    try:
                        if session_key in keys:
                            filtered.append(it)
                    except Exception:
                        pass
            # Normalize output structure
            return {"ok": True, "items": filtered, "count": len(filtered)}
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Không thể kết nối zaloapi: {e}")

@router.get("/staffzalo/{id}")
@with_log_context(
    event_action="zalo_staff.get",
    event_category="zalo_staff",
    source_layer="controller",
    source_controller="staffzalo_controller",
)
async def get_staff(
    id: str,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/staff/{id}"
            resp = await client.get(url)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Không thể kết nối zaloapi: {e}")

@router.post("/staffzalo")
@with_log_context(
    event_action="zalo_staff.create",
    event_category="zalo_staff",
    source_layer="controller",
    source_controller="staffzalo_controller",
)
async def create_staff(
    payload: Dict[str, Any],
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """Tạo staff và tự động gắn session_key (user_id) lấy từ token frontend."""
    user, scopes = auth_details
    session_key = str(user.id)
    try:
        # Default role if not provided
        if "role" not in payload:
            payload["role"] = "staff"
        # Ensure associated_session_keys includes session_key
        keys = payload.get("associated_session_keys")
        if not keys:
            payload["associated_session_keys"] = [session_key]
        else:
            if session_key not in keys:
                payload["associated_session_keys"] = list({*keys, session_key})
        # Pass owner_account_id if provided by client
        if payload.get("owner_account_id") is not None:
            payload["owner_account_id"] = str(payload["owner_account_id"]) or None
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/staff"
            resp = await client.post(url, json=payload)
            if resp.status_code not in (200, 201):
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Không thể kết nối zaloapi: {e}")

@router.patch("/staffzalo/{id}")
@with_log_context(
    event_action="zalo_staff.update",
    event_category="zalo_staff",
    source_layer="controller",
    source_controller="staffzalo_controller",
)
async def update_staff(
    id: str,
    payload: Dict[str, Any],
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """Cập nhật staff; nếu client gửi associated_session_keys, đảm bảo có session_key hiện tại."""
    user, scopes = auth_details
    session_key = str(user.id)
    try:
        if "associated_session_keys" in payload and isinstance(payload["associated_session_keys"], list):
            keys = payload["associated_session_keys"]
            if session_key not in keys:
                payload["associated_session_keys"] = list({*keys, session_key})
        # Forward owner_account_id if explicitly provided
        if payload.get("owner_account_id") is not None:
            payload["owner_account_id"] = str(payload["owner_account_id"]) or None
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/staff/{id}"
            resp = await client.patch(url, json=payload)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Không thể kết nối zaloapi: {e}")

@router.delete("/staffzalo/{id}")
@with_log_context(
    event_action="zalo_staff.delete",
    event_category="zalo_staff",
    source_layer="controller",
    source_controller="staffzalo_controller",
)
async def delete_staff(
    id: str,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/staff/{id}"
            resp = await client.delete(url)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Không thể kết nối zaloapi: {e}")
