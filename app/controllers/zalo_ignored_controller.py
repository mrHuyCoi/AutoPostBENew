from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
from typing import Optional

from app.database.database import get_db
from app.middlewares.api_key_middleware import validate_api_key
from app.models.user import User
from app.configs.settings import settings
from app.logging.decorators import with_log_context

router = APIRouter()

ZALO_API_BASE_URL = settings.ZALO_API_BASE_URL

@router.get("/ignored-conversations")
@with_log_context(
    event_action="zalo_ignored.conversations.list",
    event_category="zalo_ignored",
    source_layer="controller",
    source_controller="zalo_ignored_controller",
)
async def list_ignored_conversations(
    thread_id: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    account_id: Optional[str] = None,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """
    Liệt kê danh sách cuộc hội thoại bị bỏ qua của user hiện tại (theo session_key = user.id).
    Có thể filter theo thread_id, user_id.
    """
    try:
        user, scopes = auth_details
        session_key = str(user.id)
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/ignored-conversations"
            params = {
                "session_key": session_key,
                "limit": min(max(int(limit), 1), 200),
                "offset": max(int(offset), 0),
            }
            if thread_id:
                params["thread_id"] = thread_id
            if user_id:
                params["user_id"] = user_id
            if account_id:
                params["account_id"] = account_id

            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy danh sách ignored conversations"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy ignored conversations: {str(e)}"
        )

@router.post("/ignored-conversations")
@with_log_context(
    event_action="zalo_ignored.conversations.upsert",
    event_category="zalo_ignored",
    source_layer="controller",
    source_controller="zalo_ignored_controller",
)
async def upsert_ignored_conversation(
    payload: dict,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """
    Thêm/sửa một record ignored theo (session_key, thread_id) cho user hiện tại.
    Body: { thread_id: str, name?: str, user_id?: str }
    """
    try:
        user, scopes = auth_details
        session_key = str(user.id)
        thread_id = payload.get("thread_id")
        name = payload.get("name")
        body_user_id = payload.get("user_id")
        account_id = payload.get("account_id")

        if not thread_id:
            raise HTTPException(status_code=400, detail="thread_id là bắt buộc")
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id là bắt buộc")

        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/ignored-conversations"
            req_body = {
                "session_key": session_key,
                "thread_id": thread_id,
                "account_id": str(account_id),
            }
            if name is not None:
                req_body["name"] = name
            if body_user_id is not None:
                req_body["user_id"] = body_user_id

            resp = await client.post(url, json=req_body)
            if resp.status_code in (200, 201):
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể tạo/cập nhật ignored conversation"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi upsert ignored conversation: {str(e)}"
        )

@router.get("/ignored-conversations/{id}")
@with_log_context(
    event_action="zalo_ignored.conversations.get",
    event_category="zalo_ignored",
    source_layer="controller",
    source_controller="zalo_ignored_controller",
)
async def get_ignored_conversation(
    id: str,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Không cần session_key cho GET theo id
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/ignored-conversations/{id}"
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy")
            else:
                raise HTTPException(status_code=503, detail="Không thể lấy ignored conversation")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Không thể kết nối zaloapi service")

@router.patch("/ignored-conversations/{id}")
@with_log_context(
    event_action="zalo_ignored.conversations.update",
    event_category="zalo_ignored",
    source_layer="controller",
    source_controller="zalo_ignored_controller",
)
async def update_ignored_conversation(
    id: str,
    payload: dict,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/ignored-conversations/{id}"
            resp = await client.patch(url, json=payload)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy")
            else:
                raise HTTPException(status_code=503, detail="Không thể cập nhật")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Không thể kết nối zaloapi service")

@router.delete("/ignored-conversations/{id}")
@with_log_context(
    event_action="zalo_ignored.conversations.delete",
    event_category="zalo_ignored",
    source_layer="controller",
    source_controller="zalo_ignored_controller",
)
async def delete_ignored_conversation(
    id: str,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/ignored-conversations/{id}"
            resp = await client.delete(url)
            if resp.status_code == 200:
                return {"ok": True}
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy")
            else:
                raise HTTPException(status_code=503, detail="Không thể xóa")
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Không thể kết nối zaloapi service")
