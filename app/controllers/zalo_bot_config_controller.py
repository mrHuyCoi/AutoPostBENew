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

@router.get("/bot-configs")
@with_log_context(
    event_action="zalo_bot_config.list",
    event_category="zalo_bot_config",
    source_layer="controller",
    source_controller="zalo_bot_config_controller",
)
async def list_bot_configs(
    limit: int = 50,
    offset: int = 0,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """
    Liệt kê danh sách bot configs từ zaloapi
    """
    try:
        user, scopes = auth_details
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs"
            params = {
                "limit": min(max(int(limit), 1), 200),
                "offset": max(int(offset), 0),
            }

            resp = await client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy danh sách bot configs"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy bot configs: {str(e)}"
        )

@router.get("/bot-configs/me")
@with_log_context(
    event_action="zalo_bot_config.me.get",
    event_category="zalo_bot_config",
    source_layer="controller",
    source_controller="zalo_bot_config_controller",
)
async def get_my_bot_config(
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
    account_id: Optional[str] = None,
):
    """
    Lấy bot config của user hiện tại (session_key = user.id) từ zaloapi
    """
    try:
        user, scopes = auth_details
        session_key = str(user.id)

        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            params = { "account_id": account_id } if account_id else None
            resp = await client.get(url, params=params)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy bot config")
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy bot config"
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
            detail=f"Lỗi khi lấy bot config: {str(e)}"
        )

@router.put("/bot-configs/me")
@with_log_context(
    event_action="zalo_bot_config.me.upsert",
    event_category="zalo_bot_config",
    source_layer="controller",
    source_controller="zalo_bot_config_controller",
)
async def upsert_my_bot_config(
    payload: dict,
    auth_details: tuple[User, list[str]] = Depends(validate_api_key),
    db: AsyncSession = Depends(get_db),
):
    """
    Tạo hoặc cập nhật bot config cho user hiện tại (session_key = user.id)
    Body: { stop_minutes: int }
    """
    try:
        user, scopes = auth_details
        session_key = str(user.id)

        stop_minutes = payload.get("stop_minutes")
        if stop_minutes is None:
            raise HTTPException(status_code=400, detail="stop_minutes là bắt buộc")

        if not isinstance(stop_minutes, (int, float)) or stop_minutes < 0:
            raise HTTPException(status_code=400, detail="stop_minutes phải là số không âm")

        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            req_body = {
                "stop_minutes": int(stop_minutes)
            }
            account_id = payload.get("account_id")
            if account_id:
                req_body["account_id"] = str(account_id)

            resp = await client.put(url, json=req_body)
            if resp.status_code in (200, 201):
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể tạo/cập nhật bot config"
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
            detail=f"Lỗi khi upsert bot config: {str(e)}"
        )
