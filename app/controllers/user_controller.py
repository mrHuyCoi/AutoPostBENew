from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from typing import Optional

from app.database.database import get_db
from app.dto.user_dto import UserCreate, UserRead, UserUpdate
from app.services.user_service import UserService
from app.middlewares.auth_middleware import get_current_user, get_current_active_superuser
from app.middlewares.subscription_middleware import check_active_subscription
from app.models.user import User
from app.logging.decorators import with_log_context

router = APIRouter()

class SystemPromptUpdate(BaseModel):
    custom_system_prompt: Optional[str] = None

class APIKeyUpdate(BaseModel):
    gemini_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None

class APIKeyRead(BaseModel):
    gemini_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None

# @router.post("/register", response_model=UserRead, status_code=status.HTTP_201_CREATED)
# async def register_user(
#     data: UserCreate,
#     db: AsyncSession = Depends(get_db)
# ):
#     return await UserService.register_user(db=db, data=data)

@router.get("/me", response_model=UserRead)
@with_log_context(
    event_action="user.me.get",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def get_current_user_info(
    current_user: User = Depends(get_current_user)
):
    return current_user

@router.put("/me", response_model=UserRead)
@with_log_context(
    event_action="user.me.update",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def update_current_user(
    data: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    if data.role is not None and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền thay đổi vai trò của người dùng"
        )
    return await UserService.update_user(db=db, user_id=current_user.id, data=data)

@router.get("/me/system-prompt", response_model=SystemPromptUpdate)
@with_log_context(
    event_action="user.me.system_prompt.get",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def get_system_prompt(current_user: User = Depends(get_current_user)):
    return {"custom_system_prompt": current_user.custom_system_prompt}

@router.put("/me/system-prompt", response_model=SystemPromptUpdate)
@with_log_context(
    event_action="user.me.system_prompt.update",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def update_system_prompt(
    data: SystemPromptUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    user = await UserService.update_system_prompt(db, current_user.id, data.custom_system_prompt)
    return {"custom_system_prompt": user.custom_system_prompt}

@router.put("/me/api-key", response_model=APIKeyRead)
@with_log_context(
    event_action="user.me.api_key.update",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def update_api_key(
    data: APIKeyUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    updated_keys = await UserService.update_api_keys(db, current_user.id, data.gemini_api_key, data.openai_api_key)
    return updated_keys

@router.get("/me/api-key", response_model=APIKeyRead)
@with_log_context(
    event_action="user.me.api_key.get",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def get_api_key(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return await UserService.get_api_keys(db, current_user.id)

@router.delete("/me/api-key", response_model=APIKeyRead)
@with_log_context(
    event_action="user.me.api_key.delete",
    event_category="user",
    source_layer="controller",
    source_controller="user_controller",
)
async def delete_api_key(
    data: APIKeyUpdate = Body(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    return await UserService.delete_api_keys(db, current_user.id, data.gemini_api_key, data.openai_api_key)
