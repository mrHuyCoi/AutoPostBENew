from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import timedelta

from app.database.database import get_db
from app.services.user_service import UserService
from app.middlewares.auth_middleware import create_access_token, get_current_user
from app.configs.settings import settings
from fastapi import Form
from app.logging.decorators import with_log_context
router = APIRouter()


@router.post("/login")
@with_log_context(
    event_action="auth.login",
    event_category="auth",
    source_layer="controller",
    source_controller="auth_controller",
)
async def login(
    username:str= Form(...),
    password:str= Form(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Đăng nhập và tạo access token.
    
    - **username**: Email người dùng
    - **password**: Mật khẩu
    """
    # Xác thực người dùng với email/password
    user = await UserService.authenticate_user(
        db=db,
        email=username,
        password=password
    )
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email hoặc mật khẩu không chính xác",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Tạo JWT token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = await create_access_token(
        data={"sub": str(user.id)},
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": str(user.id),
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role
    }
