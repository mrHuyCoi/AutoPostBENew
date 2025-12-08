from fastapi import Depends, HTTPException, Security, Request
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status
import jwt

from app.database.database import get_db
from app.repositories.user_api_key_repository import UserApiKeyRepository
from app.repositories.user_repository import UserRepository
from app.models.user import User
from app.configs.settings import settings
from app.middlewares.auth_middleware import get_current_user_optional
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository
from app.utils.time import get_vn_now

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def validate_api_key(
    api_key: str = Security(api_key_header), 
    db: AsyncSession = Depends(get_db)
) -> tuple[User, list[str]]:
    """
    Xác thực API key, trả về user và scopes nếu hợp lệ.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key is missing"
        )

    api_key_obj = await UserApiKeyRepository.get_by_api_key(db, api_key)
    
    if not api_key_obj:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or expired API Key"
        )

    # Kiểm tra API Key có active không
    if hasattr(api_key_obj, "is_active") and not api_key_obj.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API Key is inactive"
        )

    user = await UserRepository.get_by_id(db, api_key_obj.user_id)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive or not found"
        )
    
    # Kiểm tra subscription chatbot còn hiệu lực (nếu hệ thống yêu cầu đăng ký gói)
    try:
        sub = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, user.id)
        now = get_vn_now()
        if (
            not sub
            or (hasattr(sub, "is_active") and not sub.is_active)
            or (hasattr(sub, "status") and sub.status != "approved")
            or (hasattr(sub, "end_date") and sub.end_date is not None and sub.end_date < now)
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Chatbot subscription is inactive or expired"
            )
    except HTTPException:
        # Re-raise các lỗi kiểm tra subscription
        raise
    except Exception:
        # An toàn: nếu truy vấn subscription lỗi, chặn truy cập để tránh bypass
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not verify chatbot subscription"
        )
    
    return user, api_key_obj.scopes

async def get_user_for_chatbot(
    request: Request,
    db: AsyncSession = Depends(get_db),
    user_from_token: User = Depends(get_current_user_optional)
) -> tuple[User, list[str]]:
    """
    Dependency lai: Xác thực người dùng qua JWT (website) hoặc API Key (API/script).
    Trả về tuple (user, scopes) để controller có thể sử dụng.
    """
    # 1. Thử xác thực bằng JWT Token (cho người dùng web)
    if user_from_token:
        # Website users có tất cả quyền
        return user_from_token, ["*"]  # "*" nghĩa là có tất cả quyền

    # 2. Thử xác thực bằng API Key (cho API/script)
    api_key = await api_key_header(request)
    if api_key:
        api_key_obj = await UserApiKeyRepository.get_by_api_key(db, api_key)
        if api_key_obj:
            user = await UserRepository.get_by_id(db, api_key_obj.user_id)
            if user and user.is_active:
                return user, api_key_obj.scopes  # Trả về scopes từ database

    # 3. Nếu cả hai cách đều thất bại
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )


# Function này không còn được sử dụng sau khi bỏ require_scope trong document_controller
# def require_scope(required_scope: str):
#     """
#     Dependency factory để kiểm tra xem user có scope cần thiết không.
#     """
#     async def scope_checker(
#         auth_details: tuple[User, list[str]] = Depends(validate_api_key)
#     ) -> User:
#         user, scopes = auth_details
#         if required_scope not in scopes:
#             raise HTTPException(
#                 status_code=status.HTTP_403_FORBIDDEN,
#                 detail=f"Không có quyền truy cập dịch vụ '{required_scope}'. Vui lòng nâng cấp gói cước."
#             )
#         return user
#     return scope_checker 