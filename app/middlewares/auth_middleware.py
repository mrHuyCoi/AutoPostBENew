from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from typing import Optional, List
import uuid
from zoneinfo import ZoneInfo

from app.database.database import get_db
from app.models.user import User
from app.configs.settings import settings
from app.repositories.user_repository import UserRepository
from fastapi.security import HTTPBearer

security = HTTPBearer()
security_optional = HTTPBearer(auto_error=False)  # Không tự động ném lỗi

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)

async def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Tạo JWT access token."""
    to_encode = data.copy()
    
    if expires_delta:
        expire = now_vn_naive() + expires_delta
    else:
        expire = now_vn_naive() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    
    return encoded_jwt

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security), 
                           db: AsyncSession = Depends(get_db)) -> User:
    """Xác thực và lấy thông tin người dùng hiện tại từ token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Không thể xác thực thông tin đăng nhập",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not credentials or not credentials.credentials:
        raise HTTPException(
            status_code=401, 
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"}
        )
        
    token = credentials.credentials
    
    try:
        # Giải mã token
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("sub")
        
        if user_id is None:
            raise credentials_exception
        
    except JWTError:
        raise credentials_exception
    
    # Lấy thông tin người dùng từ database
    user = await UserRepository.get_by_id(db, uuid.UUID(user_id))
    
    if user is None:
        raise credentials_exception
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tài khoản đã bị vô hiệu hóa",
        )
    
    # Thêm thuộc tính is_admin dựa trên role
    user.is_admin = user.role == 'admin'
    
    return user


async def get_current_active_superuser(current_user: User = Depends(get_current_user)) -> User:
    """Kiểm tra xem người dùng hiện tại có phải là admin hay không."""
    if current_user.role != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Bạn không có quyền truy cập chức năng này",
        )
    
    return current_user


async def get_current_user_optional(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security_optional), 
                                  db: AsyncSession = Depends(get_db)) -> Optional[User]:
    """Xác thực và lấy thông tin người dùng hiện tại từ token, nhưng không bắt buộc."""
    if not credentials or not credentials.credentials:
        return None
        
    token = credentials.credentials
    
    try:
        # Giải mã token
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id: str = payload.get("sub")
        
        if user_id is None:
            return None
        
    except JWTError:
        return None
    
    # Lấy thông tin người dùng từ database
    user = await UserRepository.get_by_id(db, uuid.UUID(user_id))
    
    if user is None or not user.is_active:
        return None
    
    # Thêm thuộc tính is_admin dựa trên role
    user.is_admin = user.role == 'admin'
    
    return user



