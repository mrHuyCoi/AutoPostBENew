from typing import Optional
import uuid  # Added import
from datetime import datetime  # Added import
from pydantic import BaseModel, Field
from enum import Enum


class PlatformEnum(str, Enum):
    FACEBOOK = "facebook"
    INSTAGRAM = "instagram"

class FacebookTokenRequest(BaseModel):
    """
    DTO cho request thêm tài khoản mạng xã hội (Facebook Page hoặc Instagram Account)
    dựa trên User Access Token.
    """
    user_access_token: str = Field(..., description="User Access Token của người dùng (có thể dùng cho Facebook hoặc Instagram)")
    platform: PlatformEnum = Field(..., description="Nền tảng sử dụng token (facebook hoặc instagram)")
    

class FacebookResponse(BaseModel):
    """
    DTO cho response trả về thông tin tài khoản mạng xã hội.
    """
    id: uuid.UUID  # Changed from str to uuid.UUID
    platform: str
    account_name: str
    account_id: str
    is_active: bool
    created_at: datetime  # Changed from str to datetime.datetime
    thumbnail: Optional[str] = None  # URL của profile picture/thumbnail
    
    class Config:
        from_attributes = True
