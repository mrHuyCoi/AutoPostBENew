from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional
from datetime import datetime
import uuid

class UserCreate(BaseModel):
    """DTO cho request tạo người dùng mới."""
    email: EmailStr = Field(..., description="Email của người dùng, phải là định dạng email hợp lệ")
    password: str = Field(..., min_length=8, description="Mật khẩu của người dùng, tối thiểu 8 ký tự")
    full_name: Optional[str] = Field(None, description="Họ tên đầy đủ của người dùng")
    subscription_id: Optional[uuid.UUID] = Field(
        None,
        description="ID của gói đăng ký người dùng chọn (có thể null)"
    )
    
    @validator('password')
    def password_strength(cls, v):
        """Validate độ mạnh của mật khẩu."""
        if len(v) < 8:
            raise ValueError('Mật khẩu phải có ít nhất 8 ký tự')
        return v


class UserRead(BaseModel):
    """DTO cho response trả về thông tin người dùng."""
    id: uuid.UUID
    email: str
    full_name: Optional[str] = None
    is_active: bool
    is_superuser: bool
    role: str
    created_at: datetime
    updated_at: datetime
    custom_system_prompt: Optional[str] = None
    
    class Config:
        from_attributes = True


class UserUpdate(BaseModel):
    """DTO cho request cập nhật thông tin người dùng."""
    full_name: Optional[str] = None
    is_active: Optional[bool] = None
    custom_system_prompt: Optional[str] = None
    role: Optional[str] = None
    
    class Config:
        from_attributes = True


class UserWithRole(UserRead):
    """DTO cho response trả về thông tin người dùng kèm vai trò."""
    role: Optional["RoleRead"] = None
    
    class Config:
        from_attributes = True
