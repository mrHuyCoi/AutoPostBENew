from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class SubscriptionCreate(BaseModel):
    """
    DTO cho request tạo gói đăng ký người dùng mới.
    """
    user_id: uuid.UUID = Field(..., description="ID của người dùng")
    subscription_id: uuid.UUID = Field(..., description="ID của gói đăng ký (từ bảng subscriptions)")
    start_date: datetime = Field(default_factory=datetime.utcnow, description="Ngày bắt đầu gói")
    end_date: Optional[datetime] = Field(None, description="Ngày kết thúc gói")
    is_active: bool = Field(True, description="Trạng thái kích hoạt")


class SubscriptionCreateSimple(BaseModel):
    """
    DTO đơn giản cho request tạo gói đăng ký người dùng mới, chỉ yêu cầu subscription_id.
    """
    subscription_id: uuid.UUID = Field(..., description="ID của gói đăng ký (từ bảng subscriptions)")


class UserBasicInfo(BaseModel):
    """
    DTO cho thông tin cơ bản của người dùng.
    """
    id: uuid.UUID
    email: str
    full_name: Optional[str] = None
    is_active: bool
    
    class Config:
        from_attributes = True

class SubscriptionRead(BaseModel):
    """
    DTO cho response trả về thông tin gói đăng ký người dùng.
    """
    id: uuid.UUID
    user_id: uuid.UUID
    subscription_id: uuid.UUID
    start_date: datetime
    end_date: Optional[datetime] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime
    user: Optional[UserBasicInfo] = None
    subscription_plan: Optional["SubscriptionPlanRead"] = None
    
    class Config:
        from_attributes = True


class SubscriptionPlanCreate(BaseModel):
    """
    DTO cho request tạo gói cước mới.
    """
    name: str = Field(..., description="Tên gói cước")
    description: Optional[str] = Field(None, description="Mô tả gói cước")
    price: int = Field(..., description="Giá gói cước (VND)")
    duration_days: int = Field(..., description="Thời hạn gói (ngày)")
    max_videos_per_day: int = Field(3, description="Số video tối đa mỗi ngày")
    max_scheduled_days: int = Field(7, description="Số ngày tối đa có thể lên lịch trước")
    max_stored_videos: int = Field(30, description="Số video tối đa có thể lưu trữ")
    storage_limit_gb: int = Field(5, description="Giới hạn dung lượng lưu trữ (GB)")
    max_social_accounts: int = Field(5, description="Số tài khoản mạng xã hội tối đa")
    ai_content_generation: bool = Field(True, description="Cho phép tạo nội dung bằng AI")
    is_active: bool = Field(True, description="Trạng thái kích hoạt")


class SubscriptionPlanUpdate(BaseModel):
    """
    DTO cho request cập nhật gói cước.
    """
    name: Optional[str] = Field(None, description="Tên gói cước")
    description: Optional[str] = Field(None, description="Mô tả gói cước")
    price: Optional[int] = Field(None, description="Giá gói cước (VND)")
    duration_days: Optional[int] = Field(None, description="Thời hạn gói (ngày)")
    max_videos_per_day: Optional[int] = Field(None, description="Số video tối đa mỗi ngày")
    max_scheduled_days: Optional[int] = Field(None, description="Số ngày tối đa có thể lên lịch trước")
    max_stored_videos: Optional[int] = Field(None, description="Số video tối đa có thể lưu trữ")
    storage_limit_gb: Optional[int] = Field(None, description="Giới hạn dung lượng lưu trữ (GB)")
    max_social_accounts: Optional[int] = Field(None, description="Số tài khoản mạng xã hội tối đa")
    ai_content_generation: Optional[bool] = Field(None, description="Cho phép tạo nội dung bằng AI")
    is_active: Optional[bool] = Field(None, description="Trạng thái kích hoạt")


class SubscriptionPlanRead(BaseModel):
    """
    DTO cho response trả về thông tin gói cước (plan).
    """
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    price: int
    duration_days: int
    max_videos_per_day: int
    max_scheduled_days: int
    max_stored_videos: int
    storage_limit_gb: int
    max_social_accounts: int
    ai_content_generation: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class SubscriptionUpdate(BaseModel):
    """
    DTO cho request cập nhật thông tin gói đăng ký người dùng.
    """
    subscription_id: Optional[uuid.UUID] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: Optional[bool] = None
    
    class Config:
        from_attributes = True