import uuid
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from .chatbot_plan_dto import ChatbotPlanRead

class UserChatbotSubscriptionCreate(BaseModel):
    plan_id: uuid.UUID
    months_subscribed: int = Field(..., gt=0, description="Số tháng đăng ký, phải lớn hơn 0")

class UserBasicInfo(BaseModel):
    """Thông tin cơ bản của người dùng"""
    id: uuid.UUID
    email: str
    full_name: Optional[str] = None
    is_active: bool

    class Config:
        from_attributes = True

class UserChatbotSubscriptionRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    user: Optional[UserBasicInfo] = None
    plan: ChatbotPlanRead
    start_date: datetime
    end_date: datetime
    months_subscribed: int
    total_price: int
    is_active: bool
    status: str  # 'pending', 'approved', 'rejected'
    max_api_calls: Optional[int] = None
    api_calls_used: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class UserChatbotSubscriptionApproval(BaseModel):
    """DTO cho việc admin phê duyệt subscription"""
    status: str = Field(..., description="Trạng thái: 'approved' hoặc 'rejected'")
    notes: Optional[str] = Field(None, description="Ghi chú của admin")

class UserChatbotSubscriptionUpdate(BaseModel):
    """DTO cho Admin cập nhật subscription của người dùng"""
    plan_id: Optional[uuid.UUID] = Field(None, description="ID gói cước mới (tuỳ chọn)")
    months_subscribed: Optional[int] = Field(None, gt=0, description="Số tháng đăng ký mới (tuỳ chọn)")
    is_active: Optional[bool] = Field(None, description="Kích hoạt/Vô hiệu (tuỳ chọn)")
    status: Optional[str] = Field(None, description="Trạng thái: 'pending' | 'approved' | 'rejected' (tuỳ chọn)")
 