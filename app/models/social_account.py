import uuid
from datetime import datetime
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from zoneinfo import ZoneInfo

from app.database.database import Base

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class SocialAccount(Base):
    """
    Model cho bảng social_accounts trong database.
    Lưu trữ thông tin tài khoản mạng xã hội của người dùng.
    """
    __tablename__ = "social_accounts"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    platform = Column(String, nullable=False)  # 'facebook', 'instagram', 'threads', 'youtube'
    account_name = Column(String, nullable=False)
    account_id = Column(String, nullable=True)  # ID/username của tài khoản
    access_token = Column(String, nullable=True)  # Token để truy cập API (được mã hóa)
    refresh_token = Column(String, nullable=True)
    token_expires_at = Column(DateTime, nullable=True)
    thumbnail = Column(String, nullable=True)  # URL của ảnh đại diện
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
    
    # Relationships
    user = relationship("User", back_populates="social_accounts")
    platform_posts = relationship("PlatformPost", back_populates="social_account")
