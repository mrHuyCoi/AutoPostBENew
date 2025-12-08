import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base


class Subscription(Base):
    """
    Model cho bảng subscriptions trong database.
    Lưu trữ các định nghĩa về gói cước (plans).
    """
    __tablename__ = "subscriptions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, unique=True, nullable=False)  # Tên gói: 'Cơ bản', 'Tiết kiệm', 'Chuyên nghiệp'
    description = Column(Text, nullable=True)
    price = Column(Integer, nullable=False)  # Giá gói, đơn vị: VND
    duration_days = Column(Integer, nullable=False)  # Thời hạn gói (tính bằng ngày)

    # Các quyền lợi của gói
    max_videos_per_day = Column(Integer, nullable=False, default=3)
    max_scheduled_days = Column(Integer, nullable=False, default=7)
    max_stored_videos = Column(Integer, nullable=False, default=30)
    storage_limit_gb = Column(Integer, nullable=False, default=5)
    max_social_accounts = Column(Integer, nullable=False, default=5)
    ai_content_generation = Column(Boolean, default=True) # Mặc định cho phép AI

    is_active = Column(Boolean, default=True)  # Gói này có đang được cung cấp không
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship: Một gói có thể được nhiều người dùng đăng ký
    user_subscriptions = relationship("UserSubscription", back_populates="subscription_plan")
