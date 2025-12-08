import uuid
from datetime import datetime
from sqlalchemy import Column, DateTime, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base


class UserSubscription(Base):
    """
    Model cho bảng user_subscriptions trong database.
    Lưu trữ thông tin gói đăng ký mà một người dùng cụ thể đang sử dụng.
    """
    __tablename__ = "user_subscriptions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True) # Mỗi user chỉ có 1 subscription active
    subscription_id = Column(UUID(as_uuid=True), ForeignKey("subscriptions.id"), nullable=False)
    
    start_date = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    end_date = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)
    
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="subscription")
    subscription_plan = relationship("Subscription", back_populates="user_subscriptions")
