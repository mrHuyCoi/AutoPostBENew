import uuid
from datetime import datetime
from sqlalchemy import Column, DateTime, ForeignKey, Boolean, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.database.database import Base
from app.utils.time import get_vn_now

class UserChatbotSubscription(Base):
    __tablename__ = 'user_chatbot_subscriptions'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete="CASCADE"), nullable=False)
    plan_id = Column(UUID(as_uuid=True), ForeignKey('chatbot_plans.id'), nullable=False)
    
    start_date = Column(DateTime(timezone=True), nullable=False, default=get_vn_now)
    end_date = Column(DateTime(timezone=True), nullable=False)
    months_subscribed = Column(Integer, nullable=False, default=1)
    total_price = Column(Integer, nullable=False) # Giá cuối cùng sau khi đã áp dụng tất cả giảm giá
    is_active = Column(Boolean, default=False)  # Chỉ active khi admin phê duyệt
    status = Column(String, default='pending', nullable=False)  # 'pending', 'approved', 'rejected'
    max_api_calls = Column(Integer, nullable=True)
    api_calls_used = Column(Integer, nullable=False, default=0)
    
    created_at = Column(DateTime(timezone=True), default=get_vn_now)
    updated_at = Column(DateTime(timezone=True), default=get_vn_now, onupdate=get_vn_now)

    # Relationships
    user = relationship("User", back_populates="chatbot_subscriptions")
    plan = relationship("ChatbotPlan", backref="user_subscriptions")

    def __repr__(self):
        return f"<UserChatbotSubscription(user_id='{self.user_id}', plan_id='{self.plan_id}')>" 