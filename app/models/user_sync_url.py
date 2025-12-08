import uuid
from sqlalchemy import Column, String, Boolean, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime

from app.database.database import Base


class UserSyncUrl(Base):
    __tablename__ = 'user_sync_urls'
    __table_args__ = (
        UniqueConstraint('user_id', 'type_url', name='uq_user_sync_urls_user_type'),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete="CASCADE"), nullable=False, index=True)
    url = Column(String, nullable=False)
    # Kiểu dữ liệu cần đồng bộ từ URL: 'device' | 'component' | 'service'
    type_url = Column(String, nullable=True)
    # URL chuyên biệt để đồng bộ dữ liệu theo ngày (ví dụ: chỉ dữ liệu cập nhật hôm nay)
    url_today = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("User", back_populates="sync_urls")
    devices = relationship("UserDeviceFromUrl", back_populates="sync_url", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<UserSyncUrl(user_id='{self.user_id}', url='{self.url}')>"
