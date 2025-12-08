import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base

class DeviceBrand(Base):
    """
    Model cho bảng device_brand, liên kết với bảng brands.
    Lưu trữ thông tin hãng điện thoại có thể được sử dụng lại.
    """
    __tablename__ = "device_brands"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False, index=True)  # Tên hãng điện thoại (vd: Apple, Samsung)
    warranty = Column(String, nullable=True)  # Thời gian bảo hành theo hãng
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="device_brands")
    brands = relationship("Brand", back_populates="device_brand")