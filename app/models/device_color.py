import uuid
from datetime import datetime
from sqlalchemy import Column, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from pydantic import BaseModel

from app.database.database import Base


class DeviceColor(Base):
    """
    Model cho bảng liên kết giữa thông tin máy và màu sắc.
    Lưu trữ thông tin về các màu sắc có thể có của từng model thiết bị.
    """
    __tablename__ = "device_colors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_info_id = Column(UUID(as_uuid=True), ForeignKey("device_info.id"), nullable=False)
    color_id = Column(UUID(as_uuid=True), ForeignKey("colors.id"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)  # ID của người dùng tạo liên kết, null cho liên kết mặc định
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    device_info = relationship("DeviceInfo", back_populates="device_colors")
    color = relationship("Color", back_populates="device_colors")