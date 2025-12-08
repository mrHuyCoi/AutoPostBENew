import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base


class Color(Base):
    """
    Model cho bảng màu sắc trong database.
    Lưu trữ thông tin về các màu sắc có thể có của thiết bị.
    """
    __tablename__ = "colors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)  # Tên màu (không unique nữa vì mỗi user có thể có màu trùng tên)
    hex_code = Column(String, nullable=True)  # Mã màu HEX
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)  # ID của người dùng tạo màu, null cho màu mặc định
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user_devices = relationship("UserDevice", back_populates="color", cascade="all, delete-orphan")
    device_colors = relationship("DeviceColor", back_populates="color", cascade="all, delete-orphan")