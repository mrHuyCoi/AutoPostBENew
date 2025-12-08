import uuid
from datetime import datetime
from sqlalchemy import Column, Integer, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base


class DeviceStorage(Base):
    """
    Model cho bảng dung lượng theo dòng máy trong database.
    Lưu trữ thông tin về các dung lượng có thể có của từng dòng thiết bị cụ thể.
    """
    __tablename__ = "device_storage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    device_info_id = Column(UUID(as_uuid=True), ForeignKey("device_info.id"), nullable=False)
    capacity = Column(Integer, nullable=False)  # Số dung lượng (GB)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)  # ID của người dùng tạo dung lượng, null cho dung lượng mặc định
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    device_info = relationship("DeviceInfo", back_populates="device_storages")
    user_devices = relationship("UserDevice", back_populates="device_storage", cascade="all, delete-orphan")