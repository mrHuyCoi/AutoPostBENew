import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, func, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base
from .associations import device_material_association


class DeviceInfo(Base):
    """
    Model cho bảng thông tin máy trong database.
    Lưu trữ thông tin chi tiết về các loại máy.
    """
    __tablename__ = "device_info"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    model = Column(String, nullable=False, index=True)
    brand = Column(String, nullable=True, index=True) # Thêm trường thương hiệu
    release_date = Column(String, nullable=True)  # Ra mắt
    screen = Column(String, nullable=True)  # Màn hình
    chip_ram = Column(String, nullable=True)  # Chip / RAM
    camera = Column(String, nullable=True)  # Camera sau → trước
    battery = Column(String, nullable=True)  # Pin (mAh)
    connectivity_os = Column(String, nullable=True)  # Kết nối / HĐH
    color_english = Column(String, nullable=True)  # Màu sắc tiếng anh
    dimensions_weight = Column(String, nullable=True)  # Kích thước / Trọng lượng
    sensors_health_features = Column(String, nullable=True) # Cảm biến & Tính năng sức khỏe
    warranty = Column(String, nullable=True)  # Bảo hành
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)  # ID của người dùng tạo thiết bị, null cho thiết bị mặc định
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user_devices = relationship("UserDevice", back_populates="device_info", cascade="all, delete-orphan")
    device_colors = relationship("DeviceColor", back_populates="device_info", cascade="all, delete-orphan", lazy="noload")
    device_storages = relationship("DeviceStorage", back_populates="device_info", cascade="all, delete-orphan", lazy="noload")
    materials = relationship("Material", secondary=device_material_association, back_populates="device_infos", lazy="noload")