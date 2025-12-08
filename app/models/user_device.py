import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Integer, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base
from app.utils.time import get_vn_now


class UserDevice(Base):
    """
    Model cho bảng liên kết giữa thông tin máy, màu sắc, dung lượng và người dùng.
    Lưu trữ thông tin máy do người dùng nhập vào.
    """
    __tablename__ = "user_devices"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    device_info_id = Column(UUID(as_uuid=True), ForeignKey("device_info.id"), nullable=False)
    color_id = Column(UUID(as_uuid=True), ForeignKey("colors.id"), nullable=True)
    device_storage_id = Column(UUID(as_uuid=True), ForeignKey("device_storage.id"), nullable=True)
    product_code = Column(String, nullable=True)  # Mã sản phẩm tự động tăng (SP000001, SP000002, ...)
    warranty = Column(String, nullable=True)  # Bảo hành
    device_condition = Column(String, nullable=False)  # Tình trạng máy
    device_type = Column(String, nullable=False)  # Loại máy (mới hay cũ)
    battery_condition = Column(String, nullable=True)  # Tình trạng pin
    price = Column(Float, nullable=False)  # Giá
    wholesale_price = Column(Float, nullable=True)  # Giá bán buôn
    inventory = Column(Integer, default=0)  # Tồn kho
    notes = Column(Text, nullable=True)  # Ghi chú
    created_at = Column(DateTime(timezone=True), default=get_vn_now)
    updated_at = Column(DateTime(timezone=True), default=get_vn_now, onupdate=get_vn_now)
    trashed_at = Column(DateTime(timezone=True), nullable=True)  # Thời gian xóa mềm
    purge_after = Column(DateTime(timezone=True), nullable=True)  # Thời gian xóa vĩnh viễn (1 ngày sau trashed_at)
    
    # Relationships
    user = relationship("User", back_populates="user_devices")
    device_info = relationship("DeviceInfo", back_populates="user_devices")
    color = relationship("Color", back_populates="user_devices")
    device_storage = relationship("DeviceStorage", back_populates="user_devices")