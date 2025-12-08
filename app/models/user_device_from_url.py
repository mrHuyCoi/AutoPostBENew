import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, ForeignKey, Integer, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base
from app.utils.time import get_vn_now


class UserDeviceFromUrl(Base):
    """
    Bảng lưu trữ thiết bị đồng bộ từ URL người dùng.
    Tương tự `UserDevice`, bổ sung liên kết đến `UserSyncUrl` để biết nguồn dữ liệu.
    """
    __tablename__ = "user_devices_from_url"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    sync_url_id = Column(UUID(as_uuid=True), ForeignKey("user_sync_urls.id"), nullable=True, index=True)

    device_info_id = Column(UUID(as_uuid=True), ForeignKey("device_info.id"), nullable=True)
    color_id = Column(UUID(as_uuid=True), ForeignKey("colors.id"), nullable=True)
    device_storage_id = Column(UUID(as_uuid=True), ForeignKey("device_storage.id"), nullable=True)

    product_code = Column(String, nullable=True)
    device_name = Column(String, nullable=True)
    warranty = Column(String, nullable=True)
    device_condition = Column(String, nullable=False)
    device_type = Column(String, nullable=False)
    battery_condition = Column(String, nullable=True)
    price = Column(Float, nullable=False)
    wholesale_price = Column(Float, nullable=True)
    inventory = Column(Integer, default=0)
    notes = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), default=get_vn_now)
    updated_at = Column(DateTime(timezone=True), default=get_vn_now, onupdate=get_vn_now)
    trashed_at = Column(DateTime(timezone=True), nullable=True)
    purge_after = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    user = relationship("User", back_populates="user_devices_from_url")
    sync_url = relationship("UserSyncUrl", back_populates="devices")

    # Không yêu cầu back_populates phía DeviceInfo/Color/DeviceStorage để tránh sửa nhiều file
    device_info = relationship("DeviceInfo")
    color = relationship("Color")
    device_storage = relationship("DeviceStorage")

    def __repr__(self):
        return (
            f"<UserDeviceFromUrl(user_id='{self.user_id}', model_id='{self.device_info_id}', "
            f"color_id='{self.color_id}', storage_id='{self.device_storage_id}', product_code='{self.product_code}')>"
        )
