from sqlalchemy import Column, String, DateTime, func, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.database.database import Base
from .associations import device_material_association

class Material(Base):
    __tablename__ = "materials"

    id = Column(UUID(as_uuid=True), primary_key=True, index=True, default=func.gen_random_uuid())
    name = Column(String, index=True, nullable=False)  # Tên vật liệu (ví dụ: Nhôm, Thép không gỉ, Nhựa, Kính, v.v.)
    description = Column(String, nullable=True)  # Mô tả chi tiết về vật liệu
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Quan hệ nhiều-nhiều với DeviceInfo - sử dụng string reference để tránh circular import
    device_infos = relationship("DeviceInfo", secondary=device_material_association, back_populates="materials", lazy="noload")
