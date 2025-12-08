import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from app.database.database import Base
# from app.utils.time import get_vn_now # Bỏ

class Brand(Base):
    """
    Model cho bảng thương hiệu, liên kết với dịch vụ.
    (Đã sửa timestamp)
    """
    __tablename__ = "brands"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    warranty = Column(String, nullable=False)
    note = Column(String, nullable=True)
    service_id = Column(UUID(as_uuid=True), ForeignKey("services.id"), nullable=False)
    device_brand_id = Column(UUID(as_uuid=True), ForeignKey("device_brands.id"), nullable=True)
    
    # SỬA: Dùng `func.now()` của CSDL
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    trashed_at = Column(DateTime(timezone=True), nullable=True)
    purge_after = Column(DateTime(timezone=True), nullable=True)
    price = Column(String, nullable=True)
    wholesale_price = Column(String, nullable=True)
    device_type = Column(String, nullable=True)
    color = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint('service_id', 'service_code', name='uq_brands_service_service_code'),
    )

    # Relationships (Phần này đã ĐÚNG)
    service = relationship("Service", back_populates="brands")
    device_brand = relationship("DeviceBrand", back_populates="brands")