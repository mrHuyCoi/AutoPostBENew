import uuid
from datetime import datetime
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base


class WarrantyService(Base):
    """
    Model cho bảng warranty_services trong database.
    Lưu trữ thông tin về các dịch vụ bảo hành do người dùng tạo.
    """
    __tablename__ = "warranty_services"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    value = Column(String, nullable=False, index=True)
    
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="warranty_services")

    def __repr__(self):
        return f"<WarrantyService(value='{self.value}', user_id='{self.user_id}')>" 