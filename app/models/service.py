import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, func 
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from app.database.database import Base

class Service(Base):
    """
    Model cho bảng dịch vụ.
    (Đã sửa để ánh xạ CSDL và thêm relationship)
    """
    __tablename__ = "services"

    # --- Các cột CSDL đã có và khớp ---
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    
    # --- Ánh xạ các cột có tên không khớp ---
    # 'name' trong code (FE dùng) sẽ được ánh xạ tới cột 'loai' trong CSDL
    name = Column("loai", String, nullable=False, index=True) 
    
    # 'thuonghieu' trong code (FE dùng) sẽ được ánh xạ tới cột 'thuonghieu' trong CSDL
    thuonghieu = Column("thuonghieu", String, nullable=True)

    # 'description' trong code (FE dùng) sẽ được ánh xạ tới cột 'loaimay' trong CSDL
    description = Column("loaimay", String, nullable=True)

    # 'mausac' trong code (FE dùng) sẽ được ánh xạ tới cột 'mausac' trong CSDL
    mausac = Column("mausac", String, nullable=True)

    # 'price' trong code (FE dùng) sẽ được ánh xạ tới cột 'gia' trong CSDL
    price = Column("gia", String, nullable=True)

    # 'warranty' trong code (FE dùng) sẽ được ánh xạ tới cột 'baohanh' trong CSDL
    warranty = Column("baohanh", String, nullable=True)

    # 'note' trong code (FE dùng) sẽ được ánh xạ tới cột 'ghichu' trong CSDL
    note = Column("ghichu", String, nullable=True)

    # --- Timestamps (Dùng hàm của CSDL) ---
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    trashed_at = Column(DateTime(timezone=True), nullable=True) 
    purge_after = Column(DateTime(timezone=True), nullable=True) 

    # --- Relationships ---
    user = relationship("User", backref="services")
    
    # Thêm lại relationship này để khớp với 'brand.py'
    # Dòng này khai báo 'Service' có nhiều 'Brand'
    # và liên kết ngược lại với thuộc tính 'service' trong model 'Brand'
    brands = relationship("Brand", back_populates="service", cascade="all, delete-orphan")
