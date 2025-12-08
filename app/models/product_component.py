import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, Text, ForeignKey, Numeric, DateTime, Index, text
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy.orm import relationship
from app.database.database import Base

class ProductComponent(Base):
    __tablename__ = 'product_components'

    id = Column(pgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_code = Column(String, nullable=False, index=True)  # Bỏ unique=True
    product_name = Column(String, nullable=False, index=True)
    amount = Column(Numeric(10, 2), nullable=False, default=0)
    wholesale_price = Column(Numeric(10, 2), nullable=True)  # Giá bán buôn
    trademark = Column(String, nullable=True)
    guarantee = Column(String, nullable=True) # Bảo hành
    stock = Column(Integer, nullable=False, default=0) # Tồn kho
    description = Column(Text, nullable=True)
    product_photo = Column(String, nullable=True)
    product_link = Column(String, nullable=True)
    properties = Column(Text, nullable=True) # Store properties as a JSON string with Unicode support
    category = Column(String, nullable=True) # Store category name as string
    
    # Foreign Keys
    user_id = Column(pgUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    trashed_at = Column(DateTime, nullable=True)  # Thời gian xóa mềm
    purge_after = Column(DateTime, nullable=True)  # Thời gian xóa vĩnh viễn (1 ngày sau trashed_at)

    # Relationships
    user = relationship("User", back_populates="product_components")

    # Tạo unique constraint theo user_id và product_code
    __table_args__ = (
        Index(
            'uq_product_components_user_code_active',
            'user_id',
            'product_code',
            unique=True,
            postgresql_where=text('trashed_at IS NULL')
        ),
    )

    def __repr__(self):
        return f"<ProductComponent(product_name='{self.product_name}', product_code='{self.product_code}')>" 