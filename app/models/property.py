import uuid
from datetime import datetime
from sqlalchemy import Column, String, ForeignKey, DateTime, TypeDecorator
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.types import JSON
from app.database.database import Base

class Property(Base):
    __tablename__ = 'properties'

    id = Column(pgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    key = Column(String, nullable=False, index=True)
    # Store multiple values as a JSON array
    values = Column(MutableList.as_mutable(JSON), nullable=True, default=[])
    parent_id = Column(pgUUID(as_uuid=True), ForeignKey('properties.id'), nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship for adjacency list (parent-child)
    children = relationship("Property",
                            backref=backref('parent', remote_side=[id]),
                            cascade="all, delete-orphan")
    
    # Relationship to product components (removed due to schema change)
    # product_component_properties = relationship("ProductComponentProperty", back_populates="property")
    # product_components = relationship("ProductComponent", secondary="product_component_properties", back_populates="properties")

    def __repr__(self):
        return f"<Property(key='{self.key}', value='{self.value}')>"