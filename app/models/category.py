import uuid
from datetime import datetime
from sqlalchemy import Column, String, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID as pgUUID
from sqlalchemy.orm import relationship, backref
from app.database.database import Base

class Category(Base):
    __tablename__ = 'categories'

    id = Column(pgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False, index=True)
    parent_id = Column(pgUUID(as_uuid=True), ForeignKey('categories.id'), nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship for adjacency list (parent-child)
    children = relationship("Category",
                            backref=backref('parent', remote_side=[id]),
                            cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Category(name='{self.name}')>"