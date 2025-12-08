import uuid
import secrets
from sqlalchemy import Column, String, Boolean, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from app.database.database import Base
from app.utils.time import get_vn_now

def generate_api_key():
    return secrets.token_urlsafe(32)

class UserApiKey(Base):
    __tablename__ = 'user_api_keys'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete="CASCADE"), nullable=False, unique=True)
    api_key = Column(String, unique=True, nullable=False, default=generate_api_key, index=True)
    is_active = Column(Boolean, default=True)
    scopes = Column(ARRAY(String), nullable=False) # Danh sách services mà key này có quyền truy cập

    # Relationships
    user = relationship("User", back_populates="api_key")

    def __repr__(self):
        return f"<UserApiKey(user_id='{self.user_id}')>" 