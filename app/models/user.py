import uuid
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text, LargeBinary
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, Session
from datetime import datetime, timezone
from app.database.database import Base


class User(Base):
    """
    Model cho bảng users trong database.
    Lưu trữ thông tin người dùng của hệ thống.
    """
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=True)
    google_id = Column(String, unique=True, nullable=True, index=True)
    full_name = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    role = Column(String, nullable=False, default='user')  # 'user' or 'admin'
    is_superuser = Column(Boolean, default=False)
    custom_system_prompt = Column(Text, nullable=True, default="Bạn là một chuyên gia sáng tạo nội dung cho video viral và bài viết trên mạng xã hội. Bạn luôn viết nội dung súc tích, thu hút, chuyên nghiệp, dễ lan truyền (viral), độ dài phụ thuộc vào prompt được cung cấp (nhưng nếu prompt không nói gì thì bạn viết với độ dài bình thường tầm 3 - 5 câu nhé) và phù hợp từng nền tảng. Không nên trả lời thêm ra các nội dung thừa như bạn đúng rồi, tuyệt vời như cách bạn hỏi đáp")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    gemini_api_key = Column(String, nullable=True)
    openai_api_key = Column(String, nullable=True)
    
    # Relationships
    
    subscription = relationship("UserSubscription", back_populates="user", uselist=False, cascade="all, delete-orphan")
    media_assets = relationship("MediaAsset", back_populates="user", cascade="all, delete-orphan")
    social_accounts = relationship("SocialAccount", back_populates="user", cascade="all, delete-orphan")
    user_devices = relationship("UserDevice", back_populates="user", cascade="all, delete-orphan")
    device_brands = relationship("DeviceBrand", back_populates="user", cascade="all, delete-orphan")
    product_components = relationship("ProductComponent", back_populates="user", cascade="all, delete-orphan")
    warranty_services = relationship("WarrantyService", back_populates="user", cascade="all, delete-orphan")
    # Per-user sync URLs (allow multiple types: device/component/service)
    sync_urls = relationship("UserSyncUrl", back_populates="user", cascade="all, delete-orphan")
    # User devices synced from URL (separate table)
    user_devices_from_url = relationship("UserDeviceFromUrl", back_populates="user", cascade="all, delete-orphan")

    # Chatbot Relationships
    chatbot_subscriptions = relationship("UserChatbotSubscription", back_populates="user", cascade="all, delete-orphan")
    api_key = relationship("UserApiKey", back_populates="user", uselist=False, cascade="all, delete-orphan")
    bot_controls = relationship("UserBotControl", back_populates="user", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(email='{self.email}')>"
