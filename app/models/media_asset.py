import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from app.database.database import Base


class MediaAsset(Base):
    """
    Model cho bảng media_assets trong database.
    Lưu trữ thông tin media (hình ảnh, video) được tải lên.
    """
    __tablename__ = "media_assets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    storage_path = Column(String, nullable=False)  # đường dẫn trên cloud storage
    url = Column(JSONB, nullable=True)  # URL hoặc danh sách URL công khai (lưu JSONB)
    file_name = Column(String, nullable=False)  # tên gốc của file (NOT NULL)
    file_type = Column(String, nullable=False)  # MIME type (NOT NULL in DB)
    size_bytes = Column(Integer, nullable=True) # Kích thước file tính bằng bytes
    duration = Column(Integer, nullable=True)  # Thời lượng video (giây)
    brand_name = Column(String, nullable=True)  # Tên thương hiệu liên quan đến media
    posting_purpose = Column(Text, nullable=True)  # Mục đích đăng nội dung
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    prompt_for_content = Column(String, nullable=True)  # Prompt sinh nội dung cho media này
    
    # Relationships
    user = relationship("User", back_populates="media_assets")
    platform_posts = relationship(
        "PlatformPost",
        secondary="platform_post_media_asset",
        back_populates="media_assets",
    )