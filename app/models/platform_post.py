import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.database.database import Base
from zoneinfo import ZoneInfo

# ✅ Hàm chuẩn hóa thời gian giờ Việt không timezone
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)

class PlatformPost(Base):
    """
    Model cho bảng platform_posts trong database.
    Lưu trữ thông tin bài đăng trên từng nền tảng mạng xã hội cụ thể, bao gồm lịch đăng và nội dung sinh tự động.
    """
    __tablename__ = "platform_posts"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    social_account_id = Column(UUID(as_uuid=True), ForeignKey("social_accounts.id", ondelete="CASCADE"), nullable=False)
    platform = Column(String, nullable=False)  # 'facebook', 'instagram', 'threads', 'youtube'
    platform_post_id = Column(String, nullable=True)  # ID của bài đăng trên nền tảng
    post_url = Column(String, nullable=True)  # URL của bài đăng
    status = Column(String, nullable=False, default="scheduled")  # 'scheduled', 'generating', 'ready', 'published', 'failed'
    platform_specific_data = Column(JSON, nullable=True)  # Dữ liệu đặc thù cho từng nền tảng (tags, mô tả...)
    scheduled_at = Column(DateTime, nullable=True)  # Thời gian hẹn đăng
    generated_content = Column(Text, nullable=True)  # Nội dung AI đã sinh
    published_at = Column(DateTime, nullable=True)  # Thời gian bài đăng được xuất bản
    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)

    # Relationships
    social_account = relationship("SocialAccount", back_populates="platform_posts")
    youtube_metadata = relationship("YouTubeMetadata", back_populates="platform_post", uselist=False)
    media_assets = relationship(
        "MediaAsset",
        secondary="platform_post_media_asset",
        back_populates="platform_posts",
    )