from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Enum
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
import enum

from app.database.database import Base
from zoneinfo import ZoneInfo

# ✅ Chuẩn hóa datetime theo giờ Việt Nam, không có tzinfo
def now_vn_naive():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class YouTubeContentType(enum.Enum):
    """Loại nội dung YouTube"""
    REGULAR = "regular"  # Video thường
    SHORTS = "shorts"    # YouTube Shorts


class YouTubeMetadata(Base):
    """
    Model cho bảng youtube_metadata trong database.
    Lưu trữ thông tin đặc thù cho bài đăng YouTube.
    """
    __tablename__ = "youtube_metadata"
    
    platform_post_id = Column(UUID(as_uuid=True), ForeignKey("platform_posts.id", ondelete="CASCADE"), primary_key=True)
    content_type = Column(Enum(YouTubeContentType), nullable=False, default=YouTubeContentType.REGULAR)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    tags = Column(ARRAY(String), nullable=True)
    privacy_status = Column(String, default="public")  # 'public', 'private', 'unlisted'
    
    # Thông tin đặc thù cho Shorts
    shorts_hashtags = Column(ARRAY(String), nullable=True)  # Hashtags cho Shorts
    shorts_music = Column(String, nullable=True)  # Tên nhạc nền cho Shorts

    # ✅ Thời gian tạo/cập nhật
    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
    
    # Relationships
    platform_post = relationship("PlatformPost", back_populates="youtube_metadata")
