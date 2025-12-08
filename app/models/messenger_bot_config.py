import uuid
from datetime import datetime
from sqlalchemy import Column, String, Boolean, DateTime, ForeignKey, UniqueConstraint, Integer
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class MessengerBotConfig(Base):
    """
    Cấu hình bật/tắt chatbot cho Messenger theo từng Page.
    """
    __tablename__ = "messenger_bot_configs"
    __table_args__ = (
        UniqueConstraint("user_id", "page_id", name="uq_messenger_bot_cfg_user_page"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    page_id = Column(String, nullable=False)  # ID Trang Facebook

    # Toggles
    mobile_enabled = Column(Boolean, nullable=False, default=True)
    custom_enabled = Column(Boolean, nullable=False, default=False)

    # TTL tạm dừng tự động (phút) khi phát hiện người thật takeover qua echo; 0 = tắt tính năng
    pause_ttl_minutes = Column(Integer, nullable=False, default=10)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
