import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class MessengerConversationState(Base):
    """
    Trạng thái hội thoại Messenger để tạm dừng auto-reply theo TTL.
    Khóa duy nhất theo (user_id, page_id, psid).
    """
    __tablename__ = "messenger_conversation_states"
    __table_args__ = (
        UniqueConstraint("user_id", "page_id", "psid", name="uq_messenger_conv_state_user_page_psid"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    page_id = Column(String, nullable=False)  # Page ID
    psid = Column(String, nullable=False)     # Người dùng (PSID)

    # Nếu còn hạn > now thì coi là đang paused; hết hạn thì coi như không paused
    paused_until = Column(DateTime, nullable=True)
    reason = Column(String, nullable=True)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
