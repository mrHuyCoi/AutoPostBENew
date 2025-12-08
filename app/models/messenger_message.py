import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Text, BigInteger, JSON, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class MessengerMessage(Base):
    """
    Lưu trữ tin nhắn Messenger (Facebook) đến/đi để phục vụ theo dõi và tránh trùng lặp khi retry.
    """
    __tablename__ = "messenger_messages"
    __table_args__ = (
        UniqueConstraint("message_mid", name="uq_messenger_message_mid"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Facebook identifiers
    page_id = Column(String, nullable=False)        # recipient.id (PAGE_ID)
    sender_id = Column(String, nullable=False)      # sender.id (PSID)
    recipient_id = Column(String, nullable=False)   # recipient.id (PAGE_ID)

    # Message content
    message_mid = Column(String, nullable=True)     # message.mid (unique per message)
    message_text = Column(Text, nullable=True)
    attachments = Column(JSON, nullable=True)

    # Timestamps
    timestamp_ms = Column(BigInteger, nullable=True)  # original webhook timestamp (ms)

    # Metadata
    direction = Column(String, nullable=False)      # 'in' | 'out'
    chatbot_used = Column(String, nullable=True)    # 'mobile' | 'custom'
    status = Column(String, nullable=False, default="received")  # 'received' | 'replied' | 'error'
    error = Column(Text, nullable=True)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
