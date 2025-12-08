import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Boolean, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OaConversation(Base):
    __tablename__ = "oa_conversations"
    __table_args__ = (
        UniqueConstraint("oa_account_id", "conversation_id", name="uq_oa_conversation_unique"),
        Index("ix_oa_conv_last_message_at", "oa_account_id", "last_message_at"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    oa_account_id = Column(PGUUID(as_uuid=True), ForeignKey("oa_accounts.id", ondelete="CASCADE"), nullable=False)

    conversation_id = Column(String, nullable=False)
    display_name = Column(String, nullable=True)
    type = Column(String, nullable=True)  # peer | group
    last_message_at = Column(DateTime, nullable=True)

    is_ignored = Column(Boolean, nullable=False, default=False)
    is_blocked = Column(Boolean, nullable=False, default=False)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
