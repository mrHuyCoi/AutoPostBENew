import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, Text, JSON, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OaMessage(Base):
    __tablename__ = "oa_messages"
    __table_args__ = (
        UniqueConstraint("message_id_from_zalo", name="uq_oa_message_mid"),
        Index("ix_oa_msg_conv_time", "oa_account_id", "conversation_id", "timestamp"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    oa_account_id = Column(PGUUID(as_uuid=True), ForeignKey("oa_accounts.id", ondelete="CASCADE"), nullable=False)
    conversation_id = Column(String, nullable=False)

    direction = Column(String, nullable=False)  # 'in' | 'out'
    msg_type = Column(String, nullable=True)    # 'text' | 'image' | ...
    text = Column(Text, nullable=True)
    attachments = Column(JSON, nullable=True)

    message_id_from_zalo = Column(String, nullable=True)
    timestamp = Column(DateTime, nullable=True)

    delivery_status = Column(String, nullable=True)
    raw_payload = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
