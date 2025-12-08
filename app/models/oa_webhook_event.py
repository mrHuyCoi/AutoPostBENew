import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, JSON, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OaWebhookEvent(Base):
    __tablename__ = "oa_webhook_events"
    __table_args__ = (
        UniqueConstraint("dedupe_id", name="uq_oa_webhook_dedupe"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type = Column(String, nullable=True)
    dedupe_id = Column(String, nullable=True)
    payload = Column(JSON, nullable=False)

    received_at = Column(DateTime, default=now_vn_naive)
    processed_at = Column(DateTime, nullable=True)
    process_status = Column(String, nullable=False, default="pending")
