from __future__ import annotations
import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OaBlockedUser(Base):
    __tablename__ = "oa_blocked_users"
    __table_args__ = (
        UniqueConstraint("oa_account_id", "blocked_user_id", name="uq_oa_blocked_per_account"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    oa_account_id = Column(PGUUID(as_uuid=True), ForeignKey("oa_accounts.id", ondelete="CASCADE"), nullable=False, index=True)

    # Zalo user id that is blocked for this OA account
    blocked_user_id = Column(String, nullable=False, index=True)
    note = Column(String, nullable=True)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
