import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OauthState(Base):
    __tablename__ = "oauth_states"
    __table_args__ = (
        UniqueConstraint("state", name="uq_oauth_state_state"),
    )

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state = Column(String, nullable=False)
    code_verifier = Column(String, nullable=False)
    user_id = Column(PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    redirect_uri = Column(String, nullable=True)

    created_at = Column(DateTime, default=now_vn_naive)
    used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
