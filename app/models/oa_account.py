import uuid
from datetime import datetime
from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from zoneinfo import ZoneInfo

from app.database.database import Base


def now_vn_naive() -> datetime:
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(tzinfo=None)


class OaAccount(Base):
    __tablename__ = "oa_accounts"

    id = Column(PGUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_user_id = Column(PGUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)

    # Zalo OA identifiers/info
    oa_id = Column(String, nullable=False, index=True)  # id from /me result
    oa_name = Column(String, nullable=True)
    picture_url = Column(String, nullable=True)

    app_id = Column(String, nullable=True)
    status = Column(String, nullable=False, default="connected")  # connected | revoked | error
    connected_at = Column(DateTime, default=now_vn_naive)

    created_at = Column(DateTime, default=now_vn_naive)
    updated_at = Column(DateTime, default=now_vn_naive, onupdate=now_vn_naive)
