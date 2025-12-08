from sqlalchemy import Column, ForeignKey, Table
from sqlalchemy.dialects.postgresql import UUID
from app.database.database import Base

platform_post_media_asset = Table(
    "platform_post_media_asset",
    Base.metadata,
    Column("platform_post_id", UUID(as_uuid=True), ForeignKey("platform_posts.id", ondelete="CASCADE"), primary_key=True),
    Column("media_asset_id", UUID(as_uuid=True), ForeignKey("media_assets.id", ondelete="CASCADE"), primary_key=True),
) 