from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.user_bot_control import UserBotControl


class UserBotControlRepository:
    @staticmethod
    async def get_by_user_and_platform(db: AsyncSession, user_id, platform: str) -> Optional[UserBotControl]:
        plat = (platform or "").strip().lower()
        if not plat:
            return None
        stmt = select(UserBotControl).where(
            UserBotControl.user_id == user_id,
            UserBotControl.platform == plat,
        )
        res = await db.execute(stmt)
        return res.scalars().first()

    @staticmethod
    async def is_enabled(db: AsyncSession, user_id, platform: str) -> bool:
        # If no record found, treat as enabled by default to preserve current behavior
        rec = await UserBotControlRepository.get_by_user_and_platform(db, user_id, platform)
        if rec is None:
            return True
        return bool(rec.enabled)
