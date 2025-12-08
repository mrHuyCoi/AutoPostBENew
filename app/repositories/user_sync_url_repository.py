from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import uuid
from typing import Optional

from app.models.user_sync_url import UserSyncUrl


class UserSyncUrlRepository:
    @staticmethod
    async def upsert(db: AsyncSession, user_id: uuid.UUID, url: str, is_active: bool = True, type_url: Optional[str] = None, url_today: Optional[str] = None) -> UserSyncUrl:
        """
        Upsert a sync URL per user and type. If type_url is provided, ensure we only update/create that type.
        If type_url is None, operate on the first record without type, else create a new one without type.
        """
        query = select(UserSyncUrl).filter(UserSyncUrl.user_id == user_id)
        if type_url is not None:
            query = query.filter(UserSyncUrl.type_url == type_url)
        else:
            query = query.filter(UserSyncUrl.type_url == None)  # noqa: E711
        result = await db.execute(query)
        record = result.scalars().first()
        if record:
            record.url = url
            record.is_active = is_active
            # Do not change record.type_url unless explicitly creating or it is None
            if type_url is not None and record.type_url != type_url:
                record.type_url = type_url
            if url_today is not None:
                record.url_today = url_today
        else:
            record = UserSyncUrl(user_id=user_id, url=url, is_active=is_active, type_url=type_url, url_today=url_today)
            db.add(record)
        await db.commit()
        await db.refresh(record)
        return record

    @staticmethod
    async def get_by_user_id(db: AsyncSession, user_id: uuid.UUID, only_active: bool = True, type_url: Optional[str] = None) -> Optional[UserSyncUrl]:
        query = select(UserSyncUrl).filter(UserSyncUrl.user_id == user_id)
        if type_url is not None:
            query = query.filter(UserSyncUrl.type_url == type_url)
        if only_active:
            query = query.filter(UserSyncUrl.is_active == True)
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def update(db: AsyncSession, user_id: uuid.UUID, url: Optional[str] = None, is_active: Optional[bool] = None, type_url: Optional[str] = None, url_today: Optional[str] = None) -> Optional[UserSyncUrl]:
        """
        Update a user's sync URL record filtered by type_url if provided. If not found, return None.
        """
        query = select(UserSyncUrl).filter(UserSyncUrl.user_id == user_id)
        if type_url is not None:
            query = query.filter(UserSyncUrl.type_url == type_url)
        result = await db.execute(query)
        record = result.scalars().first()
        if not record:
            return None
        if url is not None:
            record.url = url
        if is_active is not None:
            record.is_active = is_active
        # Do not mutate type_url here to avoid moving records across types inadvertently
        if url_today is not None:
            record.url_today = url_today
        await db.commit()
        await db.refresh(record)
        return record

    @staticmethod
    async def deactivate(db: AsyncSession, user_id: uuid.UUID, type_url: Optional[str] = None) -> bool:
        """
        Deactivate sync URL(s). If type_url provided, deactivate that record only; else deactivate all for user.
        """
        query = select(UserSyncUrl).filter(UserSyncUrl.user_id == user_id)
        if type_url is not None:
            query = query.filter(UserSyncUrl.type_url == type_url)
        result = await db.execute(query)
        records = result.scalars().all()
        if not records:
            return False
        for record in records:
            record.is_active = False
        await db.commit()
        return True
