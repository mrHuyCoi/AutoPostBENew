from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import Optional
import uuid

from app.models.user_api_key import UserApiKey, generate_api_key

class UserApiKeyRepository:
    @staticmethod
    async def create_or_update_api_key(db: AsyncSession, user_id: uuid.UUID, scopes: list[str]) -> UserApiKey:
        result = await db.execute(select(UserApiKey).filter(UserApiKey.user_id == user_id))
        api_key_obj = result.scalars().first()

        if api_key_obj:
            # Update scopes if key exists
            api_key_obj.scopes = scopes
            api_key_obj.is_active = True
        else:
            # Create new key
            api_key_obj = UserApiKey(user_id=user_id, scopes=scopes)
            db.add(api_key_obj)
        
        await db.commit()
        await db.refresh(api_key_obj)
        return api_key_obj

    @staticmethod
    async def get_by_api_key(db: AsyncSession, api_key: str) -> Optional[UserApiKey]:
        result = await db.execute(
            select(UserApiKey)
            .filter(UserApiKey.api_key == api_key, UserApiKey.is_active == True)
        )
        return result.scalars().first()

    @staticmethod
    async def get_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> Optional[UserApiKey]:
        result = await db.execute(select(UserApiKey).filter(UserApiKey.user_id == user_id))
        return result.scalars().first()
        
    @staticmethod
    async def regenerate_api_key(db: AsyncSession, user_id: uuid.UUID) -> Optional[UserApiKey]:
        result = await db.execute(select(UserApiKey).filter(UserApiKey.user_id == user_id))
        api_key_obj = result.scalars().first()

        if api_key_obj:
            api_key_obj.api_key = generate_api_key()
            await db.commit()
            await db.refresh(api_key_obj)
        
        return api_key_obj

    @staticmethod
    async def deactivate_api_key(db: AsyncSession, user_id: uuid.UUID) -> Optional[UserApiKey]:
        """Vô hiệu hoá API key của người dùng (nếu tồn tại)."""
        result = await db.execute(select(UserApiKey).filter(UserApiKey.user_id == user_id))
        api_key_obj = result.scalars().first()
        if api_key_obj:
            api_key_obj.is_active = False
            await db.commit()
            await db.refresh(api_key_obj)
        return api_key_obj
 