from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, asc, desc, func
from sqlalchemy.orm import selectinload
from typing import Optional, List
import uuid

from app.models.device_brand import DeviceBrand
from app.models.user import User


class DeviceBrandRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    async def create(db: AsyncSession, device_brand: DeviceBrand) -> DeviceBrand:
        db.add(device_brand)
        await db.commit()
        await db.refresh(device_brand)
        return device_brand

    @staticmethod
    async def get_by_id(db: AsyncSession, device_brand_id: uuid.UUID) -> Optional[DeviceBrand]:
        query = select(DeviceBrand).where(DeviceBrand.id == device_brand_id)
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def get_by_name_and_user(db: AsyncSession, name: str, user_id: Optional[uuid.UUID]) -> Optional[DeviceBrand]:
        # If user_id is None (admin), find brand by name only (no user filter)
        # If user_id is provided, find brand by name and user_id
        if user_id is None:
            query = select(DeviceBrand).where(DeviceBrand.name == name, DeviceBrand.user_id.is_(None))
        else:
            query = select(DeviceBrand).where(DeviceBrand.name == name, DeviceBrand.user_id == user_id)
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def get_distinct_brands_for_user(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        search: Optional[str] = None
    ) -> List[DeviceBrand]:
        # Use DISTINCT ON to get unique rows based on the brand name.
        # Order by name, then by creation date to get the most recent entry for each name.
        query = select(DeviceBrand).distinct(DeviceBrand.name).order_by(DeviceBrand.name, DeviceBrand.created_at.desc())

        if user_id is not None:
            query = query.where(
                (DeviceBrand.user_id == user_id) | (DeviceBrand.user_id.is_(None))
            )

        if search:
            search_term = f"%{search}%"
            query = query.where(DeviceBrand.name.ilike(search_term))

        # The final ordering should match the DISTINCT ON clause for correctness
        query = query.order_by(DeviceBrand.name, DeviceBrand.created_at.desc())
        
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def get_all_for_user(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        skip: int = 0,
        limit: int = 100,
        search: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = 'asc'
    ) -> List[DeviceBrand]:
        # If user_id is None (admin), show all brands
        # If user_id is provided (regular user), show both:
        # 1. Brands with user_id = null (admin-created brands)
        # 2. Brands with user_id = provided user_id (user's own brands)
        if user_id is None:
            query = select(DeviceBrand)
        else:
            query = select(DeviceBrand).where(
                (DeviceBrand.user_id == user_id) | (DeviceBrand.user_id.is_(None))
            )

        if search:
            search_term = f"%{search}%"
            query = query.where(DeviceBrand.name.ilike(search_term))

        if sort_by:
            column = getattr(DeviceBrand, sort_by, None)
            if column:
                if sort_order == 'desc':
                    query = query.order_by(desc(column))
                else:
                    query = query.order_by(asc(column))
        
        query = query.offset(skip).limit(limit)
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def update(db: AsyncSession, device_brand: DeviceBrand) -> DeviceBrand:
        await db.commit()
        await db.refresh(device_brand)
        return device_brand

    @staticmethod
    async def delete(db: AsyncSession, device_brand: DeviceBrand) -> None:
        await db.delete(device_brand)
        await db.commit()

    @staticmethod
    async def count_all_for_user(db: AsyncSession, user_id: uuid.UUID) -> int:
        query = select(func.count(DeviceBrand.id)).where(DeviceBrand.user_id == user_id)
        result = await db.execute(query)
        return result.scalar_one()
