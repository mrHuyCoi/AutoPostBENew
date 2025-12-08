from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, Integer, or_
from sqlalchemy.orm import joinedload
from typing import Optional, List
import uuid

from app.models.user_device_from_url import UserDeviceFromUrl
from app.models.device_storage import DeviceStorage
from app.models.color import Color
from app.models.device_info import DeviceInfo


class UserDeviceFromUrlRepository:
    @staticmethod
    async def create(db: AsyncSession, data: dict) -> UserDeviceFromUrl:
        obj = UserDeviceFromUrl(**data)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj

    @staticmethod
    async def get_by_product_code_and_user_id(db: AsyncSession, product_code: str, user_id: uuid.UUID) -> Optional[UserDeviceFromUrl]:
        if not product_code:
            return None
        result = await db.execute(
            select(UserDeviceFromUrl)
            .where(UserDeviceFromUrl.product_code == product_code)
            .where(UserDeviceFromUrl.user_id == user_id)
        )
        return result.scalars().first()

    @staticmethod
    async def update(db: AsyncSession, entity_id: uuid.UUID, data: dict) -> Optional[UserDeviceFromUrl]:
        result = await db.execute(select(UserDeviceFromUrl).where(UserDeviceFromUrl.id == entity_id))
        obj = result.scalars().first()
        if not obj:
            return None
        update_data = {k: v for k, v in data.items() if v is not None}
        for key, value in update_data.items():
            setattr(obj, key, value)
        await db.commit()
        await db.refresh(obj)
        return obj

    @staticmethod
    async def get_by_id(db: AsyncSession, entity_id: uuid.UUID) -> Optional[UserDeviceFromUrl]:
        result = await db.execute(select(UserDeviceFromUrl).where(UserDeviceFromUrl.id == entity_id))
        return result.scalars().first()

    # --- Listing helpers (filters/sort/pagination) ---
    @staticmethod
    def _apply_filters(query, filters: Optional[dict] = None):
        if not filters:
            return query

        # Search by product_code or device_name (avoid join to DeviceInfo)
        if filters.get("search"):
            search_term = f"%{filters['search']}%"
            query = query.where(
                or_(
                    UserDeviceFromUrl.product_code.ilike(search_term),
                    UserDeviceFromUrl.notes.ilike(search_term) if hasattr(UserDeviceFromUrl, 'notes') else UserDeviceFromUrl.product_code.ilike(search_term),
                    UserDeviceFromUrl.device_name.ilike(search_term)
                )
            )

        # Skip brand-based filtering to avoid joining DeviceInfo
        # if filters.get("brand"):
        #     pass

        if filters.get("inventory_min") is not None:
            query = query.where(UserDeviceFromUrl.inventory >= filters["inventory_min"])
        if filters.get("inventory_max") is not None:
            query = query.where(UserDeviceFromUrl.inventory <= filters["inventory_max"])

        if filters.get("price_min") is not None:
            query = query.where(UserDeviceFromUrl.price >= filters["price_min"])
        if filters.get("price_max") is not None:
            query = query.where(UserDeviceFromUrl.price <= filters["price_max"])

        if filters.get("wholesale_price_min") is not None:
            query = query.where(UserDeviceFromUrl.wholesale_price >= filters["wholesale_price_min"])
        if filters.get("wholesale_price_max") is not None:
            query = query.where(UserDeviceFromUrl.wholesale_price <= filters["wholesale_price_max"])

        if filters.get("storage_capacity") is not None:
            # We need to join with DeviceStorage to filter by capacity
            query = query.join(DeviceStorage).where(DeviceStorage.capacity == filters["storage_capacity"]) 

        return query

    @staticmethod
    async def get_by_user_id_with_details(
        db: AsyncSession,
        user_id: uuid.UUID,
        skip: int = 0,
        limit: Optional[int] = 100,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = "asc",
        filters: Optional[dict] = None,
    ) -> List[UserDeviceFromUrl]:
        query = (
            select(UserDeviceFromUrl)
            .options(
                joinedload(UserDeviceFromUrl.device_info),
                joinedload(UserDeviceFromUrl.color),
                joinedload(UserDeviceFromUrl.device_storage),
            )
            .where(UserDeviceFromUrl.user_id == user_id, UserDeviceFromUrl.trashed_at.is_(None))
        )

        query = UserDeviceFromUrlRepository._apply_filters(query, filters)

        # Sorting
        if sort_by:
            if hasattr(UserDeviceFromUrl, sort_by):
                column = getattr(UserDeviceFromUrl, sort_by)
                if sort_order == "desc":
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column.asc())
            elif sort_by in ["storageCapacity", "capacity"]:
                # JOIN cứng vào DeviceStorage để order_by hoạt động
                query = query.join(DeviceStorage, UserDeviceFromUrl.device_storage_id == DeviceStorage.id)
                if sort_order == "desc":
                    query = query.order_by(DeviceStorage.capacity.desc())
                else:
                    query = query.order_by(DeviceStorage.capacity.asc())
            elif sort_by in ["colorName", "color", "name"]:
                # JOIN cứng vào Color để order_by hoạt động
                query = query.join(Color, UserDeviceFromUrl.color_id == Color.id)
                if sort_order == "desc":
                    query = query.order_by(Color.name.desc())
                else:
                    query = query.order_by(Color.name.asc())
            elif sort_by in ["wholesale_price"]:
                if sort_order == "desc":
                    query = query.order_by(UserDeviceFromUrl.wholesale_price.desc().nullslast())
                else:
                    query = query.order_by(UserDeviceFromUrl.wholesale_price.asc().nullslast())

        query = query.offset(skip)
        if limit is not None:
            query = query.limit(limit)

        result = await db.execute(query)
        return result.scalars().unique().all()

    @staticmethod
    async def count_by_user_id(db: AsyncSession, user_id: uuid.UUID, filters: Optional[dict] = None) -> int:
        query = select(func.count(UserDeviceFromUrl.id)).where(
            UserDeviceFromUrl.user_id == user_id,
            UserDeviceFromUrl.trashed_at.is_(None),
        )
        query = UserDeviceFromUrlRepository._apply_filters(query, filters)
        result = await db.execute(query)
        return result.scalar_one()
