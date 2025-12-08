from typing import List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.warranty_service import WarrantyService
from app.schemas.warranty_service import WarrantyServiceCreate, WarrantyServiceUpdate


class WarrantyServiceRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_warranty_service(self, warranty_service: WarrantyServiceCreate, user_id: UUID) -> WarrantyService:
        db_warranty_service = WarrantyService(**warranty_service.dict(), user_id=user_id)
        self.db.add(db_warranty_service)
        await self.db.commit()
        await self.db.refresh(db_warranty_service)
        return db_warranty_service

    async def get_warranty_service(self, warranty_service_id: UUID) -> Optional[WarrantyService]:
        result = await self.db.execute(select(WarrantyService).where(WarrantyService.id == warranty_service_id))
        return result.scalars().first()

    async def get_warranty_services_by_user(self, user_id: UUID, skip: int = 0, limit: int = 100) -> List[WarrantyService]:
        result = await self.db.execute(
            select(WarrantyService)
            .where(WarrantyService.user_id == user_id)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def update_warranty_service(self, warranty_service_id: UUID, warranty_service_update: WarrantyServiceUpdate) -> Optional[WarrantyService]:
        db_warranty_service = await self.get_warranty_service(warranty_service_id)
        if db_warranty_service:
            update_data = warranty_service_update.dict(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_warranty_service, key, value)
            await self.db.commit()
            await self.db.refresh(db_warranty_service)
        return db_warranty_service

    async def delete_warranty_service(self, warranty_service_id: UUID) -> Optional[WarrantyService]:
        db_warranty_service = await self.get_warranty_service(warranty_service_id)
        if db_warranty_service:
            await self.db.delete(db_warranty_service)
            await self.db.commit()
        return db_warranty_service

repo = WarrantyServiceRepository