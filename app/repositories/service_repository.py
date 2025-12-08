from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, exists, and_, or_
from sqlalchemy.orm import selectinload
from typing import Optional, List
import uuid
from datetime import date

from app.models.service import Service
from app.models.brand import Brand
from app.dto.service_dto import ServiceUpdate

class ServiceRepository:
    @staticmethod
    async def create(db: AsyncSession, service: Service) -> Service:
        db.add(service)
        await db.commit()
        await db.refresh(service)
        return service

    @staticmethod
    async def get_by_id(db: AsyncSession, service_id: uuid.UUID) -> Optional[Service]:
        query = select(Service).where(Service.id == service_id, Service.trashed_at.is_(None))
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def get_by_name(db: AsyncSession, name: str, user_id: uuid.UUID) -> Optional[Service]:
        # Hàm này ĐÚNG vì Service.name đã được map sang 'loai'
        normalized_db_name = func.lower(func.trim(func.regexp_replace(Service.name, r"\s+", " ", "g")))
        normalized_input_name = func.lower(func.trim(func.regexp_replace(name, r"\s+", " ", "g")))
        query = (
            select(Service)
            .where(
                normalized_db_name == normalized_input_name,
                Service.user_id == user_id,
                Service.trashed_at.is_(None)
            )
            .order_by(Service.created_at.desc())
            .limit(1)
        )
        result = await db.execute(query)
        return result.scalars().first()

    @staticmethod
    async def update(db: AsyncSession, service_id: uuid.UUID, data: ServiceUpdate) -> Optional[Service]:
        # Hàm này ĐÚNG
        db_service = await ServiceRepository.get_by_id(db, service_id)
        if not db_service:
            return None
        # DTO 'ServiceUpdate' đã được sửa, nên 'data' sẽ có trường đúng
        update_data = data.model_dump(exclude_unset=True) 
        for key, value in update_data.items():
            setattr(db_service, key, value)
        await db.commit()
        await db.refresh(db_service)
        return db_service

    @staticmethod
    async def delete(db: AsyncSession, service_id: uuid.UUID) -> bool:
        db_service = await ServiceRepository.get_by_id(db, service_id)
        if not db_service:
            return False
        await db.delete(db_service)
        await db.commit()
        return True

    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 1000, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[Service]:
        # Hàm này chỉ gọi hàm bên dưới, nên nó sẽ tự động được fix
        return await ServiceRepository.get_all_with_product_count(db, skip, limit, search, user_id)

    @staticmethod
    async def count_all(db: AsyncSession, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> int:
        query = select(func.count(Service.id)).where(Service.trashed_at.is_(None)) 
        
        if user_id:
            query = query.where(Service.user_id == user_id)
            
        # SỬA 1: Sửa logic tìm kiếm (khớp với Model mới)
        if search:
            search_term = f"%{search}%"
            # Tìm kiếm theo 3 cột FE gửi lên (name=loai, description=loaimay, thuonghieu)
            query = query.where(
                or_(
                    Service.name.ilike(search_term),         # 'loai'
                    Service.thuonghieu.ilike(search_term), # 'thuonghieu'
                    Service.description.ilike(search_term) # 'loaimay'
                )
            )
            
        result = await db.execute(query)
        return result.scalar_one()

    @staticmethod
    async def get_all_with_product_count(db: AsyncSession, skip: int = 0, limit: int = 1000, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[Service]:
        """
        SỬA 2: Sửa toàn bộ hàm này (Fix N+1 Query và Logic Search)
        """
        
        # 1. Tạo SubQuery để đếm 'brands'
        brand_count_subquery = (
            select(
                Brand.service_id, 
                func.count(Brand.id).label("product_count")
            )
            .where(Brand.trashed_at.is_(None))
            .group_by(Brand.service_id)
            .subquery()
        )

        # 2. Tạo Query chính
        query = (
            select(
                Service, 
                func.coalesce(brand_count_subquery.c.product_count, 0).label("product_count")
            )
            .outerjoin(
                brand_count_subquery, 
                Service.id == brand_count_subquery.c.service_id
            )
            .where(Service.trashed_at.is_(None))
        )

        if user_id:
            query = query.where(Service.user_id == user_id)
        
        # 3. Sửa logic Search (giống hệt count_all)
        if search:
            search_term = f"%{search}%"
            query = query.where(
                or_(
                    Service.name.ilike(search_term),         # 'loai'
                    Service.thuonghieu.ilike(search_term), # 'thuonghieu'
                    Service.description.ilike(search_term) # 'loaimay'
                )
            )

        query = query.order_by(Service.created_at.desc()).offset(skip).limit(limit)
        
        result = await db.execute(query)
        
        # 4. Gán 'product_count' vào 'service' object
        services_with_count = []
        for service, count in result.all():
            service.product_count = count
            services_with_count.append(service)
        
        return services_with_count

    @staticmethod
    async def get_deleted_today_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> List[Service]:
        today = date.today()
        query = select(Service).where(
            and_(
                Service.user_id == user_id,
                Service.trashed_at.isnot(None),
                func.date(Service.trashed_at) == today
            )
        )
        result = await db.execute(query)
        return result.scalars().all()