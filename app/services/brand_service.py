from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
from fastapi import BackgroundTasks

from app.models.brand import Brand
from app.models.user import User
from app.dto.brand_dto import BrandCreate, BrandUpdate, BrandRead
from app.repositories.brand_repository import BrandRepository
from app.services.chatbot_service import ChatbotService
from app.utils.soft_delete import SoftDeleteMixin


class BrandService:
    @staticmethod
    async def create_brand(db: AsyncSession, data: BrandCreate, background_tasks: BackgroundTasks, current_user: User) -> BrandRead:
        brand = Brand(
            name=data.name,
            warranty=data.warranty,
            note=data.note,
            service_id=data.service_id,
            device_brand_id=data.device_brand_id,
            device_type=data.device_type,
            color=data.color,
            price=data.price,
            wholesale_price=data.wholesale_price
        )
        created_brand = await BrandRepository.create(db, brand, current_user.id)
        # Eager load relationships before returning
        created_brand_with_details = await BrandRepository.get_by_id_with_details(db, created_brand.id)
        background_tasks.add_task(ChatbotService.add_service, created_brand.id, current_user)
        return BrandRead.model_validate(created_brand_with_details)

    @staticmethod
    async def get_brand(db: AsyncSession, brand_id: uuid.UUID) -> BrandRead:
        brand = await BrandRepository.get_by_id_with_details(db, brand_id)
        if not brand:
            raise Exception("Không tìm thấy thương hiệu")
        return BrandRead.model_validate(brand)

    @staticmethod
    async def update_brand(db: AsyncSession, brand_id: uuid.UUID, data: BrandUpdate, background_tasks: BackgroundTasks, current_user: User) -> BrandRead:
        brand = await BrandRepository.get_by_id(db, brand_id)
        if not brand:
            raise Exception("Không tìm thấy thương hiệu")
        
        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(brand, key, value)
        
        await db.commit()
        await db.refresh(brand)
        # Eager load relationships before returning
        updated_brand_with_details = await BrandRepository.get_by_id_with_details(db, brand.id)
        background_tasks.add_task(ChatbotService.update_service, brand.id, current_user)
        return BrandRead.model_validate(updated_brand_with_details)

    @staticmethod
    async def delete_brand(db: AsyncSession, brand_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: User) -> bool:
        brand = await BrandRepository.get_by_id_with_details(db, brand_id)
        if not brand:
            raise Exception("Không tìm thấy thương hiệu để xóa")

        service_code = brand.service_code
        # Thực hiện soft delete thay vì hard delete
        deleted = await SoftDeleteMixin.soft_delete(db, Brand, brand_id, days_to_purge=1)
        if deleted:
            background_tasks.add_task(ChatbotService.delete_service, service_code, current_user)
        return deleted
    
    @staticmethod
    async def restore_brand(db: AsyncSession, brand_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: User) -> bool:
        """
        Khôi phục thương hiệu đã bị xóa mềm.
        """
        restored = await SoftDeleteMixin.restore(db, Brand, brand_id)
        if restored:
            # Lấy thông tin brand để đồng bộ lại với chatbot
            brand = await BrandRepository.get_by_id_with_details(db, brand_id)
            if brand:
                background_tasks.add_task(ChatbotService.add_service, brand.id, current_user)
        return restored

    @staticmethod
    async def get_all_brands(db: AsyncSession, skip: int = 0, limit: int = 1000, search: Optional[str] = None, service_id: Optional[uuid.UUID] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = 'asc', user_id: Optional[uuid.UUID] = None) -> List[BrandRead]:
        brands = await BrandRepository.get_all(db, skip, limit, search, service_id, sort_by, sort_order, user_id=user_id)
        return [BrandRead.model_validate(b) for b in brands]

    @staticmethod
    async def count_brands(db: AsyncSession, search: Optional[str] = None, service_id: Optional[uuid.UUID] = None, user_id: Optional[uuid.UUID] = None) -> int:
        return await BrandRepository.count_all(db, search, service_id, user_id=user_id)

    @staticmethod
    async def get_deleted_brands_today(db: AsyncSession, user_id: uuid.UUID) -> List[BrandRead]:
        """
        Lấy danh sách các brands đã bị xóa mềm trong ngày hôm nay.
        """
        brands = await BrandRepository.get_deleted_today_by_user_id(db, user_id)
        return [BrandRead.model_validate(b) for b in brands]

    @staticmethod
    async def restore_all_deleted_brands_today(db: AsyncSession, user_id: uuid.UUID, background_tasks: BackgroundTasks, current_user: User) -> bool:
        """
        Khôi phục tất cả các brands đã bị xóa mềm trong ngày hôm nay.
        """
        # Lấy danh sách brands đã xóa trong ngày
        deleted_brands = await BrandRepository.get_deleted_today_by_user_id(db, user_id)
        
        if not deleted_brands:
            return True
        
        # Khôi phục từng brand
        restored_count = 0
        for brand in deleted_brands:
            restored = await SoftDeleteMixin.restore(db, Brand, brand.id)
            if restored:
                restored_count += 1
        
        # Đồng bộ với chatbot trong background
        if restored_count > 0:
            # Lấy lại danh sách brands đã restore để đồng bộ
            restored_brands = [b for b in deleted_brands if b.id]
            background_tasks.add_task(ChatbotService.add_all_services, restored_brands, current_user)
        
        return restored_count > 0

    @staticmethod
    async def get_unique_brand_names(db: AsyncSession, service_id: uuid.UUID) -> List[dict]:
        return await BrandRepository.get_unique_brand_names_with_warranty(db, service_id)