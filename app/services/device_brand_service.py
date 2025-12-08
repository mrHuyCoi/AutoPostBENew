from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid

from app.models.device_brand import DeviceBrand
from app.dto.device_brand_dto import DeviceBrandCreate, DeviceBrandUpdate
from app.repositories.device_brand_repository import DeviceBrandRepository

class DeviceBrandService:
    @staticmethod
    async def create_device_brand(db: AsyncSession, data: DeviceBrandCreate) -> DeviceBrand:
        # Check if device brand already exists for this user
        existing = await DeviceBrandRepository.get_by_name_and_user(db, data.name, data.user_id)
        if existing:
            return existing
            
        # Auto-format warranty field
        if data.warranty and data.warranty.strip().isdigit():
            data.warranty = f"{data.warranty.strip()} tháng"

        device_brand = DeviceBrand(
            name=data.name,
            warranty=data.warranty,
            user_id=data.user_id
        )
        return await DeviceBrandRepository.create(db, device_brand)

    @staticmethod
    async def get_device_brand(db: AsyncSession, device_brand_id: uuid.UUID) -> DeviceBrand:
        device_brand = await DeviceBrandRepository.get_by_id(db, device_brand_id)
        if not device_brand:
            raise Exception("Không tìm thấy hãng điện thoại")
        return device_brand

    @staticmethod
    async def get_all_device_brands(db: AsyncSession, user_id: Optional[uuid.UUID], skip: int = 0, limit: int = 100, search: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = 'asc') -> List[DeviceBrand]:
        return await DeviceBrandRepository.get_all_for_user(db, user_id, skip, limit, search, sort_by, sort_order)

    @staticmethod
    async def get_distinct_device_brands(db: AsyncSession, user_id: Optional[uuid.UUID], search: Optional[str] = None) -> List[DeviceBrand]:
        return await DeviceBrandRepository.get_distinct_brands_for_user(db, user_id, search)

    @staticmethod
    async def update_device_brand(db: AsyncSession, device_brand_id: uuid.UUID, data: DeviceBrandUpdate) -> DeviceBrand:
        device_brand = await DeviceBrandRepository.get_by_id(db, device_brand_id)
        if not device_brand:
            raise Exception("Không tìm thấy hãng điện thoại để cập nhật")

        # Auto-format warranty field
        if data.warranty and data.warranty.strip().isdigit():
            data.warranty = f"{data.warranty.strip()} tháng"

        # Update fields from data
        for var, value in vars(data).items():
            if value is not None:
                setattr(device_brand, var, value)
        
        return await DeviceBrandRepository.update(db, device_brand)

    @staticmethod
    async def delete_device_brand(db: AsyncSession, device_brand_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        device_brand = await DeviceBrandRepository.get_by_id(db, device_brand_id)
        if not device_brand:
            raise Exception("Không tìm thấy hãng điện thoại để xóa")
        
        # If user_id is provided, check if the device brand belongs to the user
        # If user_id is None (admin), allow deletion of any brand
        if user_id is not None and device_brand.user_id != user_id:
            raise Exception("Bạn không có quyền xóa hãng điện thoại này")
            
        await DeviceBrandRepository.delete(db, device_brand)
        return True
