from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid

from app.models.property import Property
from app.dto.property_dto import PropertyCreate, PropertyUpdate, PropertyRead
from app.repositories.property_repository import PropertyRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException


class PropertyService:
    """
    Service xử lý các thao tác liên quan đến thuộc tính.
    """
    
    @staticmethod
    async def create_property(db: AsyncSession, data: PropertyCreate) -> PropertyRead:
        """Tạo một thuộc tính mới."""
        # Tạo thuộc tính mới
        new_property = await PropertyRepository.create(db, data)
        
        # Chuyển đổi sang DTO để trả về
        return PropertyRead.model_validate(new_property)
    
    @staticmethod
    async def get_property(db: AsyncSession, property_id: uuid.UUID) -> PropertyRead:
        """Lấy thông tin thuộc tính theo ID."""
        property_obj = await PropertyRepository.get_by_id(db, property_id)
        if not property_obj:
            raise NotFoundException("Không tìm thấy thuộc tính")
        return PropertyRead.model_validate(property_obj)
    
    @staticmethod
    async def update_property(db: AsyncSession, property_id: uuid.UUID, data: PropertyUpdate) -> PropertyRead:
        """Cập nhật thông tin thuộc tính."""
        # Kiểm tra xem thuộc tính có tồn tại không
        db_property = await PropertyRepository.get_by_id(db, property_id)
        if not db_property:
            raise NotFoundException("Không tìm thấy thuộc tính")
        
        # Cập nhật thuộc tính
        updated_property = await PropertyRepository.update(db, property_id, data)
        if not updated_property:
            raise NotFoundException("Không tìm thấy thuộc tính")
        
        # Chuyển đổi sang DTO để trả về
        return PropertyRead.model_validate(updated_property)
    
    @staticmethod
    async def get_all_properties(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[PropertyRead]:
        """Lấy danh sách tất cả thuộc tính."""
        properties = await PropertyRepository.get_all(db, skip, limit)
        return [PropertyRead.model_validate(p) for p in properties]
    
    @staticmethod
    async def delete_property(db: AsyncSession, property_id: uuid.UUID) -> bool:
        """Xóa thuộc tính."""
        # Kiểm tra xem thuộc tính có tồn tại không
        property_obj = await PropertyRepository.get_by_id(db, property_id)
        if not property_obj:
            raise NotFoundException("Không tìm thấy thuộc tính")
        
        # Xóa thuộc tính
        return await PropertyRepository.delete(db, property_id)
