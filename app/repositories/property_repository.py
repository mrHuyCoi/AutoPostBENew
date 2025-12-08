from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
import uuid
from sqlalchemy.orm import selectinload

from app.models.property import Property
from app.dto.property_dto import PropertyCreate, PropertyUpdate


class PropertyRepository:
    """Repository xử lý các thao tác CRUD cho đối tượng Property."""
    
    @staticmethod
    async def create(db: AsyncSession, data: PropertyCreate) -> Property:
        """Tạo một thuộc tính mới."""
        # Tạo đối tượng Property
        db_property = Property(
            key=data.key,
            values=data.values,
            parent_id=data.parent_id
        )
        
        # Lưu vào database
        db.add(db_property)
        await db.commit()
        await db.refresh(db_property)
        
        return db_property
    
    @staticmethod
    async def get_by_id(db: AsyncSession, property_id: uuid.UUID) -> Optional[Property]:
        """
        Lấy thông tin thuộc tính bằng ID.
        
        Args:
            db: Database session
            property_id: ID của thuộc tính
            
        Returns:
            Đối tượng Property hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(Property)
            .where(Property.id == property_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_by_key(db: AsyncSession, key: str) -> Optional[Property]:
        """
        Lấy thông tin thuộc tính bằng khóa.
        
        Args:
            db: Database session
            key: Khóa của thuộc tính
            
        Returns:
            Đối tượng Property hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(Property)
            .where(Property.key == key)
        )
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, property_id: uuid.UUID, data: PropertyUpdate) -> Optional[Property]:
        """Cập nhật thông tin thuộc tính."""
        db_property = await PropertyRepository.get_by_id(db, property_id)
        
        if not db_property:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_property, field, value)
        
        # Lưu thay đổi
        await db.commit()
        await db.refresh(db_property)
        
        return db_property
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Property]:
        """
        Lấy danh sách thuộc tính với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            
        Returns:
            Danh sách các đối tượng Property
        """
        result = await db.execute(select(Property).offset(skip).limit(limit))
        return result.scalars().all()
    
    @staticmethod
    async def delete(db: AsyncSession, property_id: uuid.UUID) -> bool:
        """
        Xóa thuộc tính.
        
        Args:
            db: Database session
            property_id: ID của thuộc tính
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        db_property = await PropertyRepository.get_by_id(db, property_id)
        
        if not db_property:
            return False
        
        await db.delete(db_property)
        await db.commit()
        
        return True
