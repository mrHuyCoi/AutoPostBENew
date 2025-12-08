from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
import uuid
from sqlalchemy.orm import selectinload

from app.models.category import Category
from app.dto.category_dto import CategoryCreate, CategoryUpdate


class CategoryRepository:
    """Repository xử lý các thao tác CRUD cho đối tượng Category."""
    
    @staticmethod
    async def create(db: AsyncSession, data: CategoryCreate) -> Category:
        """Tạo một danh mục mới."""
        # Tạo đối tượng Category
        db_category = Category(
            name=data.name,
            parent_id=data.parent_id
        )
        
        # Lưu vào database
        db.add(db_category)
        await db.commit()
        await db.refresh(db_category)
        
        return db_category
    
    @staticmethod
    async def get_by_id(db: AsyncSession, category_id: uuid.UUID) -> Optional[Category]:
        """
        Lấy thông tin danh mục bằng ID.
        
        Args:
            db: Database session
            category_id: ID của danh mục
            
        Returns:
            Đối tượng Category hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(Category)
            .where(Category.id == category_id)
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_by_name(db: AsyncSession, name: str) -> Optional[Category]:
        """
        Lấy thông tin danh mục bằng tên.
        
        Args:
            db: Database session
            name: Tên danh mục
            
        Returns:
            Đối tượng Category hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(Category)
            .where(Category.name == name)
        )
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, category_id: uuid.UUID, data: CategoryUpdate) -> Optional[Category]:
        """Cập nhật thông tin danh mục."""
        db_category = await CategoryRepository.get_by_id(db, category_id)
        
        if not db_category:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_category, field, value)
        
        # Lưu thay đổi
        await db.commit()
        await db.refresh(db_category)
        
        return db_category
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[Category]:
        """
        Lấy danh sách danh mục với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa
            
        Returns:
            Danh sách các đối tượng Category
        """
        result = await db.execute(select(Category).offset(skip).limit(limit))
        return result.scalars().all()
    
    @staticmethod
    async def delete(db: AsyncSession, category_id: uuid.UUID) -> bool:
        """
        Xóa danh mục.
        
        Args:
            db: Database session
            category_id: ID của danh mục
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        db_category = await CategoryRepository.get_by_id(db, category_id)
        
        if not db_category:
            return False
        
        await db.delete(db_category)
        await db.commit()
        
        return True
