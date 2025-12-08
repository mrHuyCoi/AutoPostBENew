from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid

from app.models.category import Category
from app.dto.category_dto import CategoryCreate, CategoryUpdate, CategoryRead
from app.repositories.category_repository import CategoryRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException


class CategoryService:
    """
    Service xử lý các thao tác liên quan đến danh mục.
    """
    
    @staticmethod
    async def create_category(db: AsyncSession, data: CategoryCreate) -> CategoryRead:
        """Tạo một danh mục mới."""
        # Kiểm tra xem tên danh mục đã tồn tại chưa (nếu không có parent_id)
        if data.parent_id is None:
            existing_category = await CategoryRepository.get_by_name(db, data.name)
            if existing_category:
                raise BadRequestException("Tên danh mục đã tồn tại")
        
        # Tạo danh mục mới
        new_category = await CategoryRepository.create(db, data)
        
        # Chuyển đổi sang DTO để trả về
        return CategoryRead.model_validate(new_category)
    
    @staticmethod
    async def get_category(db: AsyncSession, category_id: uuid.UUID) -> CategoryRead:
        """Lấy thông tin danh mục theo ID."""
        category = await CategoryRepository.get_by_id(db, category_id)
        if not category:
            raise NotFoundException("Không tìm thấy danh mục")
        return CategoryRead.model_validate(category)
    
    @staticmethod
    async def update_category(db: AsyncSession, category_id: uuid.UUID, data: CategoryUpdate) -> CategoryRead:
        """Cập nhật thông tin danh mục."""
        # Kiểm tra xem danh mục có tồn tại không
        db_category = await CategoryRepository.get_by_id(db, category_id)
        if not db_category:
            raise NotFoundException("Không tìm thấy danh mục")
        
        # Nếu cập nhật tên danh mục, kiểm tra xem tên mới đã tồn tại chưa (nếu không có parent_id)
        if data.name is not None and data.name != db_category.name and data.parent_id is None:
            existing_category = await CategoryRepository.get_by_name(db, data.name)
            if existing_category:
                raise BadRequestException("Tên danh mục đã tồn tại")
        
        # Cập nhật danh mục
        updated_category = await CategoryRepository.update(db, category_id, data)
        if not updated_category:
            raise NotFoundException("Không tìm thấy danh mục")
        
        # Chuyển đổi sang DTO để trả về
        return CategoryRead.model_validate(updated_category)
    
    @staticmethod
    async def get_all_categories(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[CategoryRead]:
        """Lấy danh sách tất cả danh mục."""
        categories = await CategoryRepository.get_all(db, skip, limit)
        return [CategoryRead.model_validate(c) for c in categories]
    
    @staticmethod
    async def delete_category(db: AsyncSession, category_id: uuid.UUID) -> bool:
        """Xóa danh mục."""
        # Kiểm tra xem danh mục có tồn tại không
        category = await CategoryRepository.get_by_id(db, category_id)
        if not category:
            raise NotFoundException("Không tìm thấy danh mục")
        
        # Xóa danh mục
        return await CategoryRepository.delete(db, category_id)
