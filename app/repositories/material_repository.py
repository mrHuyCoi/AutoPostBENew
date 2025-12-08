from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload
from typing import List, Optional
import uuid

from app.models.material import Material
from app.dto.material_dto import MaterialCreate, MaterialUpdate


class MaterialRepository:
    """
    Repository cho thao tác với bảng materials
    """
    
    @staticmethod
    async def get_by_id(db: AsyncSession, material_id: uuid.UUID) -> Optional[Material]:
        """Lấy vật liệu theo ID"""
        result = await db.execute(
            select(Material).where(Material.id == material_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_by_name(db: AsyncSession, name: str) -> Optional[Material]:
        """Lấy vật liệu theo tên"""
        result = await db.execute(
            select(Material).where(Material.name == name)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_all(
        db: AsyncSession, 
        skip: int = 0, 
        limit: int = 100,
        user_id: Optional[uuid.UUID] = None
    ) -> List[Material]:
        """Lấy tất cả vật liệu với phân trang"""
        query = select(Material)
        
        if user_id:
            query = query.where(Material.user_id == user_id)
            
        query = query.offset(skip).limit(limit).order_by(Material.created_at.desc())
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def create(db: AsyncSession, material_data: MaterialCreate) -> Material:
        """Tạo vật liệu mới"""
        db_material = Material(
            name=material_data.name,
            description=material_data.description,
            user_id=material_data.user_id
        )
        
        db.add(db_material)
        await db.commit()
        await db.refresh(db_material)
        return db_material
    
    @staticmethod
    async def update(
        db: AsyncSession, 
        material_id: uuid.UUID, 
        material_data: MaterialUpdate
    ) -> Optional[Material]:
        """Cập nhật thông tin vật liệu"""
        # Lấy vật liệu hiện tại
        db_material = await MaterialRepository.get_by_id(db, material_id)
        if not db_material:
            return None
        
        # Chỉ cập nhật các trường có giá trị
        update_data = material_data.dict(exclude_unset=True)
        
        if update_data:
            await db.execute(
                update(Material)
                .where(Material.id == material_id)
                .values(**update_data)
            )
            await db.commit()
            await db.refresh(db_material)
        
        return db_material
    
    @staticmethod
    async def delete(db: AsyncSession, material_id: uuid.UUID) -> bool:
        """Xóa vật liệu"""
        db_material = await MaterialRepository.get_by_id(db, material_id)
        if not db_material:
            return False
        
        await db.execute(
            delete(Material).where(Material.id == material_id)
        )
        await db.commit()
        return True
    
    @staticmethod
    async def search_by_name(
        db: AsyncSession, 
        name: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[Material]:
        """Tìm kiếm vật liệu theo tên (case-insensitive)"""
        result = await db.execute(
            select(Material)
            .where(Material.name.ilike(f"%{name}%"))
            .offset(skip)
            .limit(limit)
            .order_by(Material.created_at.desc())
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_materials_by_ids(
        db: AsyncSession, 
        material_ids: List[uuid.UUID]
    ) -> List[Material]:
        """Lấy nhiều vật liệu theo danh sách IDs"""
        if not material_ids:
            return []
            
        result = await db.execute(
            select(Material).where(Material.id.in_(material_ids))
        )
        return result.scalars().all()