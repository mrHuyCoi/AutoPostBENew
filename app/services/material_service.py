from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import uuid

from app.repositories.material_repository import MaterialRepository
from app.dto.material_dto import MaterialCreate, MaterialUpdate, MaterialRead, MaterialInfo
from app.exceptions.base_exception import AppException


class MaterialService:
    """
    Service xử lý business logic cho vật liệu
    """
    
    @staticmethod
    async def create_material(
        db: AsyncSession, 
        material_data: MaterialCreate,
        current_user_id: Optional[uuid.UUID] = None
    ) -> MaterialRead:
        """Tạo vật liệu mới"""
        # Kiểm tra trùng tên
        existing_material = await MaterialRepository.get_by_name(db, material_data.name)
        if existing_material:
            raise AppException(
                status_code=400,
                message="Tên vật liệu đã tồn tại",
                error_code="MATERIAL_NAME_EXISTS"
            )
        
        # Nếu user_id không được cung cấp, sử dụng current_user_id
        if material_data.user_id is None:
            material_data.user_id = current_user_id
        
        db_material = await MaterialRepository.create(db, material_data)
        return MaterialRead.from_orm(db_material)
    
    @staticmethod
    async def get_material(db: AsyncSession, material_id: uuid.UUID) -> MaterialRead:
        """Lấy thông tin vật liệu theo ID"""
        db_material = await MaterialRepository.get_by_id(db, material_id)
        if not db_material:
            raise AppException(
                status_code=404,
                message="Vật liệu không tồn tại",
                error_code="MATERIAL_NOT_FOUND"
            )
        
        return MaterialRead.from_orm(db_material)
    
    @staticmethod
    async def get_all_materials(
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100,
        user_id: Optional[uuid.UUID] = None
    ) -> List[MaterialRead]:
        """Lấy danh sách tất cả vật liệu"""
        db_materials = await MaterialRepository.get_all(db, skip, limit, user_id)
        return [MaterialRead.from_orm(material) for material in db_materials]
    
    @staticmethod
    async def update_material(
        db: AsyncSession,
        material_id: uuid.UUID,
        material_data: MaterialUpdate
    ) -> MaterialRead:
        """Cập nhật thông tin vật liệu"""
        # Kiểm tra vật liệu tồn tại
        existing_material = await MaterialRepository.get_by_id(db, material_id)
        if not existing_material:
            raise AppException(
                status_code=404,
                message="Vật liệu không tồn tại",
                error_code="MATERIAL_NOT_FOUND"
            )
        
        # Kiểm tra trùng tên nếu có cập nhật tên
        if material_data.name and material_data.name != existing_material.name:
            duplicate_material = await MaterialRepository.get_by_name(db, material_data.name)
            if duplicate_material and duplicate_material.id != material_id:
                raise AppException(
                    status_code=400,
                    message="Tên vật liệu đã tồn tại",
                    error_code="MATERIAL_NAME_EXISTS"
                )
        
        updated_material = await MaterialRepository.update(db, material_id, material_data)
        if not updated_material:
            raise AppException(
                status_code=404,
                message="Vật liệu không tồn tại",
                error_code="MATERIAL_NOT_FOUND"
            )
        
        return MaterialRead.from_orm(updated_material)
    
    @staticmethod
    async def delete_material(db: AsyncSession, material_id: uuid.UUID) -> bool:
        """Xóa vật liệu"""
        # Kiểm tra vật liệu tồn tại
        existing_material = await MaterialRepository.get_by_id(db, material_id)
        if not existing_material:
            raise AppException(
                status_code=404,
                message="Vật liệu không tồn tại",
                error_code="MATERIAL_NOT_FOUND"
            )
        
        # TODO: Kiểm tra ràng buộc (nếu vật liệu đang được sử dụng trong device_infos)
        # if existing_material.device_infos:
        #     raise AppException(
        #         status_code=400,
        #         message="Không thể xóa vật liệu đang được sử dụng",
        #         error_code="MATERIAL_IN_USE"
        #     )
        
        return await MaterialRepository.delete(db, material_id)
    
    @staticmethod
    async def search_materials(
        db: AsyncSession,
        name: str,
        skip: int = 0,
        limit: int = 100
    ) -> List[MaterialRead]:
        """Tìm kiếm vật liệu theo tên"""
        db_materials = await MaterialRepository.search_by_name(db, name, skip, limit)
        return [MaterialRead.from_orm(material) for material in db_materials]
    
    @staticmethod
    async def get_materials_info_by_ids(
        db: AsyncSession,
        material_ids: List[uuid.UUID]
    ) -> List[MaterialInfo]:
        """Lấy thông tin đơn giản của vật liệu theo danh sách IDs"""
        db_materials = await MaterialRepository.get_materials_by_ids(db, material_ids)
        return [MaterialInfo.from_orm(material) for material in db_materials]