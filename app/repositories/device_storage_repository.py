from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.orm import selectinload
from typing import List, Optional
import uuid
from sqlalchemy import or_

from app.models.device_storage import DeviceStorage
from app.dto.device_storage_dto import DeviceStorageCreate, DeviceStorageUpdate


class DeviceStorageRepository:
    """
    Repository xử lý các thao tác với bảng device_storage trong database.
    """
    
    @staticmethod
    async def create(db: AsyncSession, data: dict) -> DeviceStorage:
        """
        Tạo một bản ghi dung lượng thiết bị mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo dung lượng thiết bị
            
        Returns:
            Đối tượng DeviceStorage đã tạo
        """
        device_storage = DeviceStorage(**data.dict())
        db.add(device_storage)
        await db.commit()
        await db.refresh(device_storage)
        return device_storage
    
    @staticmethod
    async def get_by_id(db: AsyncSession, device_storage_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> Optional[DeviceStorage]:
        """
        Lấy thông tin dung lượng thiết bị bằng ID.
        
        Args:
            db: Database session
            device_storage_id: ID của dung lượng thiết bị
            user_id: ID của người dùng (nếu None thì lấy tất cả)
            
        Returns:
            Đối tượng DeviceStorage hoặc None nếu không tìm thấy
        """
        if user_id is not None:
            # Lấy dung lượng theo user_id cụ thể hoặc user_id = null
            query = select(DeviceStorage).where(
                DeviceStorage.id == device_storage_id
            ).where(or_(DeviceStorage.user_id == user_id, DeviceStorage.user_id.is_(None)))
        else:
            # Lấy tất cả dung lượng (không filter theo user_id)
            query = select(DeviceStorage).where(DeviceStorage.id == device_storage_id)
        
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def get_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceStorage]:
        """
        Lấy danh sách dung lượng của một thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            user_id: ID của người dùng (nếu None thì lấy tất cả)
            
        Returns:
            Danh sách các đối tượng DeviceStorage
        """
        if user_id is not None:
            # Lấy dung lượng theo user_id cụ thể hoặc user_id = null
            query = select(DeviceStorage).where(
                DeviceStorage.device_info_id == device_info_id
            ).where(or_(DeviceStorage.user_id == user_id, DeviceStorage.user_id.is_(None)))
        else:
            # Lấy tất cả dung lượng (không filter theo user_id)
            query = select(DeviceStorage).where(DeviceStorage.device_info_id == device_info_id)
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_by_device_info_id_and_capacity(db: AsyncSession, device_info_id: uuid.UUID, capacity: int) -> Optional[DeviceStorage]:
        """
        Lấy thông tin dung lượng thiết bị bằng ID thiết bị và dung lượng.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            capacity: Dung lượng (GB)
            
        Returns:
            Đối tượng DeviceStorage hoặc None nếu không tìm thấy
        """
        query = select(DeviceStorage).where(
            DeviceStorage.device_info_id == device_info_id,
            DeviceStorage.capacity == capacity
        )
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, device_storage_id: uuid.UUID, data: DeviceStorageUpdate) -> Optional[DeviceStorage]:
        """
        Cập nhật thông tin dung lượng thiết bị.
        
        Args:
            db: Database session
            device_storage_id: ID của dung lượng thiết bị
            data: Dữ liệu cập nhật
            
        Returns:
            Đối tượng DeviceStorage đã cập nhật hoặc None nếu không tìm thấy
        """
        # Tạo dict chứa các trường cần cập nhật
        update_data = {}
        if data.capacity is not None:
            update_data["capacity"] = data.capacity
        
        # Nếu không có trường nào cần cập nhật
        if not update_data:
            # Lấy đối tượng hiện tại và trả về
            return await DeviceStorageRepository.get_by_id(db, device_storage_id)
        
        # Cập nhật
        query = update(DeviceStorage).where(DeviceStorage.id == device_storage_id).values(**update_data)
        await db.execute(query)
        await db.commit()
        
        # Lấy đối tượng đã cập nhật
        return await DeviceStorageRepository.get_by_id(db, device_storage_id)
    
    @staticmethod
    async def delete(db: AsyncSession, device_storage_id: uuid.UUID) -> bool:
        """
        Xóa một dung lượng thiết bị.
        
        Args:
            db: Database session
            device_storage_id: ID của dung lượng thiết bị
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        query = delete(DeviceStorage).where(DeviceStorage.id == device_storage_id)
        result = await db.execute(query)
        await db.commit()
        return result.rowcount > 0
    
    @staticmethod
    async def delete_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID) -> bool:
        """
        Xóa tất cả dung lượng của một thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        query = delete(DeviceStorage).where(DeviceStorage.device_info_id == device_info_id)
        result = await db.execute(query)
        await db.commit()
        return result.rowcount > 0

    @staticmethod
    async def delete_by_device_info_ids(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> int:
        """
        Xóa nhiều DeviceStorage dựa trên danh sách device_info_id.
        """
        if not device_info_ids:
            return 0
        
        stmt = delete(DeviceStorage).where(DeviceStorage.device_info_id.in_(device_info_ids))
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[DeviceStorage]:
        """
        Lấy danh sách tất cả dung lượng thiết bị với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            
        Returns:
            Danh sách các đối tượng DeviceStorage
        """
        query = select(DeviceStorage).offset(skip).limit(limit)
        result = await db.execute(query)
        return result.scalars().all()