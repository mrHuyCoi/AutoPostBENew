from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
import uuid

from app.models.device_color import DeviceColor
from app.models.color import Color
from app.models.device_info import DeviceInfo
from app.dto.device_color_dto import DeviceColorCreate


class DeviceColorRepository:
    """
    Repository xử lý các thao tác CRUD cho đối tượng DeviceColor.
    """
    
    @staticmethod
    async def create(db: AsyncSession, data: DeviceColorCreate, user_id: Optional[uuid.UUID] = None) -> DeviceColor:
        """
        Tạo một liên kết giữa thiết bị và màu sắc mới.
        
        Args:
            db: Database session
            data: Dữ liệu để tạo liên kết
            user_id: ID của người dùng tạo liên kết (tùy chọn)
            
        Returns:
            DeviceColor: Đối tượng DeviceColor đã được tạo
        """
        device_color = DeviceColor(
            device_info_id=data.device_info_id,
            color_id=data.color_id,
            user_id=user_id
        )
        db.add(device_color)
        await db.commit()
        await db.refresh(device_color)
        return device_color
    
    @staticmethod
    async def get_by_id(db: AsyncSession, device_color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> Optional[DeviceColor]:
        """
        Lấy thông tin liên kết giữa thiết bị và màu sắc theo ID.
        
        Args:
            db: Database session
            device_color_id: ID của liên kết cần lấy
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            Optional[DeviceColor]: Đối tượng DeviceColor nếu tìm thấy, None nếu không tìm thấy
        """
        query = select(DeviceColor).where(DeviceColor.id == device_color_id)
        
        # Nếu có user_id, chỉ lấy liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def get_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần lấy các liên kết
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor
        """
        query = select(DeviceColor).where(DeviceColor.device_info_id == device_info_id)
        
        # Nếu có user_id, chỉ lấy liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_by_device_info_id_with_color(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị kèm thông tin màu sắc.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần lấy các liên kết
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor kèm thông tin màu sắc
        """
        query = select(DeviceColor).options(joinedload(DeviceColor.color)).where(DeviceColor.device_info_id == device_info_id)
        
        # Nếu có user_id, chỉ lấy liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def delete(db: AsyncSession, device_color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        """
        Xóa một liên kết giữa thiết bị và màu sắc.
        
        Args:
            db: Database session
            device_color_id: ID của liên kết cần xóa
            user_id: ID của người dùng (tùy chọn, để kiểm tra quyền)
            
        Returns:
            bool: True nếu xóa thành công, False nếu không tìm thấy hoặc không có quyền
        """
        device_color = await DeviceColorRepository.get_by_id(db, device_color_id, user_id)
        if not device_color:
            return False
        
        # Kiểm tra quyền: chỉ cho phép xóa liên kết của chính mình
        if user_id and device_color.user_id and device_color.user_id != user_id:
            return False
        
        await db.delete(device_color)
        await db.commit()
        return True
    
    @staticmethod
    async def delete_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        """
        Xóa tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần xóa các liên kết
            user_id: ID của người dùng (tùy chọn, để kiểm tra quyền)
            
        Returns:
            bool: True nếu xóa thành công, False nếu không tìm thấy
        """
        device_colors = await DeviceColorRepository.get_by_device_info_id(db, device_info_id, user_id)
        if not device_colors:
            return False
        
        # Nếu có user_id, chỉ xóa liên kết của user đó
        if user_id:
            device_colors = [dc for dc in device_colors if not dc.user_id or dc.user_id == user_id]
        
        for device_color in device_colors:
            await db.delete(device_color)
        await db.commit()
        return True
    
    @staticmethod
    async def delete_by_device_info_ids(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> int:
        """
        Xóa nhiều DeviceColor dựa trên danh sách device_info_id.
        """
        if not device_info_ids:
            return 0
        
        from sqlalchemy import delete
        stmt = delete(DeviceColor).where(DeviceColor.device_info_id.in_(device_info_ids))
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount
    
    @staticmethod
    async def get_colors_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[Color]:
        """
        Lấy tất cả các màu sắc của một thiết bị theo ID của thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần lấy các màu sắc
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[Color]: Danh sách các đối tượng Color
        """
        query = select(Color).join(DeviceColor).where(DeviceColor.device_info_id == device_info_id)
        
        # Nếu có user_id, chỉ lấy màu sắc từ liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_all_with_color(db: AsyncSession, skip: int = 0, limit: int = 10, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc với phân trang và tìm kiếm kèm thông tin màu sắc.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor kèm thông tin màu sắc
        """
        
        query = select(DeviceColor).options(joinedload(DeviceColor.color), joinedload(DeviceColor.device_info))
        
        # Nếu có user_id, chỉ lấy liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        # Thêm điều kiện tìm kiếm nếu có
        if search:
            search_term = f"%{search}%"
            query = query.join(Color).join(DeviceInfo).where(
                (Color.name.ilike(search_term)) |
                (DeviceInfo.model.ilike(search_term))
            )
        
        # Thêm phân trang
        query = query.order_by(DeviceColor.created_at.desc()).offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def count_all(db: AsyncSession, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> int:
        """
        Đếm tổng số liên kết giữa thiết bị và màu sắc phù hợp với điều kiện tìm kiếm.
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            int: Tổng số liên kết
        """
        from sqlalchemy import func
        
        if search:
            search_term = f"%{search}%"
            query = select(func.count()).select_from(DeviceColor).join(Color).join(DeviceInfo).where(
                (Color.name.ilike(search_term)) |
                (DeviceInfo.model.ilike(search_term))
            )
        else:
            query = select(func.count()).select_from(DeviceColor)
        
        # Nếu có user_id, chỉ đếm liên kết của user đó hoặc liên kết mặc định
        if user_id:
            query = query.where((DeviceColor.user_id == user_id) | (DeviceColor.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalar_one()