from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
import uuid

from app.models.color import Color
from app.dto.color_dto import ColorCreate, ColorUpdate
from app.models.device_info import DeviceInfo
from app.models.device_color import DeviceColor


class ColorRepository:
    """
    Repository xử lý các thao tác CRUD cho đối tượng Color.
    """
    
    @staticmethod
    async def create(db: AsyncSession, data: dict, user_id: Optional[uuid.UUID] = None) -> Color:
        """
        Tạo một màu sắc mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo màu sắc
            user_id: ID của người dùng tạo màu sắc (tùy chọn)
            
        Returns:
            Đối tượng Color đã tạo
        """
        db_color = Color(**data)
        if user_id:
            db_color.user_id = user_id
        
        db.add(db_color)
        await db.commit()
        await db.refresh(db_color)
        
        return db_color
    
    @staticmethod
    async def get_by_id(db: AsyncSession, color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> Optional[Color]:
        """
        Lấy thông tin màu sắc bằng ID.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            Đối tượng Color hoặc None nếu không tìm thấy
        """
        query = select(Color).where(Color.id == color_id)
        
        # Nếu có user_id, chỉ lấy màu sắc của user đó hoặc màu sắc mặc định (user_id = null)
        if user_id:
            query = query.where((Color.user_id == user_id) | (Color.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().first()
        
    @staticmethod
    async def get_devices_by_color_id(db: AsyncSession, color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceInfo]:
        """
        Lấy danh sách thiết bị theo ID của màu sắc.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceInfo]: Danh sách các đối tượng DeviceInfo
        """
        query = select(DeviceInfo).join(
            DeviceColor, DeviceInfo.id == DeviceColor.device_info_id
        ).where(DeviceColor.color_id == color_id)
        
        # Nếu có user_id, chỉ lấy thiết bị của user đó hoặc thiết bị mặc định
        if user_id:
            query = query.where((DeviceInfo.user_id == user_id) | (DeviceInfo.user_id.is_(None)))
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_device_color_link(db: AsyncSession, device_info_id: uuid.UUID, color_id: uuid.UUID) -> Optional[DeviceColor]:
        """
        Kiểm tra xem một thiết bị đã được liên kết với một màu sắc hay chưa.
        """
        result = await db.execute(
            select(DeviceColor).where(
                DeviceColor.device_info_id == device_info_id,
                DeviceColor.color_id == color_id
            )
        )
        return result.scalars().first()

    @staticmethod
    async def create_device_color_link(db: AsyncSession, data: dict) -> DeviceColor:
        """
        Tạo một liên kết mới giữa thiết bị và màu sắc.
        """
        db_device_color = DeviceColor(**data)
        db.add(db_device_color)
        await db.commit()
        await db.refresh(db_device_color)
        return db_device_color

    @staticmethod
    async def get_by_name(db: AsyncSession, name: str, user_id: Optional[uuid.UUID] = None) -> Optional[Color]:
        """
        Lấy thông tin màu sắc bằng tên.
        
        Args:
            db: Database session
            name: Tên màu sắc
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            Đối tượng Color hoặc None nếu không tìm thấy
        """
        query = select(Color).where(Color.name == name)
        
        # Nếu có user_id, chỉ lấy màu sắc của user đó hoặc màu sắc mặc định
        if user_id:
            query = query.where((Color.user_id == user_id) | (Color.user_id.is_(None)))
        else:
            # Lấy tất cả các màu nếu không có user_id
            pass
        
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, color_id: uuid.UUID, data: ColorUpdate, user_id: Optional[uuid.UUID] = None) -> Optional[Color]:
        """
        Cập nhật thông tin màu sắc.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            data: Dữ liệu cập nhật
            user_id: ID của người dùng (tùy chọn, để kiểm tra quyền)
            
        Returns:
            Đối tượng Color đã cập nhật hoặc None nếu không tìm thấy
        """
        db_color = await ColorRepository.get_by_id(db, color_id, user_id)
        
        if not db_color:
            return None
        
        # Kiểm tra quyền: chỉ cho phép cập nhật màu sắc của chính mình hoặc màu sắc mặc định
        if user_id and db_color.user_id and db_color.user_id != user_id:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_color, key, value)
        
        # Lưu thay đổi
        await db.commit()
        await db.refresh(db_color)
        
        return db_color
    
    @staticmethod
    async def delete(db: AsyncSession, color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        """
        Xóa màu sắc.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            user_id: ID của người dùng (tùy chọn, để kiểm tra quyền)
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy hoặc không có quyền
        """
        db_color = await ColorRepository.get_by_id(db, color_id, user_id)
        
        if not db_color:
            return False
        
        # Kiểm tra quyền: chỉ cho phép xóa màu sắc của chính mình
        if user_id and db_color.user_id and db_color.user_id != user_id:
            return False
        
        await db.delete(db_color)
        await db.commit()
        
        return True
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[Color]:
        """
        Lấy danh sách màu sắc với phân trang và tìm kiếm.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            Danh sách các đối tượng Color
        """
        query = select(Color)
        
        # Nếu có user_id, chỉ lấy màu sắc của user đó hoặc màu sắc mặc định
        if user_id:
            query = query.where((Color.user_id == user_id) | (Color.user_id.is_(None)))
        
        # Thêm điều kiện tìm kiếm nếu có
        if search:
            search_term = f"%{search}%"
            query = query.where(Color.name.ilike(search_term))
        
        # Thêm phân trang - sắp xếp theo thời gian cập nhật mới nhất
        query = query.order_by(Color.updated_at.desc()).offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()
        
    @staticmethod
    async def count_all(db: AsyncSession, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> int:
        """
        Đếm tổng số màu sắc phù hợp với điều kiện tìm kiếm.
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            Tổng số màu sắc
        """
        from sqlalchemy import func
        
        query = select(func.count()).select_from(Color)
        
        # Nếu có user_id, chỉ đếm màu sắc của user đó hoặc màu sắc mặc định
        if user_id:
            query = query.where((Color.user_id == user_id) | (Color.user_id.is_(None)))
        
        # Thêm điều kiện tìm kiếm nếu có
        if search:
            search_term = f"%{search}%"
            query = query.where(Color.name.ilike(search_term))
        
        result = await db.execute(query)
        return result.scalar_one()