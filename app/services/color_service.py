from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
from fastapi import HTTPException, status
from app.models.color import Color
from app.dto.color_dto import ColorCreate, ColorUpdate, ColorRead   
from app.repositories.color_repository import ColorRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException
from app.models.device_info import DeviceInfo



class ColorService:
    """
    Service xử lý các thao tác liên quan đến màu sắc.
    """
    
    @staticmethod
    async def create_color(db: AsyncSession, data: ColorCreate, current_user) -> Color:
        """
        Tạo một màu sắc mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo màu sắc
            current_user: Người dùng hiện tại
            
        Returns:
            Đối tượng Color đã tạo
        """
        user_id = None if current_user.is_admin else current_user.id
        # Kiểm tra xem tên màu đã tồn tại chưa (trong phạm vi user hoặc màu sắc mặc định)
        existing_color = await ColorRepository.get_by_name(db, data.name, user_id)
        if existing_color:
            raise BadRequestException(f"Màu '{data.name}' đã tồn tại")
        color_data_dict = data.dict()
        return await ColorRepository.create(db, color_data_dict, user_id)
    
    @staticmethod
    async def get_color(db: AsyncSession, color_id: uuid.UUID, current_user) -> Color:
        """
        Lấy thông tin màu sắc bằng ID.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            current_user: Người dùng hiện tại
            
        Returns:
            Đối tượng Color
        """
        user_id = None if current_user.is_admin else current_user.id
        color = await ColorRepository.get_by_id(db, color_id, user_id)
        if not color:
            raise NotFoundException("Không tìm thấy màu sắc")
        return color
        
    @staticmethod
    async def get_devices_by_color_id(db: AsyncSession, color_id: uuid.UUID, current_user) -> List[DeviceInfo]:
        """
        Lấy danh sách thiết bị theo ID của màu sắc.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            current_user: Người dùng hiện tại
            
        Returns:
            List[DeviceInfo]: Danh sách các đối tượng DeviceInfo
            
        Raises:
            NotFoundException: Nếu không tìm thấy màu sắc
        """
        user_id = None if current_user.is_admin else current_user.id
        # Kiểm tra màu sắc tồn tại
        color = await ColorRepository.get_by_id(db, color_id, user_id)
        if not color:
            raise NotFoundException(f"Không tìm thấy màu sắc với ID: {color_id}")
            
        return await ColorRepository.get_devices_by_color_id(db, color_id, user_id)
    
    @staticmethod
    async def update_color(db: AsyncSession, color_id: uuid.UUID, data: ColorUpdate, current_user) -> Color:
        """
        Cập nhật thông tin màu sắc.
        
        Args:
            db: Database session
            color_id: ID của màu sắc
            data: Dữ liệu cập nhật
            current_user: Người dùng hiện tại
            
        Returns:
            Đối tượng Color đã cập nhật
        """
        user_id = None if current_user.is_admin else current_user.id
        # Kiểm tra xem màu sắc có tồn tại không
        color = await ColorRepository.get_by_id(db, color_id, user_id)
        if not color:
            raise NotFoundException("Không tìm thấy màu sắc")
        
        # Kiểm tra xem tên màu mới đã tồn tại chưa (nếu có thay đổi tên)
        if data.name is not None and data.name != color.name:
            existing_color = await ColorRepository.get_by_name(db, data.name, user_id)
            if existing_color:
                raise BadRequestException(f"Màu '{data.name}' đã tồn tại")
        
        updated_color = await ColorRepository.update(db, color_id, data, user_id)
        if not updated_color:
            raise BadRequestException("Không có quyền cập nhật màu sắc này")
        
        return updated_color
    
    @staticmethod
    async def delete_color(db: AsyncSession, color_id: uuid.UUID, current_user) -> bool:
        """
        Xóa màu sắc.
        """
        from app.repositories.color_repository import ColorRepository
        color = await ColorRepository.get_by_id(db, color_id, None)
        if not color:
            return False
        if not current_user.is_admin and getattr(color, 'user_id', None) != current_user.id:
            return False
        return await ColorRepository.delete(db, color_id)
    
    @staticmethod
    async def get_all_colors(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, current_user = None) -> List[Color]:
        """
        Lấy danh sách màu sắc với phân trang và tìm kiếm.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm (tùy chọn)
            current_user: Người dùng hiện tại
            
        Returns:
            Danh sách các đối tượng Color
        """
        user_id = None if current_user and current_user.is_admin else (current_user.id if current_user else None)
        return await ColorRepository.get_all(db, skip, limit, search, user_id)
        
    @staticmethod
    async def count_colors(db: AsyncSession, search: Optional[str] = None, current_user = None) -> int:
        """
        Đếm tổng số màu sắc phù hợp với điều kiện tìm kiếm.
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm (tùy chọn)
            current_user: Người dùng hiện tại
            
        Returns:
            Tổng số màu sắc
        """
        user_id = None if current_user and current_user.is_admin else (current_user.id if current_user else None)
        count = await ColorRepository.count_all(db, search, user_id)
        return count or 0