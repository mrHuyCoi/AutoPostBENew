from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import uuid

from app.repositories.device_color_repository import DeviceColorRepository
from app.repositories.device_info_repository import DeviceInfoRepository
from app.repositories.color_repository import ColorRepository
from app.dto.device_color_dto import DeviceColorCreate
from app.models.device_color import DeviceColor
from app.models.color import Color
from app.exceptions.not_found_exception import NotFoundException
from app.exceptions.permission_exception import PermissionException


class DeviceColorService:
    """
    Service xử lý logic nghiệp vụ cho đối tượng DeviceColor.
    """
    
    @staticmethod
    async def create_device_color(db: AsyncSession, data: DeviceColorCreate, current_user) -> DeviceColor:
        """
        Tạo một liên kết giữa thiết bị và màu sắc mới.
        
        Args:
            db: Database session
            data: Dữ liệu để tạo liên kết
            current_user: Người dùng hiện tại
            
        Returns:
            DeviceColor: Đối tượng DeviceColor đã được tạo
        
        Raises:
            NotFoundException: Nếu không tìm thấy thiết bị hoặc màu sắc
            PermissionException: Nếu người dùng không có quyền thêm màu cho thiết bị
        """
        user_id_to_check = None if current_user.is_admin else current_user.id

        # Admin có thể truy cập bất kỳ thiết bị nào, user thường chỉ truy cập thiết bị của họ
        device_info = await DeviceInfoRepository.get_by_id(db, data.device_info_id, user_id_to_check)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {data.device_info_id}")

        # Đối với user thường, đảm bảo họ chỉ có thể thêm vào thiết bị của chính họ
        if not current_user.is_admin and device_info.user_id != current_user.id:
            raise PermissionException("Bạn không có quyền thêm màu cho thiết bị này.")

        # Kiểm tra màu sắc tồn tại (của user hoặc mặc định)
        color = await ColorRepository.get_by_id(db, data.color_id, user_id_to_check)
        if not color:
            raise NotFoundException(f"Không tìm thấy màu sắc với ID: {data.color_id}")

        # Admin tạo device_color không có user_id, user thường thì có
        user_id_for_creation = None if current_user.is_admin else current_user.id
        return await DeviceColorRepository.create(db, data, user_id_for_creation)
    
    @staticmethod
    async def get_device_color(db: AsyncSession, device_color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> DeviceColor:
        """
        Lấy thông tin liên kết giữa thiết bị và màu sắc theo ID.
        
        Args:
            db: Database session
            device_color_id: ID của liên kết cần lấy
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            DeviceColor: Đối tượng DeviceColor
        
        Raises:
            NotFoundException: Nếu không tìm thấy liên kết
        """
        device_color = await DeviceColorRepository.get_by_id(db, device_color_id, user_id)
        if not device_color:
            raise NotFoundException(f"Không tìm thấy liên kết với ID: {device_color_id}")
        return device_color
    
    @staticmethod
    async def get_device_colors_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần lấy các liên kết
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor
        
        Raises:
            NotFoundException: Nếu không tìm thấy thiết bị
        """
        # Kiểm tra thiết bị tồn tại (chỉ thiết bị của user hoặc thiết bị mặc định)
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        return await DeviceColorRepository.get_by_device_info_id(db, device_info_id, user_id)
    
    @staticmethod
    async def get_device_colors_with_color_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị kèm thông tin màu sắc.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị cần lấy các liên kết
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor kèm thông tin màu sắc
        
        Raises:
            NotFoundException: Nếu không tìm thấy thiết bị
        """
        # Kiểm tra thiết bị tồn tại (chỉ thiết bị của user hoặc thiết bị mặc định)
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        return await DeviceColorRepository.get_by_device_info_id_with_color(db, device_info_id, user_id)
    
    @staticmethod
    async def delete_device_color(db: AsyncSession, device_color_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        """
        Xóa một liên kết giữa thiết bị và màu sắc.
        """
        from app.repositories.device_color_repository import DeviceColorRepository
        device_color = await DeviceColorRepository.get_by_id(db, device_color_id, user_id)
        if not device_color:
            return False
        # Nếu là user thường, chỉ xóa nếu user_id trùng
        if user_id and getattr(device_color, 'user_id', None) != user_id:
            return False
        return await DeviceColorRepository.delete(db, device_color_id, user_id)
    
    @staticmethod
    async def delete_device_colors_by_device_info_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> bool:
        """
        Xóa tất cả các liên kết giữa thiết bị và màu sắc theo ID của thiết bị.
        """
        from app.repositories.device_color_repository import DeviceColorRepository
        device_colors = await DeviceColorRepository.get_by_device_info_id(db, device_info_id, user_id)
        if not device_colors:
            return False
        # Nếu là user thường, chỉ xóa các liên kết của mình
        if user_id:
            device_colors = [dc for dc in device_colors if getattr(dc, 'user_id', None) == user_id]
        for device_color in device_colors:
            await DeviceColorRepository.delete(db, device_color.id, user_id)
        return True
    
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
        
        Raises:
            NotFoundException: Nếu không tìm thấy thiết bị
        """
        # Kiểm tra thiết bị tồn tại (chỉ thiết bị của user hoặc thiết bị mặc định)
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        return await DeviceColorRepository.get_colors_by_device_info_id(db, device_info_id, user_id)
    
    @staticmethod
    async def get_all_device_colors(db: AsyncSession, skip: int = 0, limit: int = 10, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[DeviceColor]:
        """
        Lấy tất cả các liên kết giữa thiết bị và màu sắc với phân trang và tìm kiếm.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            List[DeviceColor]: Danh sách các đối tượng DeviceColor kèm thông tin màu sắc
        """
        return await DeviceColorRepository.get_all_with_color(db, skip, limit, search, user_id)
    
    @staticmethod
    async def count_device_colors(db: AsyncSession, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> int:
        """
        Đếm tổng số liên kết giữa thiết bị và màu sắc phù hợp với điều kiện tìm kiếm.
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm (tùy chọn)
            user_id: ID của người dùng (tùy chọn, để lọc theo user)
            
        Returns:
            int: Tổng số liên kết
        """
        return await DeviceColorRepository.count_all(db, search, user_id)