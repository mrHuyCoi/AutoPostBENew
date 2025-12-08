from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
from fastapi import BackgroundTasks

from app.models.user_device import UserDevice
from app.dto.user_device_dto import UserDeviceCreate, UserDeviceUpdate
from app.repositories.user_device_repository import UserDeviceRepository
from app.repositories.device_info_repository import DeviceInfoRepository
from app.repositories.color_repository import ColorRepository
from app.repositories.device_storage_repository import DeviceStorageRepository
from app.repositories.device_color_repository import DeviceColorRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException
from app.services.chatbot_service import ChatbotService
from app.models.user import User
from app.utils.soft_delete import SoftDeleteMixin


class UserDeviceService:
    """
    Service xử lý các thao tác liên quan đến thiết bị người dùng.
    """
    
    @staticmethod
    async def create_user_device(db: AsyncSession, data: UserDeviceCreate, current_user: User, background_tasks: BackgroundTasks) -> UserDevice:
        """
        Tạo một thiết bị người dùng mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo thiết bị người dùng
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            Đối tượng UserDevice đã tạo
        """
        # Kiểm tra màu sắc tồn tại (chỉ khi có color_id)
        if data.color_id:
            color = await ColorRepository.get_by_id(db, data.color_id)
            if not color:
                raise NotFoundException(f"Không tìm thấy màu sắc với ID: {data.color_id}")
                
            # Kiểm tra màu sắc có phù hợp với thiết bị không
            device_colors = await DeviceColorRepository.get_colors_by_device_info_id(db, data.device_info_id)
            if device_colors and data.color_id not in [color.id for color in device_colors]:
                raise ValueError(f"Màu sắc không phù hợp với thiết bị này. Vui lòng chọn một trong các màu sắc có sẵn của thiết bị.")
        
        # Kiểm tra dung lượng có phù hợp với thiết bị không (chỉ khi có device_storage_id)
        device_storage = None
        if data.device_storage_id:
            device_storage = await DeviceStorageRepository.get_by_id(db, data.device_storage_id)
            if not device_storage:
                raise NotFoundException(f"Không tìm thấy dung lượng với ID: {data.device_storage_id}")
            
            if device_storage.device_info_id != data.device_info_id:
                raise ValueError(f"Dung lượng không phù hợp với thiết bị này. Vui lòng chọn một trong các dung lượng có sẵn của thiết bị.")
        
        # Kiểm tra trùng lặp (chỉ khi có color_id)
        if data.color_id:
            duplicate_device = await UserDeviceRepository.find_duplicate(
                db=db,
                user_id=data.user_id,
                device_info_id=data.device_info_id,
                color_id=data.color_id,
                device_storage_id=data.device_storage_id,
                price=data.price,
                wholesale_price=data.wholesale_price,
                device_type=data.device_type,
                device_condition=data.device_condition,
                battery_condition=data.battery_condition,
                warranty=data.warranty
            )
        else:
            duplicate_device = None
        if duplicate_device:
            raise BadRequestException(f"Thiết bị đã tồn tại với mã sản phẩm: {duplicate_device.product_code}")
        
        # Tạo thiết bị người dùng mới
        user_device = await UserDeviceRepository.create(db, data)
        
        # Thêm sản phẩm vào chatbot trong background
        background_tasks.add_task(ChatbotService.add_product, user_device.id, current_user)
        
        # Không cần đồng bộ với ChatbotCustom vì user device không phải là product component
        # ChatbotCustom chỉ xử lý linh kiện (accessories), không phải thiết bị điện thoại
            
        return user_device
    
    @staticmethod
    async def get_user_device(db: AsyncSession, device_id: uuid.UUID, with_details: bool = False) -> UserDevice:
        """
        Lấy thông tin thiết bị người dùng bằng ID.
        
        Args:
            db: Database session
            device_id: ID của thiết bị người dùng
            with_details: Có lấy thông tin chi tiết không
            
        Returns:
            Đối tượng UserDevice
        """
        if with_details:
            user_device = await UserDeviceRepository.get_by_id_with_details(db, device_id)
        else:
            user_device = await UserDeviceRepository.get_by_id(db, device_id)
            
        if not user_device:
            raise NotFoundException("Không tìm thấy thiết bị người dùng")
        return user_device
    
    @staticmethod
    async def get_user_devices_by_user_id(
        db: AsyncSession, 
        user_id: uuid.UUID, 
        with_details: bool = False,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = 'asc',
        skip: int = 0,
        limit: Optional[int] = 100,
        filters: Optional[dict] = None
    ) -> List[UserDevice]:
        """
        Lấy danh sách thiết bị của một người dùng.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            with_details: Có lấy thông tin chi tiết không
            sort_by: Tên trường để sắp xếp
            sort_order: Thứ tự sắp xếp ('asc' hoặc 'desc')
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            filters: Bộ lọc
            
        Returns:
            Danh sách các đối tượng UserDevice
        """
        if with_details:
            return await UserDeviceRepository.get_by_user_id_with_details(
                db, user_id, skip=skip, limit=limit, sort_by=sort_by, sort_order=sort_order, filters=filters
            )
        else:
            return await UserDeviceRepository.get_by_user_id(db, user_id, skip=skip, limit=limit, filters=filters)
    
    @staticmethod
    async def count_user_devices(db: AsyncSession, user_id: uuid.UUID, filters: Optional[dict] = None) -> int:
        """
        Đếm số lượng thiết bị của một người dùng.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            filters: Bộ lọc
            
        Returns:
            Số lượng thiết bị của người dùng
        """
        return await UserDeviceRepository.count_by_user_id(db, user_id, filters=filters)
    
    @staticmethod
    async def update_user_device(db: AsyncSession, device_id: uuid.UUID, data: UserDeviceUpdate, current_user: User, background_tasks: BackgroundTasks) -> UserDevice:
        """
        Cập nhật thông tin thiết bị người dùng.
        
        Args:
            db: Database session
            device_id: ID của thiết bị người dùng
            data: Dữ liệu cập nhật
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            Đối tượng UserDevice đã cập nhật
        """
        # Kiểm tra xem thiết bị người dùng có tồn tại không
        user_device = await UserDeviceRepository.get_by_id(db, device_id)
        if not user_device:
            raise NotFoundException("Không tìm thấy thiết bị người dùng")
        
        # Nếu có cập nhật màu sắc và thiết bị
        if data.color_id and data.device_info_id:
            # Kiểm tra màu sắc tồn tại
            color = await ColorRepository.get_by_id(db, data.color_id)
            if not color:
                raise NotFoundException(f"Không tìm thấy màu sắc với ID: {data.color_id}")
                
            # Kiểm tra màu sắc có phù hợp với thiết bị không
            device_colors = await DeviceColorRepository.get_colors_by_device_info_id(db, data.device_info_id)
            if device_colors and data.color_id not in [color.id for color in device_colors]:
                raise ValueError(f"Màu sắc không phù hợp với thiết bị này. Vui lòng chọn một trong các màu sắc có sẵn của thiết bị.")
                
            # Nếu cũng cập nhật dung lượng
            if data.device_storage_id:
                # Kiểm tra dung lượng tồn tại
                device_storage = await DeviceStorageRepository.get_by_id(db, data.device_storage_id)
                if not device_storage:
                    raise NotFoundException(f"Không tìm thấy dung lượng với ID: {data.device_storage_id}")
                
                # Kiểm tra dung lượng có phù hợp với thiết bị mới không
                if device_storage.device_info_id != data.device_info_id:
                    raise ValueError(f"Dung lượng không phù hợp với thiết bị này. Vui lòng chọn một trong các dung lượng có sẵn của thiết bị.")
        # Nếu chỉ cập nhật màu sắc
        elif data.color_id:
            # Kiểm tra màu sắc tồn tại
            color = await ColorRepository.get_by_id(db, data.color_id)
            if not color:
                raise NotFoundException(f"Không tìm thấy màu sắc với ID: {data.color_id}")
                
            # Kiểm tra màu sắc có phù hợp với thiết bị hiện tại không
            device_colors = await DeviceColorRepository.get_colors_by_device_info_id(db, user_device.device_info_id)
            if device_colors and data.color_id not in [color.id for color in device_colors]:
                raise ValueError(f"Màu sắc không phù hợp với thiết bị này. Vui lòng chọn một trong các màu sắc có sẵn của thiết bị.")
        
        # Nếu chỉ cập nhật dung lượng
        if data.device_storage_id and not data.device_info_id:
            # Kiểm tra dung lượng tồn tại
            device_storage = await DeviceStorageRepository.get_by_id(db, data.device_storage_id)
            if not device_storage:
                raise NotFoundException(f"Không tìm thấy dung lượng với ID: {data.device_storage_id}")
            
            # Kiểm tra dung lượng có phù hợp với thiết bị hiện tại không
            if device_storage.device_info_id != user_device.device_info_id:
                raise ValueError(f"Dung lượng không phù hợp với thiết bị này. Vui lòng chọn một trong các dung lượng có sẵn của thiết bị.")
        
        # Kiểm tra trùng lặp trước khi cập nhật
        update_data_dict = data.dict(exclude_unset=True)
        
        check_data = {
            "device_info_id": data.device_info_id or user_device.device_info_id,
            "color_id": data.color_id or user_device.color_id,
            "device_storage_id": data.device_storage_id or user_device.device_storage_id,
            "price": data.price if data.price is not None else user_device.price,
            "wholesale_price": data.wholesale_price if data.wholesale_price is not None else user_device.wholesale_price,
            "device_type": data.device_type or user_device.device_type,
            "device_condition": data.device_condition or user_device.device_condition,
            "battery_condition": data.battery_condition or user_device.battery_condition,
            "warranty": data.warranty or user_device.warranty,
        }

        duplicate_device = await UserDeviceRepository.find_duplicate(
            db=db,
            user_id=user_device.user_id,
            **check_data,
            exclude_id=device_id
        )
        if duplicate_device:
            raise BadRequestException(f"Thiết bị sau khi cập nhật bị trùng với mã sản phẩm: {duplicate_device.product_code}")
        
        updated_user_device = await UserDeviceRepository.update(db, device_id, data)
        
        # Cập nhật sản phẩm trong chatbot trong background
        background_tasks.add_task(ChatbotService.update_product, updated_user_device.id, current_user)
        
        return updated_user_device
    
    @staticmethod
    async def delete_user_device(db: AsyncSession, device_id: uuid.UUID, current_user: User, background_tasks: BackgroundTasks) -> bool:
        """
        Xóa mềm thiết bị người dùng.
        
        Args:
            db: Database session
            device_id: ID của thiết bị người dùng
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            True nếu xóa thành công
        """
        user_device = await UserDeviceRepository.get_by_id(db, device_id)
        if not user_device:
            raise NotFoundException("Không tìm thấy thiết bị người dùng")
        
        product_code = user_device.product_code
        
        # Thực hiện soft delete thay vì hard delete
        success = await SoftDeleteMixin.soft_delete(db, UserDevice, device_id, days_to_purge=1)
        if not success:
            raise BadRequestException("Không thể xóa thiết bị người dùng")
            
        # Xóa sản phẩm khỏi chatbot trong background
        if product_code:
            background_tasks.add_task(ChatbotService.delete_product, product_code, current_user)
        
        return True
    
    @staticmethod
    async def restore_user_device(db: AsyncSession, device_id: uuid.UUID, current_user: User, background_tasks: BackgroundTasks) -> bool:
        """
        Khôi phục thiết bị người dùng đã bị xóa mềm.
        
        Args:
            db: Database session
            device_id: ID của thiết bị người dùng
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            True nếu khôi phục thành công
        """
        restored = await SoftDeleteMixin.restore(db, UserDevice, device_id)
        if restored:
            # Lấy thông tin thiết bị để đồng bộ lại với chatbot
            user_device = await UserDeviceRepository.get_by_id(db, device_id)
            if user_device and user_device.product_code:
                background_tasks.add_task(ChatbotService.add_product, user_device.id, current_user)
        return restored

    @staticmethod
    async def delete_many_devices(db: AsyncSession, user_device_ids: List[uuid.UUID], current_user: User, background_tasks: BackgroundTasks) -> int:
        """
        Xóa mềm nhiều thiết bị người dùng.
        
        Args:
            db: Database session
            user_device_ids: Danh sách ID của các thiết bị người dùng cần xóa
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            Số lượng thiết bị đã xóa
        """
        if not user_device_ids:
            return 0
            
        # Lấy product_codes trước khi xóa để đồng bộ với chatbot
        product_codes = await UserDeviceRepository.get_product_codes_by_ids(db, user_device_ids, current_user.id)
        
        # Thực hiện soft delete cho từng thiết bị
        deleted_count = 0
        for device_id in user_device_ids:
            success = await SoftDeleteMixin.soft_delete(db, UserDevice, device_id, days_to_purge=1)
            if success:
                deleted_count += 1
        
        if deleted_count > 0 and product_codes:
            # Xóa hàng loạt sản phẩm tương ứng khỏi chatbot trong background
            background_tasks.add_task(ChatbotService.bulk_delete_products, product_codes, current_user)
                    
        return deleted_count

    @staticmethod
    async def delete_all_devices(db: AsyncSession, current_user: User, background_tasks: BackgroundTasks) -> int:
        """
        Xóa mềm tất cả thiết bị của người dùng hiện tại.
        
        Args:
            db: Database session
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            Số lượng thiết bị đã xóa
        """
        # Lấy tất cả thiết bị của user trước khi xóa
        user_devices = await UserDeviceRepository.get_by_user_id(db, current_user.id, limit=None)
        
        deleted_count = 0
        for device in user_devices:
            success = await SoftDeleteMixin.soft_delete(db, UserDevice, device.id, days_to_purge=1)
            if success:
                deleted_count += 1
        
        if deleted_count > 0:
            # Xóa tất cả sản phẩm của user khỏi chatbot trong background
            background_tasks.add_task(ChatbotService.delete_all_products, current_user)
                    
        return deleted_count
    
    @staticmethod
    async def get_all_user_devices(db: AsyncSession, skip: int = 0, limit: int = 100, with_details: bool = False) -> List[UserDevice]:
        """
        Lấy danh sách thiết bị người dùng với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            with_details: Có lấy thông tin chi tiết không
            
        Returns:
            Danh sách các đối tượng UserDevice
        """
        if with_details:
            return await UserDeviceRepository.get_all_with_details(db, skip, limit)
        else:
            return await UserDeviceRepository.get_all(db, skip, limit)
    
    @staticmethod
    async def get_deleted_devices_today(db: AsyncSession, user_id: uuid.UUID) -> List[UserDevice]:
        """
        Lấy danh sách thiết bị đã bị xóa mềm trong ngày hôm nay của một user.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            Danh sách các thiết bị đã xóa trong ngày
        """
        return await UserDeviceRepository.get_deleted_today_by_user_id(db, user_id)
    
    @staticmethod
    async def restore_all_deleted_today(db: AsyncSession, user_id: uuid.UUID, current_user: User, background_tasks: BackgroundTasks) -> dict:
        """
        Khôi phục tất cả thiết bị đã bị xóa mềm trong ngày hôm nay của một user.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            current_user: Người dùng hiện tại
            background_tasks: Background tasks
            
        Returns:
            Dict chứa thông tin kết quả khôi phục
        """
        # Lấy danh sách thiết bị đã xóa trong ngày
        deleted_devices = await UserDeviceRepository.get_deleted_today_by_user_id(db, user_id)
        
        restored_count = 0
        for device in deleted_devices:
            success = await SoftDeleteMixin.restore(db, UserDevice, device.id)
            if success:
                restored_count += 1
        
        if restored_count > 0:
            # Thêm lại tất cả sản phẩm đã khôi phục vào chatbot trong background
            background_tasks.add_task(ChatbotService.add_all_products, current_user)
        
        return {
            "restored_count": restored_count,
            "message": f"Đã khôi phục {restored_count} thiết bị thành công"
        }