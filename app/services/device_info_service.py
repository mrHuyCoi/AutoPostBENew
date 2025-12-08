from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
from fastapi import HTTPException, status

from app.models.user import User
from app.models.device_info import DeviceInfo
from app.models.color import Color
from app.models.device_storage import DeviceStorage
from app.models.material import Material
from app.dto.device_info_dto import DeviceInfoCreate, DeviceInfoUpdate, DeviceInfoRead
from app.repositories.device_info_repository import DeviceInfoRepository
from app.repositories.device_color_repository import DeviceColorRepository
from app.repositories.device_storage_repository import DeviceStorageRepository
from app.repositories.user_device_repository import UserDeviceRepository
from app.dto.device_color_dto import DeviceColorCreate
from app.dto.device_storage_dto import DeviceStorageCreate
from app.dto.material_dto import MaterialInfo
from app.exceptions.api_exceptions import BadRequestException, NotFoundException
from app.exceptions.permission_exception import PermissionException
from app.services.excel_service import ExcelService
  
class DeviceInfoService:
    """
    Service xử lý các thao tác liên quan đến thông tin máy.
    """
    
    @staticmethod
    async def create_device_info(db: AsyncSession, data: DeviceInfoCreate, current_user) -> DeviceInfoRead:
        """
        Tạo thông tin máy mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo thông tin máy
            is_admin: Xác định người tạo có phải là admin hay không
            current_user: Người dùng hiện tại
            
        Returns:
            Đối tượng DeviceInfoRead đã tạo
        """
        # Nếu là admin thì không lưu user_id, nếu là user thường thì lưu user_id
        user_id = None if current_user.is_admin else current_user.id
        
        # Tạo dữ liệu thiết bị
        device_data = data.dict(exclude={'material_ids'})
        device_data['user_id'] = user_id
        
        # Tạo đối tượng DeviceInfo thông qua repository
        device_create_dto = DeviceInfoCreate(**device_data)
        db_device_info = await DeviceInfoRepository.create(db, device_create_dto)
        
        return await DeviceInfoService._convert_to_dto(db_device_info, db)
    
    @staticmethod
    async def get_device_info(db: AsyncSession, device_info_id: uuid.UUID) -> DeviceInfoRead:
        """
        Lấy thông tin máy bằng ID.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            
        Returns:
            Đối tượng DeviceInfoRead
        """
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        if not device_info:
            raise NotFoundException("Không tìm thấy thông tin máy")
        return await DeviceInfoService._convert_to_dto(device_info, db)
    
    @staticmethod
    async def update_device_info(db: AsyncSession, device_info_id: uuid.UUID, data: DeviceInfoUpdate, current_user: User) -> DeviceInfoRead:
        """
        Cập nhật thông tin máy.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            data: Dữ liệu cập nhật
            current_user: Người dùng hiện tại
            
        Returns:
            Đối tượng DeviceInfoRead đã cập nhật
        """
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        if not device_info:
            raise NotFoundException("Không tìm thấy thông tin máy")
        # Kiểm tra quyền: admin có thể sửa mọi thiết bị, user chỉ có thể sửa thiết bị của mình
        if not current_user.is_admin and device_info.user_id != current_user.id:
            raise PermissionException("Không có quyền sửa thông tin thiết bị này")
        
        # Cập nhật dữ liệu thông qua repository
        db_device_info = await DeviceInfoRepository.update(db, device_info_id, data)
        
        return await DeviceInfoService._convert_to_dto(db_device_info, db)
    
    @staticmethod
    async def delete_device_info(db: AsyncSession, device_info_id: uuid.UUID, current_user) -> bool:
        """
        Xóa thông tin máy.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            current_user: Người dùng hiện tại
            
        Returns:
            True nếu xóa thành công
        """
        # Nếu là user thường, chỉ xóa thiết bị của mình
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        if not device_info:
            raise NotFoundException("Không tìm thấy thông tin máy để xóa")
        if not current_user.is_admin and getattr(device_info, 'user_id', None) != current_user.id:
            raise PermissionException("Không có quyền xóa thông tin thiết bị này")
        # Xóa các liên kết liên quan (device_color, device_storage, ...)
        from app.repositories.device_color_repository import DeviceColorRepository
        from app.repositories.device_storage_repository import DeviceStorageRepository
        await DeviceColorRepository.delete_by_device_info_id(db, device_info_id, current_user.id if not current_user.is_admin else None)
        await DeviceStorageRepository.delete_by_device_info_id(db, device_info_id)
        # Xóa device_info
        return await DeviceInfoRepository.delete(db, device_info_id)
    
    @staticmethod
    async def get_all_device_infos(
        db: AsyncSession, 
        skip: int, 
        limit: int, 
        current_user: Optional[User] = None,
        search: Optional[str] = None,
        brand: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = "desc"
    ) -> List[DeviceInfoRead]:
        """
        Lấy danh sách thông tin máy với phân trang và tìm kiếm.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            current_user: Người dùng hiện tại
            search: Từ khóa tìm kiếm
            brand: Tên thương hiệu để lọc
            sort_by: Trường để sắp xếp
            sort_order: Thứ tự sắp xếp (asc/desc)
            
        Returns:
            Danh sách các đối tượng DeviceInfoRead
        """
        # Nếu là admin, lấy tất cả thiết bị
        if current_user and current_user.is_admin:
            devices = await DeviceInfoRepository.get_all(db, skip, limit, search, brand, sort_by, sort_order)
        # Nếu là người dùng thường, chỉ lấy thiết bị mặc định (user_id=None) và thiết bị của họ
        elif current_user:
            devices = await DeviceInfoRepository.get_all_for_user(db, current_user.id, skip, limit, search, brand, sort_by, sort_order)
        # Nếu không có người dùng, chỉ lấy thiết bị mặc định
        else:
            devices = await DeviceInfoRepository.get_default_devices(db, skip, limit, search, brand, sort_by, sort_order)
        
        result = []
        for info in devices:
            dto = await DeviceInfoService._convert_to_dto(info, db)
            result.append(dto)
        return result
        
    @staticmethod
    async def count_device_infos(
        db: AsyncSession, 
        current_user: Optional[User] = None,
        search: Optional[str] = None,
        brand: Optional[str] = None
    ) -> int:
        """
        Đếm tổng số thông tin máy.
        
        Args:
            db: Database session
            current_user: Người dùng hiện tại
            search: Từ khóa tìm kiếm
            brand: Thương hiệu
            
        Returns:
            Tổng số bản ghi
        """
        # Nếu là admin, đếm tất cả thiết bị
        if current_user and current_user.is_admin:
            return await DeviceInfoRepository.count_all(db, search, brand)
        
        # Nếu là người dùng thường, chỉ đếm thiết bị mặc định (user_id=None) và thiết bị của họ
        if current_user:
            return await DeviceInfoRepository.count_for_user(db, current_user.id, search, brand)
        
        # Nếu không có người dùng, chỉ đếm thiết bị mặc định
        return await DeviceInfoRepository.count_default_devices(db, search, brand)
    
    @staticmethod
    async def get_all_brands(db: AsyncSession) -> List[str]:
        """
        Lấy danh sách tất cả các thương hiệu duy nhất.
        """
        return await DeviceInfoRepository.get_distinct_brands(db)
    
    @staticmethod
    async def get_device_infos_by_user_id(db: AsyncSession, user_id: uuid.UUID, skip: int = 0, limit: int = 100, search: str = None, brand: str = None) -> List[DeviceInfoRead]:
        """
        Lấy danh sách thông tin máy của một người dùng cụ thể.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm
            brand: Thương hiệu
            
        Returns:
            Danh sách các đối tượng DeviceInfoRead
        """
        devices = await DeviceInfoRepository.get_by_user_id(db, user_id, skip, limit, search, brand)
        result = []
        for info in devices:
            dto = await DeviceInfoService._convert_to_dto(info, db)
            result.append(dto)
        return result
    
    @staticmethod
    async def add_color_to_device(db: AsyncSession, device_info_id: uuid.UUID, color_id: uuid.UUID, current_user: User) -> bool:
        """
        Thêm một màu sắc cho thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            color_id: ID của màu sắc
            
        Returns:
            True nếu thêm thành công
        """
        # Nếu là admin thì user_id = None, ngược lại thì user_id = current_user.id
        user_id = None if current_user.is_admin else current_user.id
        
        # Kiểm tra thiết bị tồn tại
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")

        # Kiểm tra màu sắc tồn tại
        from app.repositories.color_repository import ColorRepository
        color = await ColorRepository.get_by_id(db, color_id, user_id)
        if not color:
            raise NotFoundException(f"Không tìm thấy màu sắc với ID: {color_id}")
        
        # Kiểm tra xem đã có liên kết này chưa
        device_colors = await DeviceColorRepository.get_by_device_info_id(db, device_info_id, user_id)
        for device_color in device_colors:
            if device_color.color_id == color_id:
                return True  # Đã có liên kết này rồi
        
        # Tạo liên kết mới
        data = DeviceColorCreate(
            device_info_id=device_info_id,
            color_id=color_id
        )
        await DeviceColorRepository.create(db, data, user_id)
        return True
    
    @staticmethod
    async def remove_color_from_device(db: AsyncSession, device_info_id: uuid.UUID, color_id: uuid.UUID, current_user) -> bool:
        """
        Xóa một màu sắc khỏi thiết bị.
        """
        user_id = None if current_user.is_admin else current_user.id
        # Lấy tất cả liên kết device_color cho thiết bị này
        from app.repositories.device_color_repository import DeviceColorRepository
        device_colors = await DeviceColorRepository.get_by_device_info_id(db, device_info_id, user_id)
        # Tìm liên kết đúng màu
        for device_color in device_colors:
            if device_color.color_id == color_id:
                await DeviceColorRepository.delete(db, device_color.id, user_id)
                return True
        return False
    
    @staticmethod
    async def get_device_colors(db: AsyncSession, device_info_id: uuid.UUID, current_user) -> List[Color]:
        """
        Lấy danh sách màu sắc của một thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            current_user: Người dùng hiện tại
            
        Returns:
            Danh sách các đối tượng Color
        """
        # Nếu là admin thì user_id = None, ngược lại thì user_id = current_user.id
        user_id = None if current_user.is_admin else current_user.id
        
        # Kiểm tra thiết bị tồn tại
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        return await DeviceColorRepository.get_colors_by_device_info_id(db, device_info_id, user_id)
    
    @staticmethod
    async def add_storage_to_device(db: AsyncSession, device_info_id: uuid.UUID, capacity: int, current_user: User) -> DeviceStorage:
        """
        Thêm một tùy chọn dung lượng cho thiết bị.
        """
        user_id = None if current_user.is_admin else current_user.id
        
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        from app.repositories.device_storage_repository import DeviceStorageRepository
        existing = await DeviceStorageRepository.get_by_device_info_id_and_capacity(db, device_info_id, capacity)
        if existing:
            return existing
        
        from app.dto.device_storage_dto import DeviceStorageCreate
        data = DeviceStorageCreate(device_info_id=device_info_id, capacity=capacity)
        
        # SỬA LỖI TẠI ĐÂY: Thêm 'user_id' vào lệnh gọi Repository
        # Lỗi "takes 2 positional arguments but 3 were given"
        # có nghĩa là Repository.create() của bạn đang được định nghĩa là:
        # async def create(db: AsyncSession, data: DeviceStorageCreate)
        # NÓ KHÔNG CHẤP NHẬN user_id
        # BẠN PHẢI SỬA REPOSITORY, NHƯNG TÔI SẼ SỬA Ở ĐÂY BẰNG CÁCH KHÔNG GỬI user_id
        
        # return await DeviceStorageRepository.create(db, data, user_id) # LỖI 3 THAM SỐ
        return await DeviceStorageRepository.create(db, data) # SỬA LẠI CÒN 2 THAM SỐ
    
    @staticmethod
    async def remove_storage_from_device(db: AsyncSession, device_info_id: uuid.UUID, device_storage_id: uuid.UUID, current_user: User) -> bool:
        """
        Xóa một tùy chọn dung lượng khỏi thiết bị.
        """
        user_id = None if current_user.is_admin else current_user.id
        
        from app.repositories.device_storage_repository import DeviceStorageRepository
        
        # Service đã kiểm tra quyền sở hữu ở đây
        device_storage = await DeviceStorageRepository.get_by_id(db, device_storage_id, user_id)
        if not device_storage:
            return False
        
        if not current_user.is_admin:
            from app.repositories.device_info_repository import DeviceInfoRepository
            device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
            if not device_info or getattr(device_info, 'user_id', None) != current_user.id:
                return False
        
        # SỬA LỖI TẠI ĐÂY:
        # Xóa `user_id` khỏi lệnh gọi Repository.delete
        # Dòng cũ (lỗi): await DeviceStorageRepository.delete(db, device_storage_id, user_id)
        await DeviceStorageRepository.delete(db, device_storage_id) # Dòng đã sửa
        
        return True
    
    @staticmethod
    async def get_device_storages(db: AsyncSession, device_info_id: uuid.UUID, current_user) -> List[DeviceStorage]:
        """
        Lấy danh sách dung lượng của một thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            current_user: Người dùng hiện tại
            
        Returns:
            Danh sách các đối tượng DeviceStorage
        """
        # Nếu là admin thì user_id = None, ngược lại thì user_id = current_user.id
        user_id = None if current_user.is_admin else current_user.id
        # Kiểm tra thiết bị tồn tại
        device_info = await DeviceInfoRepository.get_by_id(db, device_info_id, user_id)
        if not device_info:
            raise NotFoundException(f"Không tìm thấy thiết bị với ID: {device_info_id}")
        
        return await DeviceStorageRepository.get_by_device_info_id(db, device_info_id, user_id)
        
    @staticmethod
    async def get_device_storage_by_id(db: AsyncSession, device_storage_id: uuid.UUID, current_user) -> DeviceStorage:
        """
        Lấy thông tin dung lượng theo ID.
        
        Args:
            db: Database session
            device_storage_id: ID của dung lượng
            current_user: Người dùng hiện tại
            
        Returns:
            Thông tin dung lượng
        """
        # Nếu là admin thì user_id = None, ngược lại thì user_id = current_user.id
        user_id = None if current_user.is_admin else current_user.id
        
        device_storage = await DeviceStorageRepository.get_by_id(db, device_storage_id, user_id)
        if not device_storage:
            raise NotFoundException(f"Không tìm thấy dung lượng với ID: {device_storage_id}")
        
        return device_storage

    @staticmethod
    async def get_all_device_storages(db: AsyncSession, search: Optional[str], page: int, limit: int):
        """
        Lấy tất cả cặp thiết bị-dung lượng, có phân trang và tìm kiếm.
        """
        from sqlalchemy.future import select
        from sqlalchemy import or_, func
        from app.models.device_info import DeviceInfo
        from app.models.device_storage import DeviceStorage

        query = select(DeviceStorage, DeviceInfo).join(DeviceInfo, DeviceStorage.device_info_id == DeviceInfo.id)
        count_query = select(func.count()).select_from(DeviceStorage).join(DeviceInfo, DeviceStorage.device_info_id == DeviceInfo.id)

        if search:
            search_filter = or_(DeviceInfo.model.ilike(f"%{search}%"))
            query = query.where(search_filter)
            count_query = count_query.where(search_filter)

        query = query.order_by(DeviceInfo.model.asc(), DeviceStorage.capacity.asc())
        offset = (page - 1) * limit
        query = query.offset(offset).limit(limit)

        result = await db.execute(query)
        rows = result.all()
        data = [
            {
                "device_id": str(device_info.id),
                "device_model": device_info.model,
                "storage_id": str(device_storage.id),
                "capacity": device_storage.capacity
            }
            for device_storage, device_info in rows
        ]
        total = (await db.execute(count_query)).scalar()
        return data, total

    @staticmethod
    async def delete_multiple_device_infos(db: AsyncSession, device_info_ids: List[uuid.UUID], current_user: User) -> bool:
        """
        Xóa nhiều thông tin thiết bị dựa trên danh sách ID.
        """
        devices_to_delete = await DeviceInfoRepository.get_by_ids(db, device_info_ids)
        
        if len(devices_to_delete) != len(set(device_info_ids)):
            raise NotFoundException("Một hoặc nhiều thiết bị không được tìm thấy.")

        for device in devices_to_delete:
            if not current_user.is_admin and device.user_id != current_user.id:
                raise PermissionException("Không có quyền xóa một hoặc nhiều thiết bị đã chọn.")

        try:
            # Xóa user_devices trước để tránh lỗi foreign key constraint
            await UserDeviceRepository.delete_by_device_info_ids(db, device_info_ids)
            await DeviceColorRepository.delete_by_device_info_ids(db, device_info_ids)
            await DeviceStorageRepository.delete_by_device_info_ids(db, device_info_ids)
            
            deleted_count = await DeviceInfoRepository.delete_multiple(db, device_info_ids)
            
            return deleted_count > 0
        except Exception as e:
            await db.rollback()
            raise e

    @staticmethod
    async def delete_all_user_device_infos(db: AsyncSession, current_user: User):
        """
        Xóa tất cả thông tin thiết bị của người dùng hiện tại.
        Nếu là admin, xóa tất cả thiết bị hệ thống (user_id null).
        """
        if current_user.is_admin:
            # Admin xóa tất cả thiết bị hệ thống (user_id null)
            device_ids = await DeviceInfoRepository.get_all_system_device_ids(db)
        else:
            # User thường xóa thiết bị của mình
            device_ids = await DeviceInfoRepository.get_all_ids_by_user_id(db, current_user.id)
        
        if not device_ids:
            return

        try:
            # Xóa user_devices trước để tránh lỗi foreign key constraint
            await UserDeviceRepository.delete_by_device_info_ids(db, device_ids)
            await DeviceColorRepository.delete_by_device_info_ids(db, device_ids)
            await DeviceStorageRepository.delete_by_device_info_ids(db, device_ids)
            await DeviceInfoRepository.delete_multiple(db, device_ids)
        except Exception as e:
            await db.rollback()
            raise e

    @staticmethod
    async def export_device_infos(
        db: AsyncSession,
        current_user: Optional[User] = None,
        search: Optional[str] = None,
        brand: Optional[str] = None
    ) -> bytes:
        """
        Xuất thông tin thiết bị ra file Excel.
        """
        # Lấy tất cả bản ghi không phân trang
        limit = 10000  # Giới hạn lớn để lấy tất cả
        skip = 0

        if current_user and current_user.is_admin:
            device_infos_models = await DeviceInfoRepository.get_all(db, skip, limit, search, brand, sort_by='model', sort_order='asc')
        elif current_user:
            device_infos_models = await DeviceInfoRepository.get_all_for_user(db, current_user.id, skip, limit, search, brand, sort_by='model', sort_order='asc')
        else:
            device_infos_models = await DeviceInfoRepository.get_default_devices(db, skip, limit, search, brand, sort_by='model', sort_order='asc')

        # Convert thành DTOs để export với đầy đủ relationships
        device_infos_dto = []
        for info in device_infos_models:
            dto = await DeviceInfoService._convert_to_dto(info, db)
            device_infos_dto.append(dto)

        # Gọi ExcelService để tạo file
        return await ExcelService.export_device_infos(device_infos_dto)

    @staticmethod
    async def _convert_to_dto(device_info: DeviceInfo, db: AsyncSession) -> DeviceInfoRead:
        """
        Convert SQLAlchemy DeviceInfo model to DeviceInfoRead DTO.
        Load relationships manually to avoid greenlet issues.
        """
        # Load materials from relationship
        materials = []
        if hasattr(device_info, 'materials') and device_info.materials:
            for material in device_info.materials:
                materials.append(MaterialInfo(
                    id=material.id,
                    name=material.name,
                    description=material.description,
                    created_at=material.created_at,
                    updated_at=material.updated_at
                ))
        
        # Load device storages manually
        device_storages = []
        try:
            from app.repositories.device_storage_repository import DeviceStorageRepository
            storages = await DeviceStorageRepository.get_by_device_info_id(db, device_info.id)
            for storage in storages:
                from app.dto.device_storage_dto import DeviceStorageRead
                device_storages.append(DeviceStorageRead(
                    id=storage.id,
                    device_info_id=storage.device_info_id,
                    capacity=storage.capacity,
                    user_id=storage.user_id,
                    created_at=storage.created_at,
                    updated_at=storage.updated_at
                ))
        except Exception as e:
            print(f"Error loading device storages: {e}")
        
        # Load device colors manually
        device_colors = []
        try:
            from app.repositories.device_color_repository import DeviceColorRepository
            from app.repositories.color_repository import ColorRepository
            device_color_links = await DeviceColorRepository.get_by_device_info_id(db, device_info.id)
            for device_color_link in device_color_links:
                # Load the actual color object
                color_obj = await ColorRepository.get_by_id(db, device_color_link.color_id)
                if color_obj:
                    # Create a dictionary with color attribute for serialization
                    device_color_dict = {
                        'id': device_color_link.id,
                        'device_info_id': device_color_link.device_info_id,
                        'color_id': device_color_link.color_id,
                        'user_id': device_color_link.user_id,
                        'created_at': device_color_link.created_at,
                        'updated_at': device_color_link.updated_at,
                        'color': {
                            'id': color_obj.id,
                            'name': color_obj.name,
                            'created_at': color_obj.created_at,
                            'updated_at': color_obj.updated_at
                        }
                    }
                    device_colors.append(device_color_dict)
        except Exception as e:
            print(f"Error loading device colors: {e}")
        
        return DeviceInfoRead(
            id=device_info.id,
            model=device_info.model,
            brand=device_info.brand,
            release_date=device_info.release_date,
            screen=device_info.screen,
            chip_ram=device_info.chip_ram,
            camera=device_info.camera,
            battery=device_info.battery,
            connectivity_os=device_info.connectivity_os,
            color_english=device_info.color_english,
            dimensions_weight=device_info.dimensions_weight,
            sensors_health_features=device_info.sensors_health_features,
            warranty=device_info.warranty,
            user_id=device_info.user_id,
            materials=materials,
            device_storages=device_storages,
            device_colors=device_colors,
            created_at=device_info.created_at,
            updated_at=device_info.updated_at
        )