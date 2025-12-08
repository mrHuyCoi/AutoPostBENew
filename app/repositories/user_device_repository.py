from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, Integer, or_, and_
from typing import Optional, List
import uuid
from sqlalchemy.orm import joinedload
import re
from datetime import datetime, date

from app.models.user_device import UserDevice
from app.dto.user_device_dto import UserDeviceCreate, UserDeviceUpdate
from app.models.device_storage import DeviceStorage
from app.models.color import Color
from app.models.device_info import DeviceInfo


class UserDeviceRepository:
    """
    Repository xử lý các thao tác CRUD cho đối tượng UserDevice.
    """
    
    @staticmethod
    def _format_warranty(warranty: str) -> str:
        """
        Định dạng trường bảo hành.
        
        Args:
            warranty: Trường bảo hành
        
        Returns:
            Trường bảo hành đã định dạng
        """
        if warranty:
            # Kiểm tra nếu chỉ chứa số thì thêm "tháng" ở cuối
            if re.match(r'^\d+$', warranty.strip()):
                warranty = f"{warranty.strip()} tháng"
        return warranty
    
    @staticmethod
    def _format_percentage_field(value: Optional[str]) -> Optional[str]:
        """
        Định dạng các trường dạng phần trăm. Hỗ trợ các trường hợp:
        - "99" -> "99%"
        - 0.99 hoặc "0.99" -> "99%"
        - "99%" giữ nguyên (chuẩn hóa bỏ .0)
        - "99.0" -> "99%" (nếu trong khoảng 0..100)
        """
        if value is None:
            return value
        s = str(value).strip()
        if not s:
            return s
        # Đã có ký hiệu %
        if re.match(r'^\d+(?:\.\d+)?%$', s):
            try:
                num = float(s.replace('%', ''))
                return f"{int(round(num))}%"
            except Exception:
                return s
        # Giá trị số
        try:
            f = float(s)
            if 0.0 <= f <= 1.0:
                return f"{int(round(f * 100))}%"
            if 0.0 <= f <= 100.0:
                return f"{int(round(f))}%"
        except Exception:
            pass
        # Số nguyên dạng chuỗi
        if re.match(r'^\d+$', s):
            return f"{s}%"
        return s
    
    @staticmethod
    def _format_battery_condition(battery_condition: str) -> str:
        """
        Định dạng trường tình trạng pin.
        
        Args:
            battery_condition: Trường tình trạng pin
        
        Returns:
            Trường tình trạng pin đã định dạng với dấu phần trăm
        """
        if battery_condition is None:
            return battery_condition
        return UserDeviceRepository._format_percentage_field(battery_condition)
    
    @staticmethod
    async def create(db: AsyncSession, data: UserDeviceCreate) -> UserDevice:
        """
        Tạo một thông tin thiết bị của người dùng mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo thông tin thiết bị
            
        Returns:
            Đối tượng UserDevice đã tạo
        """
        # Tạo mã sản phẩm tự động nếu không được cung cấp
        product_code = data.product_code
        if not product_code:
            # Lấy mã sản phẩm lớn nhất hiện tại (theo từng user, loại trừ bản ghi đã xóa mềm)
            last_code_row = await db.execute(
                select(UserDevice.product_code)
                .where(
                    UserDevice.product_code.like("SP%"),
                    UserDevice.user_id == data.user_id,
                    UserDevice.trashed_at.is_(None)
                )
                .order_by(func.cast(func.substr(UserDevice.product_code, 3), Integer).desc())
                .limit(1)
            )
            last_code = last_code_row.scalar_one_or_none()
            
            if last_code and last_code.startswith("SP") and last_code[2:].isdigit():
                last_number = int(last_code[2:])
                new_number = last_number + 1
                product_code = f"SP{new_number:06d}"
            else:
                product_code = "SP000001"
        
        # Xử lý trường bảo hành
        warranty = UserDeviceRepository._format_warranty(data.warranty)
        
        # Xử lý tự động tình trạng máy và pin dựa trên loại máy
        device_condition = data.device_condition
        battery_condition = data.battery_condition
        
        # Nếu loại máy là "Mới" hoặc "mới" thì tự động set tình trạng máy và pin
        if data.device_type and data.device_type.lower() == "mới":
            device_condition = "Mới"
            battery_condition = "100%"
        else:
            # Xử lý trường tình trạng pin bình thường
            battery_condition = UserDeviceRepository._format_battery_condition(battery_condition)
            # Xử lý tình trạng máy nếu nhập dạng số/thập phân (ví dụ 0.99 -> 99%)
            device_condition = UserDeviceRepository._format_percentage_field(device_condition)
        
        # Tạo đối tượng UserDevice
        db_user_device = UserDevice(
            user_id=data.user_id,
            device_info_id=data.device_info_id,
            color_id=data.color_id,
            device_storage_id=data.device_storage_id,
            product_code=product_code,
            warranty=warranty,
            device_condition=device_condition,
            device_type=data.device_type,
            battery_condition=battery_condition,
            price=data.price,
            wholesale_price=data.wholesale_price,
            inventory=data.inventory,
            notes=data.notes
        )
        
        # Lưu vào database
        db.add(db_user_device)
        await db.commit()

        # Lấy lại đối tượng với các mối quan hệ đã được tải sẵn (eager loading)
        # để tránh lỗi MissingGreenlet khi truy cập các mối quan hệ sau này.
        created_device = await UserDeviceRepository.get_by_id_with_details(db, db_user_device.id)
        
        return created_device
    
    @staticmethod
    async def get_by_id(db: AsyncSession, user_device_id: uuid.UUID) -> Optional[UserDevice]:
        """
        Lấy thông tin thiết bị của người dùng bằng ID.
        
        Args:
            db: Database session
            user_device_id: ID của thông tin thiết bị
            
        Returns:
            Đối tượng UserDevice hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserDevice)
            .where(UserDevice.id == user_device_id, UserDevice.trashed_at.is_(None))
        )
        return result.scalars().first()
    
    @staticmethod
    async def get_by_id_with_details(db: AsyncSession, user_device_id: uuid.UUID) -> Optional[UserDevice]:
        """
        Lấy thông tin chi tiết thiết bị của người dùng bằng ID, bao gồm thông tin liên quan.
        
        Args:
            db: Database session
            user_device_id: ID của thông tin thiết bị
            
        Returns:
            Đối tượng UserDevice với thông tin liên quan hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserDevice)
            .options(
                joinedload(UserDevice.device_info),
                joinedload(UserDevice.color),
                joinedload(UserDevice.device_storage)
            )
            .where(UserDevice.id == user_device_id, UserDevice.trashed_at.is_(None))
        )
        return result.scalars().first()
    
    @staticmethod
    def _apply_filters(query, filters: Optional[dict] = None):
        if not filters:
            return query

        # Search by model or product code
        if filters.get("search"):
            search_term = f"%{filters['search']}%"
            query = query.join(DeviceInfo).where(
                or_(
                    DeviceInfo.model.ilike(search_term),
                    UserDevice.product_code.ilike(search_term)
                )
            )

        if filters.get("brand"):
            # If we haven't joined DeviceInfo yet for search, join it now
            if "search" not in filters:
                query = query.join(DeviceInfo)
            query = query.where(DeviceInfo.brand.ilike(f"%{filters['brand']}%"))
        
        if filters.get("inventory_min") is not None:
            query = query.where(UserDevice.inventory >= filters["inventory_min"])
        if filters.get("inventory_max") is not None:
            query = query.where(UserDevice.inventory <= filters["inventory_max"])
            
        if filters.get("price_min") is not None:
            query = query.where(UserDevice.price >= filters["price_min"])
        if filters.get("price_max") is not None:
            query = query.where(UserDevice.price <= filters["price_max"])
            
        if filters.get("wholesale_price_min") is not None:
            query = query.where(UserDevice.wholesale_price >= filters["wholesale_price_min"])
        if filters.get("wholesale_price_max") is not None:
            query = query.where(UserDevice.wholesale_price <= filters["wholesale_price_max"])
            
        if filters.get("storage_capacity") is not None:
            # We need to join with DeviceStorage to filter by capacity
            query = query.join(DeviceStorage).where(DeviceStorage.capacity == filters["storage_capacity"])
            
        return query

    @staticmethod
    async def get_by_user_id(
        db: AsyncSession, 
        user_id: uuid.UUID, 
        skip: int = 0, 
        limit: int = 100,
        filters: Optional[dict] = None
    ) -> List[UserDevice]:
        query = select(UserDevice).where(UserDevice.user_id == user_id, UserDevice.trashed_at.is_(None))
        query = UserDeviceRepository._apply_filters(query, filters)
        query = query.offset(skip).limit(limit)
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_by_user_id_with_details(
        db: AsyncSession, 
        user_id: uuid.UUID, 
        skip: int = 0, 
        limit: Optional[int] = 100,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = 'asc',
        filters: Optional[dict] = None
    ) -> List[UserDevice]:
        query = (
            select(UserDevice)
            .options(
                joinedload(UserDevice.device_info),
                joinedload(UserDevice.color),
                joinedload(UserDevice.device_storage)
            )
            .where(UserDevice.user_id == user_id, UserDevice.trashed_at.is_(None))
        )
        
        query = UserDeviceRepository._apply_filters(query, filters)

        # Sắp xếp
        if sort_by:
            if hasattr(UserDevice, sort_by):
                column = getattr(UserDevice, sort_by)
                if sort_order == 'desc':
                    query = query.order_by(column.desc())
                else:
                    query = query.order_by(column.asc())
            elif sort_by in ['storageCapacity', 'capacity']:
                # JOIN cứng vào DeviceStorage để order_by hoạt động
                query = query.join(DeviceStorage, UserDevice.device_storage_id == DeviceStorage.id)
                if sort_order == 'desc':
                    query = query.order_by(DeviceStorage.capacity.desc())
                else:
                    query = query.order_by(DeviceStorage.capacity.asc())
            elif sort_by in ['colorName', 'color', 'name']:
                # JOIN cứng vào Color để order_by hoạt động
                query = query.join(Color, UserDevice.color_id == Color.id)
                if sort_order == 'desc':
                    query = query.order_by(Color.name.desc())
                else:
                    query = query.order_by(Color.name.asc())
            elif sort_by in ['wholesale_price']:
                # Sắp xếp theo giá bán buôn
                if sort_order == 'desc':
                    query = query.order_by(UserDevice.wholesale_price.desc().nullslast())
                else:
                    query = query.order_by(UserDevice.wholesale_price.asc().nullslast())

        query = query.offset(skip)
        if limit is not None:
            query = query.limit(limit)
        
        result = await db.execute(query)
        return result.scalars().unique().all()
    
    @staticmethod
    async def update(db: AsyncSession, user_device_id: uuid.UUID, data: UserDeviceUpdate) -> Optional[UserDevice]:
        """
        Cập nhật thông tin thiết bị của người dùng.
        
        Args:
            db: Database session
            user_device_id: ID của thông tin thiết bị
            data: Dữ liệu cập nhật
            
        Returns:
            Đối tượng UserDevice đã cập nhật hoặc None nếu không tìm thấy
        """
        # Sử dụng get_by_id_with_details để tải sẵn các mối quan hệ (eager loading)
        db_user_device = await UserDeviceRepository.get_by_id_with_details(db, user_device_id)
        
        if not db_user_device:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        
        # Xử lý tự động tình trạng máy và pin nếu loại máy được cập nhật
        if 'device_type' in update_data and update_data['device_type'] and update_data['device_type'].lower() == "mới":
            update_data['device_condition'] = "Mới"
            update_data['battery_condition'] = "100%"
        
        for key, value in update_data.items():
            # Xử lý trường bảo hành nếu có cập nhật
            if key == 'warranty':
                value = UserDeviceRepository._format_warranty(value)
            # Xử lý trường tình trạng pin nếu có cập nhật (trừ khi đã được set tự động)
            elif key == 'battery_condition' and not (update_data.get('device_type', '').lower() == "mới"):
                value = UserDeviceRepository._format_battery_condition(value)
            # Xử lý trường tình trạng máy nếu là số/thập phân
            elif key == 'device_condition' and not (update_data.get('device_type', '').lower() == "mới"):
                value = UserDeviceRepository._format_percentage_field(value)
            setattr(db_user_device, key, value)
        
        # Lưu thay đổi
        await db.commit()
        
        # Tải lại đối tượng với các mối quan hệ để trả về dữ liệu mới nhất
        updated_device = await UserDeviceRepository.get_by_id_with_details(db, user_device_id)

        return updated_device
    
    @staticmethod
    async def delete(db: AsyncSession, user_device_id: uuid.UUID) -> bool:
        """
        Xóa thông tin thiết bị của người dùng.
        
        Args:
            db: Database session
            user_device_id: ID của thông tin thiết bị
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        db_user_device = await UserDeviceRepository.get_by_id(db, user_device_id)
        
        if not db_user_device:
            return False
        
        await db.delete(db_user_device)
        await db.commit()
        
        return True
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[UserDevice]:
        """
        Lấy danh sách thông tin thiết bị của người dùng với phân trang.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            
        Returns:
            Danh sách các đối tượng UserDevice
        """
        result = await db.execute(
            select(UserDevice)
            .where(UserDevice.trashed_at.is_(None))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_all_with_details(db: AsyncSession, skip: int = 0, limit: int = 100) -> List[UserDevice]:
        """
        Lấy danh sách chi tiết thông tin thiết bị của người dùng với phân trang, bao gồm thông tin liên quan.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            
        Returns:
            Danh sách các đối tượng UserDevice với thông tin liên quan
        """
        result = await db.execute(
            select(UserDevice)
            .options(
                joinedload(UserDevice.device_info),
                joinedload(UserDevice.color),
                joinedload(UserDevice.device_storage)
            )
            .where(UserDevice.trashed_at.is_(None))
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
    
    @staticmethod
    async def get_by_product_code_and_user_id(db: AsyncSession, product_code: str, user_id: uuid.UUID) -> Optional[UserDevice]:
        """
        Lấy thông tin thiết bị của người dùng bằng product_code và user_id.
        
        Args:
            db: Database session
            product_code: Mã sản phẩm
            user_id: ID của người dùng
            
        Returns:
            Đối tượng UserDevice hoặc None nếu không tìm thấy
        """
        result = await db.execute(
            select(UserDevice)
            .where(UserDevice.product_code == product_code)
            .where(UserDevice.user_id == user_id)
            .where(UserDevice.trashed_at.is_(None))
        )
        return result.scalars().first()

    @staticmethod
    async def find_duplicate(
        db: AsyncSession,
        user_id: uuid.UUID,
        device_info_id: uuid.UUID,
        color_id: uuid.UUID,
        device_storage_id: uuid.UUID,
        price: float,
        wholesale_price: Optional[float],
        device_type: str,
        device_condition: str,
        battery_condition: Optional[str],
        warranty: Optional[str],
        exclude_id: Optional[uuid.UUID] = None
    ) -> Optional[UserDevice]:
        """
        Tìm kiếm một thiết bị trùng lặp dựa trên các thuộc tính chính.

        Args:
            db: Database session
            user_id: ID của người dùng
            device_info_id: ID thông tin thiết bị
            color_id: ID màu sắc
            device_storage_id: ID dung lượng
            price: Giá
            wholesale_price: Giá bán buôn
            device_type: Loại thiết bị
            device_condition: Tình trạng thiết bị
            battery_condition: Tình trạng pin
            warranty: Bảo hành
            exclude_id: ID của thiết bị cần loại trừ khỏi tìm kiếm (dùng khi cập nhật)

        Returns:
            Đối tượng UserDevice trùng lặp nếu có, ngược lại là None
        """
        # Xử lý trường bảo hành, tình trạng pin và tình trạng máy để so sánh nhất quán
        formatted_warranty = UserDeviceRepository._format_warranty(warranty)
        formatted_battery_condition = UserDeviceRepository._format_battery_condition(battery_condition)
        formatted_device_condition = UserDeviceRepository._format_percentage_field(device_condition)

        query = select(UserDevice).where(
            UserDevice.user_id == user_id,
            UserDevice.device_info_id == device_info_id,
            UserDevice.color_id == color_id,
            UserDevice.device_storage_id == device_storage_id,
            UserDevice.warranty == formatted_warranty,
            UserDevice.device_condition == formatted_device_condition,
            UserDevice.device_type == device_type,
            UserDevice.battery_condition == formatted_battery_condition,
            UserDevice.price == price,
            UserDevice.wholesale_price == wholesale_price,
            UserDevice.trashed_at.is_(None)
        )

        if exclude_id:
            query = query.where(UserDevice.id != exclude_id)

        result = await db.execute(query)
        return result.scalars().first()
    @staticmethod
    async def count_by_user_id(db: AsyncSession, user_id: uuid.UUID, filters: Optional[dict] = None) -> int:
        """
        Đếm số lượng thiết bị của người dùng.

        Args:
            db: Database session
            user_id: ID của người dùng
            filters: Bộ lọc

        Returns:
            Số lượng thiết bị
        """
        query = select(func.count(UserDevice.id)).where(UserDevice.user_id == user_id, UserDevice.trashed_at.is_(None))
        query = UserDeviceRepository._apply_filters(query, filters)
        result = await db.execute(query)
        return result.scalar_one()

    @staticmethod
    async def get_product_codes_by_ids(db: AsyncSession, user_device_ids: List[uuid.UUID], user_id: uuid.UUID) -> List[str]:
        """
        Lấy danh sách product_code từ danh sách ID thiết bị.
        """
        query = select(UserDevice.product_code).where(UserDevice.id.in_(user_device_ids), UserDevice.user_id == user_id, UserDevice.trashed_at.is_(None))
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def delete_many(db: AsyncSession, user_device_ids: List[uuid.UUID], user_id: uuid.UUID) -> int:
        """
        Xóa nhiều thiết bị người dùng dựa trên danh sách ID.
        """
        query = select(UserDevice).where(UserDevice.id.in_(user_device_ids), UserDevice.user_id == user_id, UserDevice.trashed_at.is_(None))
        result = await db.execute(query)
        devices_to_delete = result.scalars().all()
        count = len(devices_to_delete)
        if count == 0:
            return 0
        for device in devices_to_delete:
            await db.delete(device)
        await db.commit()
        return count

    @staticmethod
    async def delete_all_by_user(db: AsyncSession, user_id: uuid.UUID) -> (int, List[str]):
        """
        Xóa tất cả thiết bị của một người dùng.
        """
        query = select(UserDevice).where(UserDevice.user_id == user_id)
        result = await db.execute(query)
        devices_to_delete = result.scalars().all()
        count = len(devices_to_delete)
        if count == 0:
            return 0, []
        product_codes = [device.product_code for device in devices_to_delete if device.product_code]
        for device in devices_to_delete:
            await db.delete(device)
        await db.commit()
        return count, product_codes

    @staticmethod
    async def delete_by_device_info_ids(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> int:
        """
        Xóa tất cả user_devices có device_info_id trong danh sách.
        """
        from sqlalchemy import delete
        
        if not device_info_ids:
            return 0
            
        stmt = delete(UserDevice).where(UserDevice.device_info_id.in_(device_info_ids))
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount

    @staticmethod
    async def get_deleted_today_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> List[UserDevice]:
        """
        Lấy danh sách thiết bị đã bị xóa mềm trong ngày hôm nay của một user.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            Danh sách các thiết bị đã xóa trong ngày
        """
        today = date.today()
        
        query = select(UserDevice).options(
            joinedload(UserDevice.device_info),
            joinedload(UserDevice.color),
            joinedload(UserDevice.device_storage)
        ).where(
            and_(
                UserDevice.user_id == user_id,
                UserDevice.trashed_at.isnot(None),  # Đã bị xóa mềm
                func.date(UserDevice.trashed_at) == today  # Xóa trong ngày hôm nay
            )
        )
        
        result = await db.execute(query)
        return result.scalars().all()