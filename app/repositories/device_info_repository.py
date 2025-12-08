from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, func, distinct, case, delete
from sqlalchemy.orm import selectinload
from typing import Optional, List
import uuid
import logging

logger = logging.getLogger(__name__)

from app.models.device_info import DeviceInfo
from app.models.material import Material
from app.models.color import Color
from app.models.device_color import DeviceColor
from app.models.associations import device_material_association
from app.dto.device_info_dto import DeviceInfoCreate, DeviceInfoUpdate


class DeviceInfoRepository:
    """
    Repository xử lý các thao tác CRUD cho đối tượng DeviceInfo.
    """
    
    @staticmethod
    async def create(db: AsyncSession, data: DeviceInfoCreate) -> DeviceInfo:
        """
        Tạo một thông tin máy mới.
        
        Args:
            db: Database session
            data: Dữ liệu tạo thông tin máy
            
        Returns:
            Đối tượng DeviceInfo đã tạo
        """
        # Tách material_ids ra khỏi dữ liệu
        material_ids = data.material_ids if data.material_ids else []
        device_data = data.dict(exclude={'material_ids'})
        
        # Tạo đối tượng DeviceInfo
        db_device_info = DeviceInfo(**device_data)
        
        # Xử lý vật liệu nếu có
        if material_ids:
            materials = await db.execute(select(Material).where(Material.id.in_(material_ids)))
            db_device_info.materials = materials.scalars().all()
        
        # Lưu vào database
        db.add(db_device_info)
        await db.commit()
        await db.refresh(db_device_info)
        
        return db_device_info
    
    @staticmethod
    async def get_by_id(db: AsyncSession, device_info_id: uuid.UUID, user_id: Optional[uuid.UUID] = None) -> Optional[DeviceInfo]:
        """
        Lấy thông tin máy bằng ID.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            user_id: ID của người dùng (nếu None thì lấy thiết bị có user_id = null)
            
        Returns:
            Đối tượng DeviceInfo hoặc None nếu không tìm thấy
        """
        query = select(DeviceInfo)
        if user_id is not None:
            query = query.where(DeviceInfo.id == device_info_id, or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None)))
        else:
            query = query.where(DeviceInfo.id == device_info_id)
        
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def get_by_model(db: AsyncSession, model: str) -> Optional[DeviceInfo]:
        """
        Lấy thông tin máy bằng tên model.
        
        Args:
            db: Database session
            model: Tên model của thiết bị
            
        Returns:
            Đối tượng DeviceInfo hoặc None nếu không tìm thấy
        """
        query = select(DeviceInfo).where(DeviceInfo.model == model)
        result = await db.execute(query)
        return result.scalars().first()
    
    @staticmethod
    async def get_by_model_and_materials(db: AsyncSession, model: str, material_ids: List[uuid.UUID], user_id: Optional[uuid.UUID]) -> Optional[DeviceInfo]:
        """
        Lấy thông tin máy bằng tên model và một bộ vật liệu chính xác.
        """
        sorted_material_ids = sorted(material_ids, key=lambda x: str(x))

        q = select(DeviceInfo).options(selectinload(DeviceInfo.materials)).where(
            DeviceInfo.model == model
        )

        if user_id:
            q = q.where(or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None)))
        else:
            q = q.where(DeviceInfo.user_id.is_(None))

        result = await db.execute(q)
        devices = result.scalars().all()

        for device in devices:
            existing_material_ids = sorted([m.id for m in device.materials], key=lambda x: str(x))
            # Debug logging
            logger.info(f"Comparing device {device.id}: existing materials {[str(mid) for mid in existing_material_ids]} vs new materials {[str(mid) for mid in sorted_material_ids]}")
            if existing_material_ids == sorted_material_ids:
                logger.info(f"Found matching device: {device.id}")
                return device

        return None

    @staticmethod
    async def get_by_variant(db: AsyncSession, model: str, color_name: str, material_ids: List[uuid.UUID], user_id: Optional[uuid.UUID]) -> Optional[DeviceInfo]:
        """
        Lấy thông tin máy bằng model, tên màu, và một bộ vật liệu chính xác.
        """
        sorted_material_ids = sorted(material_ids, key=lambda x: str(x))

        q = (
            select(DeviceInfo)
            .where(DeviceInfo.model == model)
            .join(DeviceInfo.device_colors)
            .join(DeviceColor.color)
            .where(Color.name == color_name)
        )

        if user_id:
            q = q.where(or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None)))
        else:
            q = q.where(DeviceInfo.user_id.is_(None))

        result = await db.execute(q)
        devices = result.scalars().unique().all()

        for device in devices:
            existing_material_ids = sorted([m.id for m in device.materials], key=lambda x: str(x))
            if existing_material_ids == sorted_material_ids:
                return device

        return None

    @staticmethod
    async def get_by_model_and_user(db: AsyncSession, model: str, user_id: uuid.UUID) -> Optional[DeviceInfo]:
        """
        Lấy thông tin máy bằng tên model và user_id.
        """
        query = select(DeviceInfo).where(
            DeviceInfo.model == model,
            or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None))
        )
        result = await db.execute(query)
        # This might return multiple devices, so we take the first one for backward compatibility.
        return result.scalars().first()
    
    @staticmethod
    async def update(db: AsyncSession, device_info_id: uuid.UUID, data: DeviceInfoUpdate) -> Optional[DeviceInfo]:
        """
        Cập nhật thông tin máy.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            data: Dữ liệu cập nhật
            
        Returns:
            Đối tượng DeviceInfo đã cập nhật hoặc None nếu không tìm thấy
        """
        db_device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        if not db_device_info:
            return None
        
        # Cập nhật các trường
        update_data = data.dict(exclude_unset=True)
        
        # Xử lý material_ids nếu có trong dữ liệu cập nhật
        if 'material_ids' in update_data:
            material_ids = update_data.pop('material_ids')
            if material_ids is not None:
                # Xóa tất cả associations cũ trước
                await DeviceInfoRepository.delete_device_material_associations(db, [device_info_id])
                # Thêm materials mới
                materials = await db.execute(select(Material).where(Material.id.in_(material_ids)))
                db_device_info.materials = materials.scalars().all()
            else:
                # Xóa tất cả associations nếu material_ids là None hoặc rỗng
                await DeviceInfoRepository.delete_device_material_associations(db, [device_info_id])
                db_device_info.materials = []
        
        # Cập nhật các trường còn lại
        for key, value in update_data.items():
            setattr(db_device_info, key, value)
        
        # Lưu thay đổi
        await db.commit()
        await db.refresh(db_device_info)
        
        # Load lại materials relationship sau khi update
        result = await db.execute(
            select(DeviceInfo)
            .options(selectinload(DeviceInfo.materials))
            .where(DeviceInfo.id == device_info_id)
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def get_by_ids(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> List[DeviceInfo]:
        """
        Lấy danh sách thông tin máy bằng danh sách ID.
        """
        if not device_info_ids:
            return []
        query = select(DeviceInfo).where(DeviceInfo.id.in_(device_info_ids))
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def delete_multiple(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> int:
        """
        Xóa nhiều thông tin máy dựa trên danh sách ID.
        """
        if not device_info_ids:
            return 0
        
        # Xóa các liên kết device-material trước
        await DeviceInfoRepository.delete_device_material_associations(db, device_info_ids)
        
        # Sử dụng delete() trực tiếp trên table object để hiệu quả hơn
        stmt = delete(DeviceInfo).where(DeviceInfo.id.in_(device_info_ids))
        result = await db.execute(stmt)
        await db.commit()
        return result.rowcount

    @staticmethod
    async def get_all_ids_by_user_id(db: AsyncSession, user_id: uuid.UUID) -> List[uuid.UUID]:
        """
        Lấy danh sách tất cả ID thông tin máy của một người dùng.
        """
        query = select(DeviceInfo.id).where(DeviceInfo.user_id == user_id)
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def get_all_system_device_ids(db: AsyncSession) -> List[uuid.UUID]:
        """
        Lấy danh sách tất cả ID thông tin máy của hệ thống (user_id là null).
        """
        query = select(DeviceInfo.id).where(DeviceInfo.user_id.is_(None))
        result = await db.execute(query)
        return result.scalars().all()

    @staticmethod
    async def delete(db: AsyncSession, device_info_id: uuid.UUID) -> bool:
        """
        Xóa thông tin máy.
        
        Args:
            db: Database session
            device_info_id: ID của thông tin máy
            
        Returns:
            True nếu xóa thành công, False nếu không tìm thấy
        """
        db_device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        if not db_device_info:
            return False
        
        # Xóa các liên kết device-material trước
        await DeviceInfoRepository.delete_device_material_associations(db, [device_info_id])
        
        await db.delete(db_device_info)
        await db.commit()
        return True
    
    @staticmethod
    async def get_all(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, brand: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = "desc") -> List[DeviceInfo]:
        """
        Lấy danh sách thông tin máy.
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm
            brand: Hãng sản xuất
            sort_by: Trường sắp xếp
            sort_order: Thứ tự sắp xếp
            
        Returns:
            Danh sách các đối tượng DeviceInfo
        """
        query = select(DeviceInfo).options(selectinload(DeviceInfo.materials))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))

        if sort_by and hasattr(DeviceInfo, sort_by):
            column = getattr(DeviceInfo, sort_by)
            query = query.order_by(column.asc() if sort_order == "asc" else column.desc())
        else:
            query = query.order_by(DeviceInfo.updated_at.desc(), DeviceInfo.created_at.desc())

        query = query.offset(skip).limit(limit)
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_by_user_id(db: AsyncSession, user_id: uuid.UUID, skip: int = 0, limit: int = 100, search: Optional[str] = None) -> List[DeviceInfo]:
        """
        Lấy danh sách thông tin máy của một người dùng cụ thể.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm
            
        Returns:
            Danh sách các đối tượng DeviceInfo
        """
        query = select(DeviceInfo).where(DeviceInfo.user_id == user_id)
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        
        query = query.order_by(DeviceInfo.updated_at.desc(), DeviceInfo.created_at.desc()).offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_default_devices(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, brand: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = "desc") -> List[DeviceInfo]:
        """
        Lấy danh sách thông tin máy mặc định (không có user_id).
        
        Args:
            db: Database session
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm
            brand: Hãng sản xuất
            sort_by: Trường sắp xếp
            sort_order: Thứ tự sắp xếp
            
        Returns:
            Danh sách các đối tượng DeviceInfo
        """
        query = select(DeviceInfo).options(selectinload(DeviceInfo.materials)).where(DeviceInfo.user_id.is_(None))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))

        if sort_by and hasattr(DeviceInfo, sort_by):
            column = getattr(DeviceInfo, sort_by)
            query = query.order_by(column.asc() if sort_order == "asc" else column.desc())
        else:
            query = query.order_by(DeviceInfo.updated_at.desc())

        query = query.offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def get_all_for_user(db: AsyncSession, user_id: uuid.UUID, skip: int = 0, limit: int = 100, search: Optional[str] = None, brand: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = "desc") -> List[DeviceInfo]:
        """
        Lấy danh sách thông tin máy mặc định và của người dùng cụ thể.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            skip: Số lượng bản ghi bỏ qua
            limit: Số lượng bản ghi tối đa trả về
            search: Từ khóa tìm kiếm
            brand: Hãng sản xuất
            sort_by: Trường sắp xếp
            sort_order: Thứ tự sắp xếp
            
        Returns:
            Danh sách các đối tượng DeviceInfo
        """
        query = select(DeviceInfo).options(selectinload(DeviceInfo.materials)).where(or_(DeviceInfo.user_id.is_(None), DeviceInfo.user_id == user_id))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))

        if sort_by and hasattr(DeviceInfo, sort_by):
            column = getattr(DeviceInfo, sort_by)
            query = query.order_by(column.asc() if sort_order == "asc" else column.desc())
        else:
            query = query.order_by(DeviceInfo.updated_at.desc(), DeviceInfo.created_at.desc())

        query = query.offset(skip).limit(limit)
        
        result = await db.execute(query)
        return result.scalars().all()
        
    @staticmethod
    async def count_all(db: AsyncSession, search: Optional[str] = None, brand: Optional[str] = None) -> int:
        """
        Đếm tổng số thông tin máy.
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm
            
        Returns:
            Tổng số bản ghi
        """
        query = select(func.count(DeviceInfo.id))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))
        result = await db.execute(query)
        return result.scalar_one()
    
    @staticmethod
    async def count_by_user_id(db: AsyncSession, user_id: uuid.UUID, search: Optional[str] = None) -> int:
        """
        Đếm tổng số thông tin máy của một người dùng cụ thể.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            search: Từ khóa tìm kiếm
            
        Returns:
            Tổng số bản ghi
        """
        query = select(func.count(DeviceInfo.id)).select_from(DeviceInfo).where(DeviceInfo.user_id == user_id)
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        result = await db.execute(query)
        return result.scalar_one()
    
    @staticmethod
    async def count_default_devices(db: AsyncSession, search: Optional[str] = None, brand: Optional[str] = None) -> int:
        """
        Đếm tổng số thông tin máy mặc định (không có user_id).
        
        Args:
            db: Database session
            search: Từ khóa tìm kiếm
            
        Returns:
            Tổng số bản ghi
        """
        query = select(func.count(DeviceInfo.id)).where(DeviceInfo.user_id.is_(None))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))
        return await db.scalar(query)
    
    @staticmethod
    async def count_for_user(db: AsyncSession, user_id: uuid.UUID, search: Optional[str] = None, brand: Optional[str] = None) -> int:
        """
        Đếm tổng số thông tin máy của một người dùng (bao gồm cả thiết bị mặc định).
        """
        query = select(func.count(DeviceInfo.id)).where(or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None)))
        if search:
            query = query.where(DeviceInfo.model.ilike(f"%{search}%"))
        if brand:
            query = query.where(DeviceInfo.brand.ilike(f"%{brand}%"))
        return await db.scalar(query)

    @staticmethod
    async def update_materials(db: AsyncSession, device_info_id: uuid.UUID, material_ids: List[uuid.UUID]) -> bool:
        """
        Cập nhật danh sách vật liệu cho một thiết bị.
        
        Args:
            db: Database session
            device_info_id: ID của thiết bị
            material_ids: Danh sách ID của các vật liệu
            
        Returns:
            True nếu cập nhật thành công, False nếu không tìm thấy thiết bị
        """
        db_device_info = await DeviceInfoRepository.get_by_id(db, device_info_id)
        
        if not db_device_info:
            return False
        
        # Xử lý vật liệu
        if material_ids:
            materials = await db.execute(select(Material).where(Material.id.in_(material_ids)))
            db_device_info.materials = materials.scalars().all()
        else:
            db_device_info.materials = []
        
        # Lưu thay đổi
        await db.commit()
        return True
    
    @staticmethod
    async def get_distinct_brands(db: AsyncSession, user_id: Optional[uuid.UUID] = None) -> List[str]:
        """
        Lấy danh sách các hãng sản xuất khác nhau.
        
        Args:
            db: Database session
            user_id: ID của người dùng
            
        Returns:
            Danh sách các hãng sản xuất
        """
        query = select(distinct(DeviceInfo.brand)).where(DeviceInfo.brand.isnot(None))
        if user_id:
            query = query.where(or_(DeviceInfo.user_id == user_id, DeviceInfo.user_id.is_(None)))
        query = query.order_by(DeviceInfo.brand)
        result = await db.execute(query)
        return result.scalars().all()
    
    @staticmethod
    async def delete_device_material_associations(db: AsyncSession, device_info_ids: List[uuid.UUID]) -> None:
        """
        Xóa tất cả các liên kết device-material cho danh sách device_info_ids.
        
        Args:
            db: Database session
            device_info_ids: Danh sách ID của các thiết bị
        """
        if not device_info_ids:
            return
        
        stmt = delete(device_material_association).where(
            device_material_association.c.device_info_id.in_(device_info_ids)
        )
        await db.execute(stmt)
        # Không commit ở đây vì sẽ được commit trong method gọi