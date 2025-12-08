from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid
import json

from app.models.product_component import ProductComponent
from app.dto.product_component_dto import ProductComponentCreate, ProductComponentUpdate, ProductComponentRead
from app.repositories.product_component_repository import ProductComponentRepository
from app.exceptions.api_exceptions import BadRequestException, NotFoundException
from app.services.chatbot_sync_service import ChatbotSyncService
from app.utils.soft_delete import SoftDeleteMixin


class ProductComponentService:
    """
    Service xử lý các thao tác liên quan đến thành phần sản phẩm.
    """
    
    @staticmethod
    async def create_product_component(db: AsyncSession, data: ProductComponentCreate) -> ProductComponentRead:
        """Tạo một thành phần sản phẩm mới."""
        # Nếu không có mã sản phẩm, tạo mã tự động
        if not data.product_code:
            data.product_code = await ProductComponentService.generate_product_code(db, data.user_id)
        
        # Cho phép trùng product_code theo user, bỏ kiểm tra trùng lặp
        
        # Tạo thành phần sản phẩm mới
        new_product_component = await ProductComponentRepository.create(db, data)
        
        # Chuyển đổi sang DTO để trả về
        # Properties are already stored as JSON string, no need to modify
        return ProductComponentRead(**new_product_component.__dict__)
    
    @staticmethod
    async def create_product_component_with_sync(db: AsyncSession, data: ProductComponentCreate, user) -> ProductComponentRead:
        """Tạo một thành phần sản phẩm mới (đồng bộ với ChatbotCustom được xử lý ở controller)."""
        # Tạo linh kiện
        new_component = await ProductComponentService.create_product_component(db, data)
        
        # Không cần đồng bộ ở đây vì controller đã có background task
        return new_component
    
    @staticmethod
    async def get_product_component(db: AsyncSession, product_component_id: uuid.UUID, user_id: uuid.UUID) -> ProductComponentRead:
        """Lấy thông tin thành phần sản phẩm theo ID, bắt buộc kèm user_id để kiểm tra sở hữu."""
        product_component = await ProductComponentRepository.get_by_id_for_user(db, product_component_id, user_id)
        if not product_component:
            raise NotFoundException("Không tìm thấy thành phần sản phẩm")
        # Properties are already stored as JSON string, no need to modify
        return ProductComponentRead(**product_component.__dict__)
    
    @staticmethod
    async def get_product_component_by_code(db: AsyncSession, product_code: str, user_id) -> Optional[ProductComponentRead]:
        """Lấy thông tin thành phần sản phẩm theo mã sản phẩm và user_id."""
        # Handle both string and UUID types for user_id
        if isinstance(user_id, str):
            user_uuid = user_id
        elif isinstance(user_id, uuid.UUID):
            user_uuid = user_id
        else:
            user_uuid = user_id
            
        product_component = await ProductComponentRepository.get_by_product_code_and_user_id(db, product_code, user_uuid)
        if not product_component:
            return None
        return ProductComponentRead(**product_component.__dict__)
    
    @staticmethod
    async def update_product_component(db: AsyncSession, product_component_id: uuid.UUID, data: ProductComponentUpdate, user_id: uuid.UUID) -> ProductComponentRead:
        """Cập nhật thông tin thành phần sản phẩm."""
        # Kiểm tra xem thành phần sản phẩm có tồn tại không
        db_product_component = await ProductComponentRepository.get_by_id_for_user(db, product_component_id, user_id)
        if not db_product_component:
            raise NotFoundException("Không tìm thấy thành phần sản phẩm")
        
        # Cho phép trùng product_code theo user khi cập nhật, bỏ kiểm tra trùng lặp
        
        # Cập nhật thành phần sản phẩm
        updated_product_component = await ProductComponentRepository.update(db, product_component_id, data)
        if not updated_product_component:
            raise NotFoundException("Không tìm thấy thành phần sản phẩm")
        
        # Chuyển đổi sang DTO để trả về
        # Properties are already stored as JSON string, no need to modify
        return ProductComponentRead(**updated_product_component.__dict__)
    
    @staticmethod
    async def update_product_component_with_sync(db: AsyncSession, product_component_id: uuid.UUID, data: ProductComponentUpdate, user) -> ProductComponentRead:
        """Cập nhật thành phần sản phẩm (đồng bộ với ChatbotCustom được xử lý ở controller)."""
        # Cập nhật linh kiện
        updated_component = await ProductComponentService.update_product_component(db, product_component_id, data, user_id=user.id)
        
        # Không cần đồng bộ ở đây vì controller đã có background task
        return updated_component
    
    @staticmethod
    async def get_all_product_components(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, sort_by: Optional[str] = None, sort_order: Optional[str] = None, user_id: Optional[uuid.UUID] = None, filters: Optional[dict] = None) -> dict:
        """Lấy danh sách tất cả thành phần sản phẩm với phân trang, tìm kiếm và filter."""
        product_components, total = await ProductComponentRepository.get_all(db, skip, limit, search, sort_by, sort_order, user_id, filters)
        
        # Tính toán thông tin phân trang
        total_pages = (total + limit - 1) // limit  # Math.ceil(total / limit)
        current_page = (skip // limit) + 1
        
        result = []
        for pc in product_components:
            # Properties are already stored as JSON string, no need to modify
            result.append(ProductComponentRead(**pc.__dict__))
        
        return {
            "data": result,
            "total": total,
            "page": current_page,
            "limit": limit,
            "total_pages": total_pages,
            "has_next": current_page < total_pages,
            "has_prev": current_page > 1
        }
    
    @staticmethod
    async def delete_product_component(db: AsyncSession, product_component_id: uuid.UUID, user_id: uuid.UUID) -> bool:
        """Xóa mềm thành phần sản phẩm."""
        # Kiểm tra xem thành phần sản phẩm có tồn tại không
        product_component = await ProductComponentRepository.get_by_id_for_user(db, product_component_id, user_id)
        if not product_component:
            raise NotFoundException("Không tìm thấy thành phần sản phẩm")
        
        # Thực hiện soft delete thay vì hard delete
        return await SoftDeleteMixin.soft_delete(db, ProductComponent, product_component_id, days_to_purge=1)
    
    @staticmethod
    async def delete_product_component_with_sync(db: AsyncSession, product_component_id: uuid.UUID, user) -> bool:
        """Xóa thành phần sản phẩm (đồng bộ với ChatbotCustom được xử lý ở controller)."""
        # Lấy thông tin linh kiện trước khi xóa để đồng bộ
        component = await ProductComponentService.get_product_component(db, product_component_id, user_id=user.id)
        if not component:
            raise NotFoundException("Không tìm thấy thành phần sản phẩm")
        
        # Xóa mềm linh kiện
        success = await ProductComponentService.delete_product_component(db, product_component_id, user_id=user.id)
        
        # Không cần đồng bộ ở đây vì controller đã có background task
        return success
    
    @staticmethod
    async def restore_product_component(db: AsyncSession, product_component_id: uuid.UUID) -> bool:
        """Khôi phục thành phần sản phẩm đã bị xóa mềm."""
        restored = await SoftDeleteMixin.restore(db, ProductComponent, product_component_id)
        if not restored:
            raise NotFoundException("Không thể khôi phục thành phần sản phẩm")
        return restored
    
    @staticmethod
    async def bulk_delete_product_components_with_sync(db: AsyncSession, product_component_ids: list[uuid.UUID], user) -> int:
        """Xóa mềm hàng loạt thành phần sản phẩm (đồng bộ với ChatbotCustom được xử lý ở controller)."""
        # Thực hiện soft delete cho từng linh kiện
        deleted_count = 0
        for component_id in product_component_ids:
            success = await SoftDeleteMixin.soft_delete(db, ProductComponent, component_id, days_to_purge=1)
            if success:
                deleted_count += 1
        
        # Không cần đồng bộ ở đây vì controller đã có background task
        return deleted_count
    
    @staticmethod
    async def get_deleted_today(db: AsyncSession, user_id: uuid.UUID) -> List[ProductComponentRead]:
        """Lấy danh sách thành phần sản phẩm đã xóa trong ngày hôm nay cho user."""
        from app.repositories.product_component_repository import ProductComponentRepository
        deleted_components = await ProductComponentRepository.get_deleted_today_by_user_id(db, user_id)
        return [ProductComponentRead(**pc.__dict__) for pc in deleted_components]

    @staticmethod
    async def restore_all_deleted_today(db: AsyncSession, user_id: uuid.UUID) -> dict:
        """Khôi phục tất cả thành phần sản phẩm đã xóa trong ngày hôm nay cho user."""
        from app.repositories.product_component_repository import ProductComponentRepository
        deleted_components = await ProductComponentRepository.get_deleted_today_by_user_id(db, user_id)
        restored = 0
        restored_ids: list[uuid.UUID] = []
        for pc in deleted_components:
            ok = await SoftDeleteMixin.restore(db, ProductComponent, pc.id)
            if ok:
                restored += 1
                restored_ids.append(pc.id)
        return {"restored_count": restored, "restored_ids": restored_ids, "message": f"Đã khôi phục {restored} linh kiện"}

    @staticmethod
    async def delete_all_product_components(db: AsyncSession, user_id: uuid.UUID) -> int:
        """Xóa mềm tất cả thành phần sản phẩm của một người dùng."""
        # Lấy tất cả product components của user trước khi xóa
        from sqlalchemy import select
        result = await db.execute(
            select(ProductComponent.id).where(
                ProductComponent.user_id == user_id,
                ProductComponent.trashed_at.is_(None)  # Chỉ lấy những cái chưa bị xóa
            )
        )
        component_ids = [row[0] for row in result.fetchall()]
        
        # Thực hiện soft delete cho từng component
        deleted_count = 0
        for component_id in component_ids:
            success = await SoftDeleteMixin.soft_delete(db, ProductComponent, component_id, days_to_purge=1)
            if success:
                deleted_count += 1
        
        return deleted_count
    
    @staticmethod
    async def generate_product_code(db: AsyncSession, user_id: uuid.UUID) -> str:
        """Tạo mã sản phẩm tự động theo định dạng LK000001, LK000002, ... cho từng người dùng."""
        # Đếm số lượng sản phẩm của người dùng
        from sqlalchemy import select, func
        from app.models.product_component import ProductComponent
        
        # Lấy số lượng sản phẩm của người dùng
        result = await db.execute(
            select(func.count(ProductComponent.id))
            .where(ProductComponent.user_id == user_id)
        )
        count = result.scalar() or 0
        
        # Tạo mã sản phẩm mới
        new_code = f"LK{count + 1:06d}"
        
        # Kiểm tra xem mã đã tồn tại cho user này chưa (phòng trường hợp có xóa sản phẩm)
        existing_product = await ProductComponentRepository.get_by_product_code_and_user_id(db, new_code, user_id)
        if existing_product:
            # Nếu đã tồn tại, tìm mã tiếp theo chưa sử dụng cho user này
            i = count + 1
            while True:
                new_code = f"LK{i:06d}"
                existing_product = await ProductComponentRepository.get_by_product_code_and_user_id(db, new_code, user_id)
                if not existing_product:
                    break
                i += 1
        
        return new_code

    @staticmethod
    async def get_filter_options(db: AsyncSession, user_id: uuid.UUID) -> dict:
        """Lấy các tùy chọn bộ lọc cho thành phần sản phẩm."""
        # Lấy danh sách danh mục không trùng lặp
        categories = await ProductComponentRepository.get_distinct_categories(db, user_id)
        
        # Lấy danh sách thương hiệu không trùng lặp
        trademarks = await ProductComponentRepository.get_distinct_trademarks(db, user_id)
        
        # Lấy danh sách thuộc tính và giá trị
        properties_data = await ProductComponentRepository.get_distinct_properties(db, user_id)
        
        # Xử lý dữ liệu thuộc tính
        property_keys = []
        property_values: dict = {}
        
        for prop_data in properties_data:
            try:
                if isinstance(prop_data, str):
                    parsed_props = json.loads(prop_data)
                    if isinstance(parsed_props, list):
                        for prop in parsed_props:
                            if isinstance(prop, dict) and 'key' in prop:
                                key = prop['key']
                                if key not in property_keys:
                                    property_keys.append(key)
                                    property_values[key] = []
                                
                                if 'values' in prop and isinstance(prop['values'], list):
                                    for value in prop['values']:
                                        if value not in property_values[key]:
                                            property_values[key].append(value)
            except (json.JSONDecodeError, TypeError):
                continue
        
        return {
            "categories": categories,
            "trademarks": trademarks,
            "property_keys": property_keys,
            "property_values": property_values
        }
