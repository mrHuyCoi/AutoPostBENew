from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import uuid

from app.models.service import Service
from app.dto.service_dto import ServiceCreate, ServiceUpdate, ServiceRead
from app.repositories.service_repository import ServiceRepository
from app.utils.soft_delete import SoftDeleteMixin

class ServiceService:
    @staticmethod
    async def create_service(db: AsyncSession, data: ServiceCreate, user_id: uuid.UUID) -> Service:
        
        # (Bạn có thể bật lại kiểm tra này nếu muốn)
        # existing = await ServiceRepository.get_by_name(db, data.name, user_id)
        # if existing:
        #     raise Exception(f"Dịch vụ '{data.name}' đã tồn tại")
        
        # SỬA 1: Cập nhật object Service() để khớp với DTO và Model mới
        service_to_create = Service(
            user_id=user_id,
            name=data.name,             # Model 'name' -> CSDL 'loai'
            thuonghieu=data.thuonghieu,
            description=data.description, # Model 'description' -> CSDL 'loaimay'
            mausac=data.mausac,
            price=data.price,           # Model 'price' -> CSDL 'gia'
            warranty=data.warranty,     # Model 'warranty' -> CSDL 'baohanh'
            note=data.note              # Model 'note' -> CSDL 'ghichu'
        )

        return await ServiceRepository.create(db, service_to_create)

    @staticmethod
    async def get_service(db: AsyncSession, service_id: uuid.UUID) -> Service:
        service = await ServiceRepository.get_by_id(db, service_id)
        if not service:
            raise Exception("Không tìm thấy dịch vụ")
        return service

    @staticmethod
    async def update_service(db: AsyncSession, service_id: uuid.UUID, data: ServiceUpdate) -> Service:
        # Hàm này đã đúng vì DTO 'ServiceUpdate' đã được sửa
        db_service = await ServiceRepository.get_by_id(db, service_id)
        if not db_service:
            raise Exception("Không tìm thấy dịch vụ để cập nhật")
        
        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_service, key, value)
            
        await db.commit()
        await db.refresh(db_service)
        return db_service

    @staticmethod
    async def delete_service(db: AsyncSession, service_id: uuid.UUID) -> bool:
        from app.models.brand import Brand
        from app.repositories.brand_repository import BrandRepository
        
        service = await ServiceRepository.get_by_id(db, service_id)
        if not service:
            raise Exception("Không tìm thấy dịch vụ để xóa")
        
        brands = await BrandRepository.get_all(db, service_id=service_id, limit=10000, user_id=service.user_id)
        
        for brand in brands:
            await SoftDeleteMixin.soft_delete(db, Brand, brand.id, days_to_purge=1)
        
        deleted = await SoftDeleteMixin.soft_delete(db, Service, service_id, days_to_purge=1)
        if not deleted:
            raise Exception("Không thể xóa dịch vụ")
        
        return deleted
    
    @staticmethod
    async def restore_service(db: AsyncSession, service_id: uuid.UUID) -> bool:
        restored = await SoftDeleteMixin.restore(db, Service, service_id)
        if not restored:
            raise Exception("Không thể khôi phục dịch vụ")
        return restored

    @staticmethod
    async def get_deleted_services_today(db: AsyncSession, user_id: uuid.UUID) -> List[dict]:
        from app.repositories.brand_repository import BrandRepository
        
        services = await ServiceRepository.get_deleted_today_by_user_id(db, user_id)
        result = []
        deleted_brands = await BrandRepository.get_deleted_today_by_user_id(db, user_id)
        
        for service in services:
            service_brands = [brand for brand in deleted_brands if brand.service_id == service.id]
            
            # SỬA 2: Cập nhật dict trả về để khớp với DTO 'DeletedServiceWithBrands' mới
            result.append({
                "id": str(service.id),
                "name": service.name,
                "description": getattr(service, 'description', None),
                "user_id": str(getattr(service, 'user_id', user_id)),
                "created_at": service.created_at,
                "trashed_at": service.trashed_at,
                
                # Thêm các trường mới
                "thuonghieu": getattr(service, 'thuonghieu', None),
                "mausac": getattr(service, 'mausac', None),
                "price": getattr(service, 'price', None),
                "warranty": getattr(service, 'warranty', None),
                "note": getattr(service, 'note', None),
                
                "brands": [
                    {
                        "id": str(brand.id),
                        "name": brand.name,
                        "service_code": brand.service_code,
                        "price": brand.price,
                        "device_brand": {"name": brand.device_brand.name} if brand.device_brand else None
                    }
                    for brand in service_brands
                ]
            })
        
        return result

    @staticmethod
    async def restore_all_deleted_services_today(db: AsyncSession, user_id: uuid.UUID) -> dict:
        from app.models.brand import Brand
        
        deleted_services = await ServiceRepository.get_deleted_today_by_user_id(db, user_id)
        
        if not deleted_services:
            return {"restored_count": 0, "message": "Không có dịch vụ nào để khôi phục"}
        
        restored_count = 0
        for service in deleted_services:
            service_restored = await SoftDeleteMixin.restore(db, Service, service.id)
            if service_restored:
                restored_count += 1
                
                from app.repositories.brand_repository import BrandRepository
                deleted_brands = await BrandRepository.get_deleted_today_by_user_id(db, user_id)
                
                service_brands = [b for b in deleted_brands if b.service_id == service.id]
                for brand in service_brands:
                    await SoftDeleteMixin.restore(db, Brand, brand.id)
        
        return {
            "restored_count": restored_count,
            "message": f"Đã khôi phục {restored_count} dịch vụ thành công"
        }

    @staticmethod
    async def get_all_services(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[Service]:
        return await ServiceRepository.get_all(db, skip, limit, search, user_id)

    @staticmethod
    async def count_services(db: AsyncSession, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> int:
        return await ServiceRepository.count_all(db, search, user_id)

    @staticmethod
    async def bulk_delete_services(db: AsyncSession, service_ids: List[uuid.UUID]) -> dict:
        """
        Bulk soft delete multiple services
        """
        results = {
            "success_count": 0,
            "error_count": 0,
            "errors": []
        }
        
        for service_id in service_ids:
            try: # <-- ĐÃ SỬA LỖI (thêm dấu :)
                # SỬA 3: Gọi 'delete_service' của class này để xóa brands con
                deleted = await cls.delete_service(db, service_id) 
                
                if deleted:
                    results["success_count"] += 1
                else:
                    results["error_count"] += 1
                    results["errors"].append(f"Không tìm thấy dịch vụ với ID: {service_id}")
            except Exception as e:
                results["error_count"] += 1
                results["errors"].append(f"Lỗi khi xóa dịch vụ {service_id}: {str(e)}")
        
        return results

    @staticmethod
    async def get_services_with_product_count(db: AsyncSession, skip: int = 0, limit: int = 100, search: Optional[str] = None, user_id: Optional[uuid.UUID] = None) -> List[dict]:
        return await ServiceRepository.get_all_with_product_count(db, skip, limit, search, user_id)