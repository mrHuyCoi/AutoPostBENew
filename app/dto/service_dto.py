from pydantic import BaseModel, Field
import uuid
from typing import Optional, List
from datetime import datetime

class ServiceCreate(BaseModel):
    name: str = Field(..., description="Tên dịch vụ (Map sang 'loai')")
    thuonghieu: Optional[str] = Field(None, description="Tên thương hiệu")
    description: Optional[str] = Field(None, description="Mô tả/Loại máy (Map sang 'loaimay')")
    mausac: Optional[str] = Field(None, description="Màu sắc")
    price: Optional[str] = Field(None, description="Giá (Map sang 'gia')")
    warranty: Optional[str] = Field(None, description="Bảo hành (Map sang 'baohanh')")
    note: Optional[str] = Field(None, description="Ghi chú (Map sang 'ghichu')")

class ServiceUpdate(BaseModel):
    name: Optional[str] = Field(None, description="Tên dịch vụ")
    thuonghieu: Optional[str] = Field(None, description="Tên thương hiệu")
    description: Optional[str] = Field(None, description="Mô tả/Loại máy")
    mausac: Optional[str] = Field(None, description="Màu sắc")
    price: Optional[str] = Field(None, description="Giá")
    warranty: Optional[str] = Field(None, description="Bảo hành")
    note: Optional[str] = Field(None, description="Ghi chú")

class ServiceRead(BaseModel):
    id: uuid.UUID
    name: str
    user_id: uuid.UUID
    thuonghieu: Optional[str] = None
    description: Optional[str] = None
    mausac: Optional[str] = None
    price: Optional[str] = None
    warranty: Optional[str] = None
    note: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    product_count: int = 0 
    class Config: 
        from_attributes = True

# --- Additional DTOs for deleted-today endpoint ---
class DeviceBrandBrief(BaseModel):
    name: Optional[str] = None
    class Config: 
        from_attributes = True

class BrandBrief(BaseModel):
    id: uuid.UUID
    name: Optional[str] = None
    service_code: Optional[str] = None
    price: Optional[str] = None 
    device_brand: Optional[DeviceBrandBrief] = None
    class Config:
        from_attributes = True

class DeletedServiceWithBrands(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str] = None 
    user_id: uuid.UUID
    created_at: Optional[datetime] = None
    trashed_at: Optional[datetime] = None
    brands: List[BrandBrief] = []
    thuonghieu: Optional[str] = None
    mausac: Optional[str] = None
    price: Optional[str] = None
    warranty: Optional[str] = None
    note: Optional[str] = None
    class Config:
        from_attributes = True


# SỬA: THÊM CLASS NÀY Ở CUỐI FILE (đây là class bị thiếu)
class BulkDeletePayload(BaseModel):
    ids: List[uuid.UUID]
