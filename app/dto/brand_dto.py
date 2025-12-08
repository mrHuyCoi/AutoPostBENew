from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uuid

# Simple DTOs to avoid circular imports
class ServiceReadSimple(BaseModel):
    id: uuid.UUID
    name: str

    class Config:
        from_attributes = True

class DeviceBrandReadSimple(BaseModel):
    id: uuid.UUID
    name: str

    class Config:
        from_attributes = True

class BrandBase(BaseModel):
    name: str = Field(..., description="Tên thương hiệu")
    warranty: str = Field(..., description="Thông tin bảo hành")
    note: Optional[str] = Field(None, description="Ghi chú")
    service_id: uuid.UUID = Field(..., description="ID của dịch vụ liên quan")
    device_brand_id: Optional[uuid.UUID] = Field(None, description="ID của hãng điện thoại")
    price: Optional[str] = Field(None, description="Giá bán lẻ")
    wholesale_price: Optional[str] = Field(None, description="Giá bán buôn")
    device_type: Optional[str] = Field(None, description="Tên loại máy")
    color: Optional[str] = Field(None, description="Màu sắc của máy")

class BrandCreate(BaseModel):
    name: Optional[str] = None
    warranty: Optional[str] = None
    note: Optional[str] = None
    service_id: uuid.UUID
    device_brand_id: Optional[uuid.UUID] = None
    price: Optional[str] = None
    wholesale_price: Optional[str] = None
    device_type: Optional[str] = None
    color: Optional[str] = None

class BrandUpdate(BaseModel):
    name: Optional[str] = None
    warranty: Optional[str] = None
    note: Optional[str] = None
    device_brand_id: Optional[uuid.UUID] = None
    price: Optional[str] = None
    wholesale_price: Optional[str] = None
    device_type: Optional[str] = None
    color: Optional[str] = None
    service_code: Optional[str] = None

class BrandRead(BaseModel):
    id: uuid.UUID
    service_code: str
    name: str
    warranty: str
    note: Optional[str]
    service_id: uuid.UUID
    device_brand_id: Optional[uuid.UUID]
    device_type: Optional[str]
    color: Optional[str]
    price: Optional[str]
    wholesale_price: Optional[str]
    
    # SỬA: Cho phép 2 trường này được None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    service: Optional[ServiceReadSimple] = None
    device_brand: Optional[DeviceBrandReadSimple] = None

    class Config:
        from_attributes = True

# (Các class DTO khác giữ nguyên)
class BulkDeletePayload(BaseModel):
    ids: List[uuid.UUID]
