from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime
import uuid
from .material_dto import MaterialInfo
from .device_storage_dto import DeviceStorageRead


class DeviceInfoCreate(BaseModel):
    """
    DTO cho request tạo thông tin máy mới.
    """
    model: str = Field(..., description="Tên model của thiết bị")
    brand: Optional[str] = Field(None, description="Thương hiệu của thiết bị")
    release_date: Optional[str] = Field(None, description="Ngày ra mắt của thiết bị")
    screen: Optional[str] = Field(None, description="Thông tin màn hình của thiết bị")
    chip_ram: Optional[str] = Field(None, description="Thông tin chip và RAM của thiết bị")
    camera: Optional[str] = Field(None, description="Thông tin camera của thiết bị")
    battery: Optional[str] = Field(None, description="Thông tin pin của thiết bị (mAh)")
    connectivity_os: Optional[str] = Field(None, description="Thông tin kết nối và hệ điều hành")
    color_english: Optional[str] = Field(None, description="Tên màu bằng tiếng Anh")
    dimensions_weight: Optional[str] = Field(None, description="Kích thước và trọng lượng")
    sensors_health_features: Optional[str] = Field(None, description="Cảm biến & Tính năng sức khỏe")
    warranty: Optional[str] = Field(None, description="Thông tin bảo hành")
    user_id: Optional[uuid.UUID] = Field(None, description="ID của người dùng tạo thiết bị (chỉ admin truyền)")
    material_ids: Optional[List[uuid.UUID]] = Field(None, description="Danh sách ID của các vật liệu")


class DeviceInfoUpdate(BaseModel):
    """
    DTO cho request cập nhật thông tin máy.
    """
    model: Optional[str] = Field(None, description="Tên model của thiết bị")
    brand: Optional[str] = Field(None, description="Thương hiệu của thiết bị")
    release_date: Optional[str] = Field(None, description="Ngày ra mắt của thiết bị")
    screen: Optional[str] = Field(None, description="Thông tin màn hình của thiết bị")
    chip_ram: Optional[str] = Field(None, description="Thông tin chip và RAM của thiết bị")
    camera: Optional[str] = Field(None, description="Thông tin camera của thiết bị")
    battery: Optional[str] = Field(None, description="Thông tin pin của thiết bị (mAh)")
    connectivity_os: Optional[str] = Field(None, description="Thông tin kết nối và hệ điều hành")
    color_english: Optional[str] = Field(None, description="Tên màu bằng tiếng Anh")
    dimensions_weight: Optional[str] = Field(None, description="Kích thước và trọng lượng")
    sensors_health_features: Optional[str] = Field(None, description="Cảm biến & Tính năng sức khỏe")
    warranty: Optional[str] = Field(None, description="Thông tin bảo hành")
    material_ids: Optional[List[uuid.UUID]] = Field(None, description="Danh sách ID của các vật liệu")


class DeviceInfoRead(BaseModel):
    """
    DTO cho response trả về thông tin máy.
    """
    id: uuid.UUID
    model: str
    brand: Optional[str] = None
    release_date: Optional[str] = None
    screen: Optional[str] = None
    chip_ram: Optional[str] = None
    camera: Optional[str] = None
    battery: Optional[str] = None
    connectivity_os: Optional[str] = None
    color_english: Optional[str] = None
    dimensions_weight: Optional[str] = None
    sensors_health_features: Optional[str] = None
    warranty: Optional[str] = None
    user_id: Optional[uuid.UUID] = None
    materials: Optional[List[MaterialInfo]] = []
    device_storages: Optional[List[DeviceStorageRead]] = []
    device_colors: Optional[List[Any]] = []  # Sử dụng Any để tránh forward reference
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class DeviceInfoReadSimple(BaseModel):
    """
    DTO đơn giản cho export, không có relationships phức tạp.
    """
    id: uuid.UUID
    model: str
    brand: Optional[str] = None
    release_date: Optional[str] = None
    screen: Optional[str] = None
    chip_ram: Optional[str] = None
    camera: Optional[str] = None
    battery: Optional[str] = None
    connectivity_os: Optional[str] = None
    color_english: Optional[str] = None
    dimensions_weight: Optional[str] = None
    sensors_health_features: Optional[str] = None
    warranty: Optional[str] = None
    user_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


# Rebuild models để resolve forward references trong Pydantic v2
DeviceInfoRead.model_rebuild()
DeviceInfoReadSimple.model_rebuild()