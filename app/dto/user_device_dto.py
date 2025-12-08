from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
import uuid

from app.dto.device_info_dto import DeviceInfoRead
from app.dto.color_dto import ColorRead
from app.dto.storage_dto import StorageRead
from app.dto.user_dto import UserRead


class UserDeviceCreate(BaseModel):
    """
    DTO cho request tạo thông tin thiết bị của người dùng mới.
    """
    user_id: uuid.UUID = Field(..., description="ID của người dùng")
    device_info_id: uuid.UUID = Field(..., description="ID của thông tin máy")
    color_id: Optional[uuid.UUID] = Field(None, description="ID của màu sắc")
    device_storage_id: Optional[uuid.UUID] = Field(None, description="ID của dung lượng")
    product_code: Optional[str] = Field(None, description="Mã sản phẩm (tự động tạo nếu không cung cấp)")
    warranty: Optional[str] = Field(None, description="Thông tin bảo hành")
    device_condition: str = Field(..., description="Tình trạng máy")
    device_type: str = Field(..., description="Loại máy (mới hay cũ)")
    battery_condition: Optional[str] = Field(None, description="Tình trạng pin")
    price: float = Field(..., description="Giá")
    wholesale_price: Optional[float] = Field(None, description="Giá bán buôn")
    inventory: int = Field(0, description="Tồn kho")
    notes: Optional[str] = Field(None, description="Ghi chú")
    
    @field_validator('device_storage_id', mode='before')
    @classmethod
    def validate_device_storage_id(cls, v):
        if v == '' or v is None:
            return None
        return v
    
    @field_validator('color_id', mode='before')
    @classmethod
    def validate_color_id(cls, v):
        if v == '' or v is None:
            return None
        return v


class UserDeviceUpdate(BaseModel):
    """
    DTO cho request cập nhật thông tin thiết bị của người dùng.
    """
    device_info_id: Optional[uuid.UUID] = Field(None, description="ID của thông tin máy")
    color_id: Optional[uuid.UUID] = Field(None, description="ID của màu sắc")
    device_storage_id: Optional[uuid.UUID] = Field(None, description="ID của dung lượng")
    product_code: Optional[str] = Field(None, description="Mã sản phẩm")
    warranty: Optional[str] = Field(None, description="Thông tin bảo hành")
    device_condition: Optional[str] = Field(None, description="Tình trạng máy")
    device_type: Optional[str] = Field(None, description="Loại máy (mới hay cũ)")
    battery_condition: Optional[str] = Field(None, description="Tình trạng pin")
    price: Optional[float] = Field(None, description="Giá")
    wholesale_price: Optional[float] = Field(None, description="Giá bán buôn")
    inventory: Optional[int] = Field(None, description="Tồn kho")
    notes: Optional[str] = Field(None, description="Ghi chú")


class UserDeviceRead(BaseModel):
    """
    DTO cho response trả về thông tin thiết bị của người dùng.
    """
    id: uuid.UUID
    user_id: uuid.UUID
    device_info_id: uuid.UUID
    color_id: Optional[uuid.UUID] = None
    device_storage_id: Optional[uuid.UUID] = None
    product_code: Optional[str] = None
    device_name: Optional[str] = None
    warranty: Optional[str] = None
    device_condition: str
    device_type: str
    battery_condition: Optional[str] = None
    price: float
    wholesale_price: Optional[float] = None
    inventory: int
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class UserDeviceDetailRead(BaseModel):
    """
    DTO cho response trả về thông tin chi tiết thiết bị của người dùng.
    """
    id: uuid.UUID
    user_id: uuid.UUID
    product_code: Optional[str] = None
    device_name: Optional[str] = None
    warranty: Optional[str] = None
    device_condition: str
    device_type: str
    battery_condition: Optional[str] = None
    price: float
    wholesale_price: Optional[float] = None
    inventory: int
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    device_info: Optional[DeviceInfoRead] = None
    color: Optional[ColorRead] = None
    device_storage: Optional[StorageRead] = None
    device_storage_id: Optional[uuid.UUID] = None
    
    class Config:
        from_attributes = True


class BulkDeleteRequest(BaseModel):
    """
    DTO cho request xóa nhiều thiết bị người dùng.
    """
    user_device_ids: List[uuid.UUID] = Field(..., description="Danh sách ID của các thiết bị cần xóa")