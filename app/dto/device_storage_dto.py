from pydantic import BaseModel, UUID4, Field
from typing import Optional
import uuid
from datetime import datetime


class DeviceStorageBase(BaseModel):
    """Base model cho DeviceStorage."""
    capacity: int = Field(..., description="Dung lượng của thiết bị (GB)")


class DeviceStorageCreate(DeviceStorageBase):
    """Model cho việc tạo mới DeviceStorage."""
    device_info_id: UUID4 = Field(..., description="ID của thiết bị")


class DeviceStorageUpdate(BaseModel):
    """Model cho việc cập nhật DeviceStorage."""
    capacity: Optional[int] = Field(None, description="Dung lượng của thiết bị (GB)")


class DeviceStorageResponse(DeviceStorageBase):
    """Model cho việc trả về thông tin DeviceStorage."""
    id: UUID4
    device_info_id: UUID4
    
    class Config:
        from_attributes = True


class DeviceStorageRead(DeviceStorageBase):
    """Model cho việc đọc thông tin DeviceStorage."""
    id: UUID4
    device_info_id: UUID4
    user_id: Optional[UUID4] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class DeviceStorageWithDeviceResponse(BaseModel):
    device_id: uuid.UUID
    device_model: str
    storage_id: uuid.UUID
    capacity: int