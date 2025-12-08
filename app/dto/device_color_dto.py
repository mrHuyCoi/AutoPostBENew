from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime
import uuid

from app.dto.color_dto import ColorRead


class DeviceColorCreate(BaseModel):
    """
    DTO cho request tạo liên kết giữa thiết bị và màu sắc.
    """
    device_info_id: uuid.UUID = Field(..., description="ID của thông tin máy")
    color_id: uuid.UUID = Field(..., description="ID của màu sắc")


class DeviceColorRead(BaseModel):
    """
    DTO cho response trả về thông tin liên kết giữa thiết bị và màu sắc.
    """
    id: uuid.UUID
    device_info_id: uuid.UUID
    color_id: uuid.UUID
    user_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DeviceColorWithColorRead(BaseModel):
    """
    DTO cho response trả về thông tin liên kết giữa thiết bị và màu sắc kèm thông tin màu sắc và model thiết bị.
    """
    id: uuid.UUID
    device_info_id: uuid.UUID
    color_id: uuid.UUID
    user_id: Optional[uuid.UUID] = None
    device_info: Any  # Sử dụng Any để tránh forward reference
    color: ColorRead
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Rebuild models để resolve forward references trong Pydantic v2
DeviceColorRead.model_rebuild()
DeviceColorWithColorRead.model_rebuild()