from pydantic import BaseModel
from typing import Optional, List
from uuid import UUID
from datetime import datetime

class DeviceInfoRead(BaseModel):
    id: UUID
    model: str
    brand: Optional[str]
    release_date: Optional[str]
    screen: Optional[str]
    chip_ram: Optional[str]
    camera: Optional[str]
    battery: Optional[str]
    connectivity_os: Optional[str]
    color_english: Optional[str]
    dimensions_weight: Optional[str]
    sensors_health_features: Optional[str]
    warranty: Optional[str]
    user_id: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True

class ColorRead(BaseModel):
    id: UUID
    name: str
    hex_code: str
    user_id: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True

class DeviceColorWithColorRead(BaseModel):
    id: UUID
    device_info: DeviceInfoRead
    color: ColorRead
    user_id: Optional[UUID]
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
