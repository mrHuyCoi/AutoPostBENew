from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid

class DeviceBrandBase(BaseModel):
    name: str = Field(..., description="Tên hãng điện thoại")
    warranty: Optional[str] = Field(None, description="Thời gian bảo hành theo hãng")
    user_id: Optional[uuid.UUID] = Field(None, description="ID của người dùng")

class DeviceBrandCreate(BaseModel):
    name: str = Field(..., description="Tên hãng điện thoại")
    warranty: Optional[str] = Field(None, description="Thời gian bảo hành theo hãng")
    user_id: Optional[uuid.UUID] = Field(None, description="ID của người dùng")

class DeviceBrandUpdate(BaseModel):
    name: Optional[str] = None
    warranty: Optional[str] = None

class DeviceBrandRead(DeviceBrandBase):
    id: uuid.UUID
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
