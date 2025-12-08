from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class StorageCreate(BaseModel):
    """
    DTO cho request tạo dung lượng mới.
    """
    capacity: int = Field(..., description="Dung lượng của thiết bị (GB)")


class StorageUpdate(BaseModel):
    """
    DTO cho request cập nhật dung lượng.
    """
    capacity: Optional[int] = Field(None, description="Dung lượng của thiết bị (GB)")


class StorageRead(BaseModel):
    """
    DTO cho response trả về thông tin dung lượng.
    """
    id: uuid.UUID
    capacity: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True