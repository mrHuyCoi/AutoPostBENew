from pydantic import BaseModel, Field
from typing import Optional, List
import uuid
from datetime import datetime


class PropertyCreate(BaseModel):
    """DTO cho request tạo thuộc tính mới."""
    key: str = Field(..., description="Khóa của thuộc tính")
    values: Optional[List[str]] = Field(None, description="Các giá trị của thuộc tính")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của thuộc tính cha")


class PropertyRead(BaseModel):
    """DTO cho response trả về thông tin thuộc tính."""
    id: uuid.UUID
    key: str
    values: Optional[List[str]] = None
    parent_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class PropertyUpdate(BaseModel):
    """DTO cho request cập nhật thông tin thuộc tính."""
    key: Optional[str] = Field(None, description="Khóa của thuộc tính")
    values: Optional[List[str]] = Field(None, description="Các giá trị của thuộc tính")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của thuộc tính cha")
    
    class Config:
        from_attributes = True
