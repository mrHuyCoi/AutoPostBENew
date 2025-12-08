from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import uuid


class MaterialCreate(BaseModel):
    """
    DTO cho request tạo vật liệu mới.
    """
    name: str = Field(..., description="Tên vật liệu")
    description: Optional[str] = Field(None, description="Mô tả chi tiết về vật liệu")
    user_id: Optional[uuid.UUID] = Field(None, description="ID của người dùng tạo vật liệu (chỉ admin truyền)")


class MaterialUpdate(BaseModel):
    """
    DTO cho request cập nhật vật liệu.
    """
    name: Optional[str] = Field(None, description="Tên vật liệu")
    description: Optional[str] = Field(None, description="Mô tả chi tiết về vật liệu")


class MaterialRead(BaseModel):
    """
    DTO cho response trả về thông tin vật liệu.
    """
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    user_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class MaterialInfo(BaseModel):
    """
    DTO đơn giản cho thông tin vật liệu trong các response.
    """
    id: uuid.UUID
    name: str
    
    class Config:
        from_attributes = True
