from pydantic import BaseModel, Field
from typing import Optional
import uuid
from datetime import datetime


class CategoryCreate(BaseModel):
    """DTO cho request tạo danh mục mới."""
    name: str = Field(..., description="Tên danh mục")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của danh mục cha")


class CategoryRead(BaseModel):
    """DTO cho response trả về thông tin danh mục."""
    id: uuid.UUID
    name: str
    parent_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class CategoryUpdate(BaseModel):
    """DTO cho request cập nhật thông tin danh mục."""
    name: Optional[str] = Field(None, description="Tên danh mục")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của danh mục cha")
    
    class Config:
        from_attributes = True


class CategoryCreate(BaseModel):
    """DTO cho request tạo danh mục mới."""
    name: str = Field(..., description="Tên danh mục")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của danh mục cha")


class CategoryRead(BaseModel):
    """DTO cho response trả về thông tin danh mục."""
    id: uuid.UUID
    name: str
    parent_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class CategoryUpdate(BaseModel):
    """DTO cho request cập nhật thông tin danh mục."""
    name: Optional[str] = Field(None, description="Tên danh mục")
    parent_id: Optional[uuid.UUID] = Field(None, description="ID của danh mục cha")
    
    class Config:
        from_attributes = True
