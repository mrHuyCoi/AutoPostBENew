from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class ColorCreate(BaseModel):
    """
    DTO cho request tạo màu sắc mới.
    """
    name: str = Field(..., description="Tên màu sắc")
    hex_code: Optional[str] = Field(None, description="Mã màu HEX")


class ColorUpdate(BaseModel):
    """
    DTO cho request cập nhật màu sắc.
    """
    name: Optional[str] = Field(None, description="Tên màu sắc")
    hex_code: Optional[str] = Field(None, description="Mã màu HEX")


class ColorRead(BaseModel):
    """
    DTO cho response trả về thông tin màu sắc.
    """
    id: uuid.UUID
    name: str
    hex_code: Optional[str] = None
    user_id: Optional[uuid.UUID] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True