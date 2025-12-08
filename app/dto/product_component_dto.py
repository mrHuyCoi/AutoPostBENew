from pydantic import BaseModel, Field
from typing import Optional, List
import uuid
from datetime import datetime


class ProductComponentCreate(BaseModel):
    """DTO cho request tạo thành phần sản phẩm mới."""
    product_code: Optional[str] = Field(None, description="Mã sản phẩm, phải là duy nhất")
    product_name: str = Field(..., description="Tên sản phẩm")
    amount: float = Field(..., description="Số lượng")
    wholesale_price: Optional[float] = Field(None, description="Giá bán buôn")
    trademark: Optional[str] = Field(None, description="Thương hiệu")
    guarantee: Optional[str] = Field(None, description="Bảo hành")
    stock: int = Field(..., description="Tồn kho")
    description: Optional[str] = Field(None, description="Mô tả sản phẩm")
    product_photo: Optional[str] = Field(None, description="Đường dẫn ảnh sản phẩm")
    product_link: Optional[str] = Field(None, description="Đường dẫn liên kết sản phẩm")
    user_id: Optional[uuid.UUID] = Field(None, description="ID của người dùng sở hữu")
    category: Optional[str] = Field(None, description="Tên danh mục")
    properties: Optional[str] = Field(None, description="Thuộc tính dưới dạng chuỗi JSON")


class ProductComponentRead(BaseModel):
    """DTO cho response trả về thông tin thành phần sản phẩm."""
    id: uuid.UUID
    product_code: str
    product_name: str
    amount: float
    wholesale_price: Optional[float] = None
    trademark: Optional[str] = None
    guarantee: Optional[str] = None
    stock: int
    description: Optional[str] = None
    product_photo: Optional[str] = None
    product_link: Optional[str] = None
    user_id: uuid.UUID
    category: Optional[str] = None
    properties: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True


class ProductComponentUpdate(BaseModel):
    """DTO cho request cập nhật thông tin thành phần sản phẩm."""
    product_code: Optional[str] = Field(None, description="Mã sản phẩm, phải là duy nhất")
    product_name: Optional[str] = Field(None, description="Tên sản phẩm")
    amount: Optional[float] = Field(None, description="Số lượng")
    wholesale_price: Optional[float] = Field(None, description="Giá bán buôn")
    trademark: Optional[str] = Field(None, description="Thương hiệu")
    guarantee: Optional[str] = Field(None, description="Bảo hành")
    stock: Optional[int] = Field(None, description="Tồn kho")
    description: Optional[str] = Field(None, description="Mô tả sản phẩm")
    product_photo: Optional[str] = Field(None, description="Đường dẫn ảnh sản phẩm")
    product_link: Optional[str] = Field(None, description="Đường dẫn liên kết sản phẩm")
    category: Optional[str] = Field(None, description="Tên danh mục")
    properties: Optional[str] = Field(None, description="Thuộc tính dưới dạng chuỗi JSON")
    
    class Config:
        from_attributes = True


class PaginatedProductComponentResponse(BaseModel):
    """DTO cho response trả về danh sách thành phần sản phẩm có phân trang."""
    data: List[ProductComponentRead]
    total: int
    page: int
    limit: int
    total_pages: int
    has_next: bool
    has_prev: bool
