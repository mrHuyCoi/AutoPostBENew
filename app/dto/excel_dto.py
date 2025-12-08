from pydantic import BaseModel, Field
from typing import List, Optional
import uuid


class UserDeviceExcelImport(BaseModel):
    """
    DTO cho dữ liệu import thiết bị người dùng từ Excel.
    """
    user_id: uuid.UUID = Field(..., description="ID của người dùng")
    device_info_id: uuid.UUID = Field(..., description="ID của thông tin máy")
    color_id: uuid.UUID = Field(..., description="ID của màu sắc")
    device_storage_id: uuid.UUID = Field(..., description="ID của dung lượng")
    warranty: Optional[str] = Field(None, description="Thông tin bảo hành")
    device_condition: str = Field(..., description="Tình trạng máy")
    device_type: str = Field(..., description="Loại máy (mới hay cũ)")
    battery_condition: Optional[str] = Field(None, description="Tình trạng pin")
    price: float = Field(..., description="Giá")
    inventory: int = Field(0, description="Tồn kho")
    notes: Optional[str] = Field(None, description="Ghi chú")


class ImportResult(BaseModel):
    """
    DTO cho kết quả import từ Excel.
    """
    total: int = Field(..., description="Tổng số bản ghi")
    success: int = Field(..., description="Số bản ghi thành công")
    error: int = Field(..., description="Số bản ghi lỗi")
    errors: List[str] = Field(default_factory=list, description="Danh sách lỗi")
    updated_count: int = Field(0, description="Số bản ghi đã cập nhật")
    created_count: int = Field(0, description="Số bản ghi đã tạo mới")