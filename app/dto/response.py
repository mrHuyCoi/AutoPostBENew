

from typing import Any, Dict, Generic, TypeVar, Optional
from pydantic import BaseModel, Field

T = TypeVar("T")

class ResponseModel(BaseModel, Generic[T]):
    data: Optional[T] = None
    message: Optional[str] = None
    status_code: Optional[int] = 200
    total: Optional[int] = None
    totalPages: Optional[int] = None
    pagination: Optional[dict] = None

    @classmethod
    def success(cls, data: T = None, message: str = "Thành công", status_code: int = 200, total: int = None, totalPages: int = None, pagination: dict = None):
        return cls(data=data, message=message, status_code=status_code, total=total, totalPages=totalPages, pagination=pagination)

    @classmethod
    def error(cls, message: str = "Có lỗi xảy ra", status_code: int = 400):
        return cls(data=None, message=message, status_code=status_code)


class SuccessResponse(BaseModel):
    data: Dict[str, Any]
    message: str

  
class ErrorResponse(BaseModel):
    error_code: str
    message: str
    status_code: int
