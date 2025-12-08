from fastapi import status
from typing import Optional

class AppException(Exception):
    """
    Exception cơ sở cho tất cả các exception trong ứng dụng
    """
    def __init__(
        self, 
        message: str, 
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code: str = "INTERNAL_ERROR"
    ):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        super().__init__(self.message)

class NotFoundException(AppException):
    """Exception khi không tìm thấy resource"""
    def __init__(self, message: str = "Resource not found", error_code: str = "NOT_FOUND"):
        super().__init__(message=message, status_code=status.HTTP_404_NOT_FOUND, error_code=error_code)

class BadRequestException(AppException):
    """Exception khi request không hợp lệ"""
    def __init__(self, message: str = "Bad request", error_code: str = "BAD_REQUEST"):
        super().__init__(message=message, status_code=status.HTTP_400_BAD_REQUEST, error_code=error_code)

class UnauthorizedException(AppException):
    """Exception khi không có quyền truy cập"""
    def __init__(self, message: str = "Unauthorized", error_code: str = "UNAUTHORIZED"):
        super().__init__(message=message, status_code=status.HTTP_401_UNAUTHORIZED, error_code=error_code)

class ForbiddenException(AppException):
    """Exception khi bị cấm truy cập"""
    def __init__(self, message: str = "Forbidden", error_code: str = "FORBIDDEN"):
        super().__init__(message=message, status_code=status.HTTP_403_FORBIDDEN, error_code=error_code)

class ExternalAPIException(AppException):
    """Exception khi gọi API bên ngoài bị lỗi"""
    def __init__(self, message: str = "External API error", error_code: str = "EXTERNAL_API_ERROR"):
        super().__init__(message=message, status_code=status.HTTP_502_BAD_GATEWAY, error_code=error_code)
