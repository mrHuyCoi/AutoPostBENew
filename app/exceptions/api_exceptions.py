from fastapi import HTTPException, status


class BadRequestException(HTTPException):
    """
    Exception cho các lỗi yêu cầu không hợp lệ (400).
    """
    def __init__(self, detail: str = "Bad Request"):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail
        )


class UnauthorizedException(HTTPException):
    """
    Exception cho các lỗi không được phép truy cập (401).
    """
    def __init__(self, detail: str = "Unauthorized"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"}
        )


class ForbiddenException(HTTPException):
    """
    Exception cho các lỗi không có quyền truy cập (403).
    """
    def __init__(self, detail: str = "Forbidden"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail
        )


class NotFoundException(HTTPException):
    """
    Exception cho các lỗi không tìm thấy tài nguyên (404).
    """
    def __init__(self, detail: str = "Not Found"):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=detail
        )


class ConflictException(HTTPException):
    """
    Exception cho các lỗi xung đột (409).
    """
    def __init__(self, detail: str = "Conflict"):
        super().__init__(
            status_code=status.HTTP_409_CONFLICT,
            detail=detail
        )


class InternalServerException(HTTPException):
    """
    Exception cho các lỗi server nội bộ (500).
    """
    def __init__(self, detail: str = "Internal Server Error"):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail
        )
