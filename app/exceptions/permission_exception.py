from fastapi import HTTPException, status

class PermissionException(HTTPException):
    def __init__(self, detail: str = "Bạn không có quyền thực hiện hành động này"):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)
