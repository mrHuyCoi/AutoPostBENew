from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import traceback

from app.exceptions.base_exception import AppException
from app.logging.logger import log_error, generate_error_ref_id


def _build_payload_from_exception(
    request: Request,
    exc: Exception,
    *,
    default_action: str,
    status_code: int,
    message: str,
    error_code: str,
    error_ref_id: str,
):
    """Tạo payload log từ exception, merge với log_context nếu có."""

    base_payload = {}

    # Lấy context đã được gắn bởi decorator (nếu có)
    ctx = getattr(exc, "log_context", None)
    if isinstance(ctx, dict):
        base_payload.update(ctx)

    base_payload.setdefault("event.action", default_action)
    base_payload.setdefault("event.category", "error")
    base_payload.setdefault("source.layer", "middleware")
    base_payload.setdefault("source.function", "ErrorHandlerMiddleware.dispatch")

    stack_trace = "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)
    )

    base_payload.update(
        {
            "message": message,
            "error.type": exc.__class__.__name__,
            "error.code": error_code,
            "error.ref_id": error_ref_id,
            "http.status_code": status_code,
            "request.method": request.method,
            "request.path": request.url.path,
            "request.query": request.url.query,
            "error.stack_trace": stack_trace,
        }
    )

    return base_payload


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    Middleware để xử lý lỗi từ application
    """
    
    async def dispatch(self, request: Request, call_next):
        try:
            # Tiếp tục xử lý request
            return await call_next(request)
        except AppException as e:
            # Xử lý các exception tùy chỉnh từ ứng dụng
            error_ref_id = getattr(e, "error_ref_id", None) or generate_error_ref_id()

            payload = _build_payload_from_exception(
                request,
                e,
                default_action="app.exception",
                status_code=e.status_code,
                message=e.message,
                error_code=e.error_code,
                error_ref_id=error_ref_id,
            )

            log_error(payload)

            return JSONResponse(
                status_code=e.status_code,
                content={
                    "message": e.message,
                    "error_code": e.error_code,
                    "error_ref_id": error_ref_id,
                },
            )
        except Exception as e:
            # Xử lý các exception không xác định
            error_ref_id = getattr(e, "error_ref_id", None) or generate_error_ref_id()

            payload = _build_payload_from_exception(
                request,
                e,
                default_action="unhandled.exception",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                message="Unhandled exception",
                error_code="INTERNAL_SERVER_ERROR",
                error_ref_id=error_ref_id,
            )

            log_error(payload)

            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "message": "Internal server error",
                    "error_code": "INTERNAL_SERVER_ERROR",
                    "error_ref_id": error_ref_id,
                    "detail": str(e) if getattr(request.app, "debug", False) else None,
                },
            )
