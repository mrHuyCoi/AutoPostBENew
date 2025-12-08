import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger
from asgi_correlation_id import correlation_id


SERVICE_NAME = os.getenv("SERVICE_NAME", "dangbaitudong-api")
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.getenv("APP_LOG_FILE", None)  # Override full path if needed

_configured = False


def _configure_logger() -> None:
    global _configured
    if _configured:
        return

    logger.remove()

    # Determine log file path
    if LOG_FILE:
        log_path = Path(LOG_FILE)
    else:
        log_path = Path(LOG_DIR) / "app.log"
    
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Console output (for dev/debugging)
    logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format="{message}",
        backtrace=False,
        diagnose=False,
    )

    # File output (for Filebeat collection)
    logger.add(
        log_path,
        level=LOG_LEVEL,
        format="{message}",
        backtrace=False,
        diagnose=False,
        rotation="50 MB",
        retention="5 days",
        compression="gz",
    )

    _configured = True


def get_trace_id() -> Optional[str]:
    """Lấy trace id hiện tại từ asgi-correlation-id (nếu có)."""
    try:
        return correlation_id.get()  # type: ignore[no-any-return]
    except LookupError:
        return None


def generate_error_ref_id() -> str:
    """Sinh mã lỗi ngắn, dễ copy (dùng để tra cứu log)."""
    return uuid.uuid4().hex[:8].upper()


def _log_json(payload: Dict[str, Any], level: str = "INFO") -> None:
    """Ghi một bản ghi log dạng JSON với đầy đủ context."""
    _configure_logger()

    if "@timestamp" not in payload:
        payload["@timestamp"] = datetime.now(timezone.utc).isoformat()

    payload["log.level"] = level.upper()
    payload.setdefault("service.name", SERVICE_NAME)

    trace_id = get_trace_id()
    if trace_id and "trace.id" not in payload:
        payload["trace.id"] = trace_id

    try:
        log_line = json.dumps(payload, ensure_ascii=False, default=str)
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(log_line)
    except Exception:
        logger.error("Failed to serialize log payload")


def log_error(payload: Dict[str, Any]) -> None:
    """Ghi một bản ghi log lỗi dạng JSON với đầy đủ context."""
    _log_json(payload, level="ERROR")


def log_info(payload: Dict[str, Any]) -> None:
    """Ghi một bản ghi log info dạng JSON."""
    _log_json(payload, level="INFO")


def log_warning(payload: Dict[str, Any]) -> None:
    """Ghi một bản ghi log warning dạng JSON."""
    _log_json(payload, level="WARNING")


def log_debug(payload: Dict[str, Any]) -> None:
    """Ghi một bản ghi log debug dạng JSON."""
    _log_json(payload, level="DEBUG")
