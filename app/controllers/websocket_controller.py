from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request, Depends, HTTPException, status
from typing import Dict, Set, Optional
import json
import uuid
from jose import jwt, JWTError

from app.configs.settings import settings
from app.database.database import get_db
from app.repositories.user_repository import UserRepository
from app.repositories.user_api_key_repository import UserApiKeyRepository
from app.middlewares.api_key_middleware import get_user_for_chatbot
from app.logging.decorators import with_log_context

router = APIRouter()


class ConnectionManager:
    """Quản lý kết nối theo user_id."""
    def __init__(self) -> None:
        self._by_user: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str) -> None:
        await websocket.accept()
        bucket = self._by_user.get(user_id)
        if bucket is None:
            bucket = set()
            self._by_user[user_id] = bucket
        bucket.add(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str) -> None:
        bucket = self._by_user.get(user_id)
        if not bucket:
            return
        try:
            bucket.discard(websocket)
        finally:
            if not bucket:
                try:
                    del self._by_user[user_id]
                except KeyError:
                    pass

    async def broadcast_to_user(self, user_id: str, message: str) -> None:
        conns = list(self._by_user.get(user_id, set()))
        for ws in conns:
            try:
                await ws.send_text(message)
            except Exception:
                # Nếu client đã đóng, loại bỏ
                try:
                    self.disconnect(ws, user_id)
                except Exception:
                    pass


manager = ConnectionManager()


async def _resolve_user_id_from_ws(websocket: WebSocket) -> str:
    """Xác thực kết nối WebSocket bằng JWT (query/header) hoặc X-API-Key. Trả về user_id (str)."""
    # 1) Lấy JWT từ query param hoặc header Authorization
    token: Optional[str] = None
    try:
        token = websocket.query_params.get("token")
    except Exception:
        token = None

    if not token:
        auth_header = websocket.headers.get("authorization") or websocket.headers.get("Authorization")
        if isinstance(auth_header, str) and auth_header.lower().startswith("bearer "):
            token = auth_header.split(" ", 1)[1].strip()

    # 2) Nếu có JWT: giải mã và xác thực user
    if token:
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
            user_id_str = payload.get("sub")
            if not user_id_str:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token: missing subject")
            # Kiểm tra user trong DB
            async for db in get_db():
                user = await UserRepository.get_by_id(db, uuid.UUID(user_id_str))
                if user and getattr(user, "is_active", True):
                    return str(user.id)
                break
        except (JWTError, ValueError):
            # Nếu JWT lỗi, tiếp tục thử API Key
            pass

    # 3) Thử xác thực bằng X-API-Key
    api_key = websocket.headers.get("x-api-key") or websocket.headers.get("X-API-Key")
    if api_key:
        async for db in get_db():
            api_key_obj = await UserApiKeyRepository.get_by_api_key(db, api_key)
            if api_key_obj:
                user = await UserRepository.get_by_id(db, api_key_obj.user_id)
                if user and getattr(user, "is_active", True):
                    return str(user.id)
            break

    # 4) Nếu không xác thực được
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized WebSocket connection")


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Xác thực trước khi chấp nhận kết nối
    try:
        user_id = await _resolve_user_id_from_ws(websocket)
    except HTTPException:
        # Policy Violation (1008) khi không xác thực được
        try:
            await websocket.close(code=1008)
        finally:
            return

    await manager.connect(websocket, user_id)
    try:
        while True:
            text = await websocket.receive_text()
            # Hỗ trợ ping/pong đơn giản để giữ kết nối
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue
            # Echo nhẹ để kiểm tra
            await websocket.send_text(json.dumps({"type": "ack", "echo": text}))
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)


# API nhận dữ liệu từ worker/service và broadcast đến client
@router.post("/ws/push")
@with_log_context(
    event_action="websocket.push",
    event_category="websocket",
    source_layer="controller",
    source_controller="websocket_controller",
)
async def push_to_websocket(request: Request, auth=Depends(get_user_for_chatbot)):
    # auth = (user, scopes)
    try:
        user, _scopes = auth
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    data = await request.json()
    # Đảm bảo gửi chuỗi JSON xuống WS
    message = json.dumps(data, ensure_ascii=False)
    await manager.broadcast_to_user(str(getattr(user, "id")), message)
    return {"ok": True}
