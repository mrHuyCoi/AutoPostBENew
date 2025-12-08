from fastapi import APIRouter, Depends, HTTPException, status, Request, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
import httpx
import json
import asyncio
from typing import AsyncGenerator, Optional

from app.database.database import get_db
from app.middlewares.auth_middleware import get_current_user
from app.middlewares.api_key_middleware import validate_api_key, api_key_header
from app.models.user import User
from app.configs.settings import settings
from app.logging.decorators import with_log_context

router = APIRouter()

# URL của zaloapi service
ZALO_API_BASE_URL = settings.ZALO_API_BASE_URL  # Base URL cho zaloapi service

@router.get("/login-qr")
@with_log_context(
    event_action="zalo.login_qr_stream",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def login_qr_stream(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Tạo QR code để đăng nhập Zalo và stream events.
    
    - **current_user**: Người dùng hiện tại được xác thực từ token
    """
    
    user = current_user

    async def event_stream() -> AsyncGenerator[str, None]:
        try:
            # Gọi đến zaloapi service với user_id
            async with httpx.AsyncClient(timeout=300.0) as client:
                # Truyền user_id từ token vào query parameter (đúng endpoint: /api/auth/qr?key=)
                params = {
                    "key": str(user.id),  # Sử dụng user_id từ API Key làm session key
                }
                
                url = f"{ZALO_API_BASE_URL}/api/auth/qr"
                # Forward user's API key to zaloapi so it can be persisted with the session
                x_api_key = await api_key_header(request)
                headers = {"X-API-Key": x_api_key} if x_api_key else {}
                async with client.stream("GET", url, params=params, headers=headers) as response:
                    if response.status_code != 200:
                        error_data = {
                            "type": "error",
                            "error": f"Lỗi kết nối đến zaloapi service: {response.status_code}"
                        }
                        yield f"data: {json.dumps(error_data)}\n\n"
                        return
                    
                    async for chunk in response.aiter_text():
                        if chunk.strip():
                            # Forward the SSE data from zaloapi
                            yield chunk
                            
        except httpx.TimeoutException:
            error_data = {
                "type": "error", 
                "error": "Timeout khi kết nối đến zaloapi service"
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            
        except httpx.ConnectError:
            error_data = {
                "type": "error",
                "error": "Không thể kết nối đến zaloapi service. Vui lòng kiểm tra service có đang chạy không."
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            
        except Exception as e:
            error_data = {
                "type": "error",
                "error": f"Lỗi không xác định: {str(e)}"
            }
            yield f"data: {json.dumps(error_data)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )

@router.get("/status")
@with_log_context(
    event_action="zalo.status.get",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_zalo_status(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Kiểm tra trạng thái kết nối Zalo của user hiện tại.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Kiểm tra trạng thái session của user
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/auth/session/{session_key}"
            
            response = await client.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể kiểm tra trạng thái Zalo"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi kiểm tra trạng thái: {str(e)}"
        )

@router.get("/sessions")
@with_log_context(
    event_action="zalo.sessions.list",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def list_zalo_sessions(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Liệt kê tất cả các phiên Zalo (tài khoản) đã đăng nhập của user hiện tại.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/auth/sessions"
            params = {"key": str(user.id)}
            response = await client.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể liệt kê các phiên Zalo"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi liệt kê phiên: {str(e)}"
        )

@router.post("/send-image-file")
@with_log_context(
    event_action="zalo.send_image_file",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def send_zalo_image_file(
    request: Request,
    thread_id: str = Form(...),
    message: str | None = Form(None),
    account_id: str | None = Form(None),
    image: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Gửi ảnh từ tệp người dùng tải lên (multipart/form-data) tới một thread Zalo thông qua service zaloapi.

    Form fields: thread_id (required), message (optional), image (file field)
    Forward đến zaloapi: POST /api/groups/send-image-file (multipart)
    """
    try:
        if not thread_id or not str(thread_id).strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing thread_id")
        if not image:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing image file")

        user = current_user
        session_key = str(user.id)
        url = f"{ZALO_API_BASE_URL}/api/groups/send-image-file"

        file_bytes = await image.read()
        files = {
            "image": (image.filename or "image.jpg", file_bytes, image.content_type or "application/octet-stream")
        }
        data = {
            "session_key": session_key,
            "thread_id": str(thread_id),
        }
        if message and str(message).strip():
            data["message"] = str(message)
        if account_id and str(account_id).strip():
            data["account_id"] = str(account_id)

        async with httpx.AsyncClient(timeout=120.0) as client:
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}
            response = await client.post(url, data=data, files=files, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                try:
                    err = response.json()
                except Exception:
                    err = {"detail": f"zaloapi error HTTP {response.status_code}"}
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=err.get("error") or err.get("detail") or "Không thể gửi ảnh (file)")
    except HTTPException:
        raise
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi gửi ảnh (file): {str(e)}",
        )

@router.post("/send-message")
@with_log_context(
    event_action="zalo.send_message",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def send_zalo_message(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Gửi tin nhắn văn bản tới một thread Zalo thông qua service zaloapi.

    Body JSON: { "thread_id": string, "message": string, "quote"?: object, "account_id"?: string }
    - Xác thực: Bearer JWT (user), và X-API-Key (đính kèm nếu có) để service node có thể kiểm tra quyền.
    - Chuyển tiếp tới zaloapi: POST /api/groups/send-message với payload { session_key, thread_id, message, quote? }.
    - quote: Đối tượng tin nhắn cần trích dẫn (đầy đủ các trường của tin nhắn gốc).
    """
    try:
        body = await request.json()
        thread_id = body.get("thread_id")
        message = body.get("message")
        account_id = body.get("account_id")
        quote = body.get("quote")
        if not thread_id or not str(thread_id).strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing thread_id")
        if not message or not str(message).strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing message")

        user = current_user
        session_key = str(user.id)
        url = f"{ZALO_API_BASE_URL}/api/groups/send-message"

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Forward X-API-Key if present
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}
            payload = {"session_key": session_key, "thread_id": str(thread_id), "message": str(message)}
            if account_id and str(account_id).strip():
                payload["account_id"] = str(account_id)
            if quote is not None:
                payload["quote"] = quote
            response = await client.post(url, json=payload, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                # Try parse error body
                try:
                    err = response.json()
                except Exception:
                    err = {"detail": f"zaloapi error HTTP {response.status_code}"}
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=err.get("error") or err.get("detail") or "Không thể gửi tin nhắn")
    except HTTPException:
        raise
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi gửi tin nhắn: {str(e)}",
        )

@router.post("/send-image")
@with_log_context(
    event_action="zalo.send_image",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def send_zalo_image(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Gửi ảnh tới một thread Zalo thông qua service zaloapi.

    Body JSON: { "thread_id": string, "image_url"?: string, "file_path"?: string, "message"?: string }
    - Xác thực: Bearer JWT (user), và X-API-Key (đính kèm nếu có).
    - Chuyển tiếp tới zaloapi: POST /api/groups/send-image với payload { session_key, thread_id, image_url?, file_path?, message? }.
    """
    try:
        body = await request.json()
        thread_id = body.get("thread_id")
        image_url = body.get("image_url")
        file_path = body.get("file_path")
        message = body.get("message")
        account_id = body.get("account_id")

        if not thread_id or not str(thread_id).strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing thread_id")
        if (not image_url or not str(image_url).strip()) and (not file_path or not str(file_path).strip()):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing image_url or file_path")

        user = current_user
        session_key = str(user.id)
        url = f"{ZALO_API_BASE_URL}/api/groups/send-image"

        async with httpx.AsyncClient(timeout=60.0) as client:
            # Forward X-API-Key if present
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}
            payload = {
                "session_key": session_key,
                "thread_id": str(thread_id),
                **({"image_url": str(image_url)} if image_url else {}),
                **({"file_path": str(file_path)} if file_path else {}),
                **({"message": str(message)} if message else {}),
                **({"account_id": str(account_id)} if account_id else {}),
            }
            response = await client.post(url, json=payload, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                try:
                    err = response.json()
                except Exception:
                    err = {"detail": f"zaloapi error HTTP {response.status_code}"}
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=err.get("error") or err.get("detail") or "Không thể gửi ảnh")
    except HTTPException:
        raise
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi gửi ảnh: {str(e)}",
        )
@router.get("/conversations")
@with_log_context(
    event_action="zalo.conversations.list_v1",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_zalo_conversations(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    account_id: str | None = None,
):
    """
    Lấy danh sách cuộc trò chuyện Zalo của user hiện tại.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=30.0) as client:
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/messages/threads/{session_key}"
            params = {}
            if account_id and str(account_id).strip():
                params["account_id"] = str(account_id)
            
            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy danh sách cuộc trò chuyện"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy cuộc trò chuyện: {str(e)}"
        )

@router.get("/messages")
@with_log_context(
    event_action="zalo.messages.list_v1",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_zalo_messages(
    thread_id: str = None,
    peer_id: str = None,
    limit: int = 50,
    order: str = "desc",
    account_id: str | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy tin nhắn từ một cuộc trò chuyện Zalo cụ thể.
    
    - **thread_id**: ID của thread (cho nhóm)
    - **peer_id**: ID của peer (cho chat 1-1)
    - **limit**: Số lượng tin nhắn tối đa (mặc định 50)
    - **order**: Thứ tự sắp xếp (asc/desc, mặc định desc)
    """
    if not thread_id and not peer_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cần cung cấp thread_id hoặc peer_id"
        )
    
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=30.0) as client:
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/messages/conversation/{session_key}"
            
            params = {
                "limit": min(max(limit, 1), 200),
                "order": order if order in ["asc", "desc"] else "desc"
            }
            
            if thread_id:
                params["thread_id"] = thread_id
            if peer_id:
                params["peer_id"] = peer_id
            if account_id and str(account_id).strip():
                params["account_id"] = str(account_id)

            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy tin nhắn"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy tin nhắn: {str(e)}"
        )

@router.delete("/logout")
@with_log_context(
    event_action="zalo.logout",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def logout_zalo(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    account_id: str | None = None,
):
    """
    Đăng xuất khỏi Zalo và xóa session.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Xóa session của user: gọi đúng endpoint zaloapi (POST /api/auth/logout)
            payload = {"key": str(user.id)}
            if account_id and str(account_id).strip():
                payload["account_id"] = str(account_id)
            url = f"{ZALO_API_BASE_URL}/api/auth/logout"
            
            response = await client.post(url, json=payload)
            
            if response.status_code == 200:
                return {"message": "Đã đăng xuất khỏi Zalo thành công"}
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Lỗi khi đăng xuất khỏi Zalo"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi đăng xuất: {str(e)}"
        )

@router.get("/conversations")
@with_log_context(
    event_action="zalo.conversations.list_v2",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_conversations(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    account_id: str | None = None,
):
    """
    Lấy danh sách cuộc trò chuyện Zalo của user.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Sử dụng user_id làm session_key
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/messages/threads/{session_key}"
            
            response = await client.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy danh sách cuộc trò chuyện"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy cuộc trò chuyện: {str(e)}"
        )

@router.get("/messages")
@with_log_context(
    event_action="zalo.messages.list_v2",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_messages(
    thread_id: str = None,
    peer_id: str = None,
    limit: int = 50,
    order: str = "asc",
    account_id: str | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Lấy tin nhắn trong cuộc trò chuyện.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Sử dụng user_id làm session_key
            session_key = str(user.id)
            
            params = {
                "limit": limit,
                "order": order
            }
            
            if thread_id:
                params["thread_id"] = thread_id
            if peer_id:
                params["peer_id"] = peer_id
                
            url = f"{ZALO_API_BASE_URL}/api/messages/conversation/{session_key}"
            if account_id and str(account_id).strip():
                params["account_id"] = str(account_id)
            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy tin nhắn"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy tin nhắn: {str(e)}"
        )

@router.put("/chatbot-priority")
@with_log_context(
    event_action="zalo.chatbot_priority.set",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def set_chatbot_priority(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Đặt ưu tiên chatbot cho session của user.
    
    - **priority**: 'mobile' hoặc 'custom'
    """
    body = await request.json()
    priority = body.get("priority")
    # Cho phép xóa ưu tiên bằng cách gửi 'null' (string) hoặc null
    if isinstance(priority, str) and priority.lower() == "null":
        priority = None
    # Chỉ validate khi priority không phải None
    if priority is not None and priority not in ['mobile', 'custom']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Priority phải là 'mobile' hoặc 'custom'"
        )
    
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/session/{session_key}/chatbot-priority"
            
            account_id = body.get("account_id")
            payload = {"priority": priority}
            if account_id and str(account_id).strip():
                payload["account_id"] = str(account_id)
            response = await client.put(url, json=payload)
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể cập nhật ưu tiên chatbot"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi cập nhật ưu tiên chatbot: {str(e)}"
        )

@router.get("/chatbot-priority")
@with_log_context(
    event_action="zalo.chatbot_priority.get",
    event_category="zalo",
    source_layer="controller",
    source_controller="zalo_controller",
)
async def get_chatbot_priority(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    account_id: str | None = None,
):
    """
    Lấy thông tin ưu tiên chatbot của session hiện tại.
    """
    try:
        user = current_user
        async with httpx.AsyncClient(timeout=10.0) as client:
            session_key = str(user.id)
            url = f"{ZALO_API_BASE_URL}/api/session/{session_key}/chatbot-priority"
            params = {}
            if account_id and str(account_id).strip():
                params["account_id"] = str(account_id)
            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Nếu session không tồn tại, trả về priority mặc định
                return {
                    "ok": True,
                    "chatbot_priority": "mobile"
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Không thể lấy thông tin ưu tiên chatbot"
                )
                
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy ưu tiên chatbot: {str(e)}"
        )

@router.get("/bot-configs/me")
async def get_my_bot_config(
    request: Request,
    current_user: User = Depends(get_current_user), # Dùng get_current_user thay vì validate_api_key
    account_id: Optional[str] = None,
):
    """
    Lấy bot config của user hiện tại (session_key = user.id) từ zaloapi
    """
    try:
        user = current_user
        session_key = str(user.id)
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            params = { "account_id": account_id } if account_id else None
            
            # Forward X-API-Key (nếu frontend gửi)
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}

            resp = await client.get(url, params=params, headers=headers)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy bot config")
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Không thể lấy bot config: {resp.text}"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy bot config: {str(e)}"
        )

@router.put("/bot-configs/me")
async def upsert_my_bot_config(
    request: Request,
    payload: dict, # Nhận payload thô
    current_user: User = Depends(get_current_user), # Dùng get_current_user
):
    """
    Tạo hoặc cập nhật bot config cho user hiện tại (session_key = user.id)
    Body: { stop_minutes: int, account_id?: str }
    """
    try:
        user = current_user
        session_key = str(user.id)

        stop_minutes = payload.get("stop_minutes")
        if stop_minutes is None:
            raise HTTPException(status_code=400, detail="stop_minutes là bắt buộc")

        if not isinstance(stop_minutes, (int, float)) or stop_minutes < 0:
            raise HTTPException(status_code=400, detail="stop_minutes phải là số không âm")

        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            
            # Tạo payload cho zaloapi
            req_body = {
                "stop_minutes": int(stop_minutes)
            }
            account_id = payload.get("account_id")
            if account_id:
                req_body["account_id"] = str(account_id)
            
            # Forward X-API-Key (nếu frontend gửi)
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}

            resp = await client.put(url, json=req_body, headers=headers)
            if resp.status_code in (200, 201):
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Không thể tạo/cập nhật bot config: {resp.text}"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi upsert bot config: {str(e)}"
        )

@router.get("/bot-configs/me")
async def get_my_bot_config(
    request: Request,
    current_user: User = Depends(get_current_user),
    account_id: Optional[str] = None, # Lỗi NameError xảy ra vì 'Optional' bị thiếu
):
    """
    Lấy bot config của user hiện tại (session_key = user.id) từ zaloapi
    """
    try:
        user = current_user
        session_key = str(user.id)
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            params = { "account_id": account_id } if account_id else None
            
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}

            resp = await client.get(url, params=params, headers=headers)

            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Không tìm thấy bot config")
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Không thể lấy bot config: {resp.text}"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi lấy bot config: {str(e)}"
        )

@router.put("/bot-configs/me")
async def upsert_my_bot_config(
    request: Request,
    payload: dict, 
    current_user: User = Depends(get_current_user),
):
    """
    Tạo hoặc cập nhật bot config cho user hiện tại (session_key = user.id)
    Body: { stop_minutes: int, account_id?: str }
    """
    try:
        user = current_user
        session_key = str(user.id)

        stop_minutes = payload.get("stop_minutes")
        if stop_minutes is None:
            raise HTTPException(status_code=400, detail="stop_minutes là bắt buộc")

        if not isinstance(stop_minutes, (int, float)) or stop_minutes < 0:
            raise HTTPException(status_code=400, detail="stop_minutes phải là số không âm")

        async with httpx.AsyncClient(timeout=15.0) as client:
            url = f"{ZALO_API_BASE_URL}/api/bot-configs/{session_key}"
            
            req_body = {
                "stop_minutes": int(stop_minutes)
            }
            account_id = payload.get("account_id")
            if account_id:
                req_body["account_id"] = str(account_id)
            
            x_api_key = await api_key_header(request)
            headers = {"X-API-Key": x_api_key} if x_api_key else {}

            resp = await client.put(url, json=req_body, headers=headers)
            if resp.status_code in (200, 201):
                return resp.json()
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Không thể tạo/cập nhật bot config: {resp.text}"
                )
    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối đến zaloapi service"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi upsert bot config: {str(e)}"
        )
