from fastapi import APIRouter, Body, Depends, HTTPException, status, Form, File, UploadFile, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List
import json
import httpx
import logging

from app.database.database import get_db
from app.models.user import User
from app.middlewares.api_key_middleware import get_user_for_chatbot, api_key_header
from app.repositories.user_chatbot_subscription_repository import UserChatbotSubscriptionRepository
from app.repositories.user_bot_control_repository import UserBotControlRepository

async def get_authorized_user_for_components(
    auth: tuple[User, list[str]] = Depends(get_user_for_chatbot),
    request: Request = None,
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Xác thực cho Chatbot Linh Kiện:
    - Cho phép MỘT trong HAI: Bearer token HOẶC X-API-Key.
    - Nếu request có X-API-Key: kiểm tra subscription + scopes trước khi cho phép.
    """
    user, _ = auth

    # Chỉ kiểm tra subscription khi client dùng API Key
    api_key = None
    try:
        api_key = await api_key_header(request)
    except Exception:
        api_key = None

    if api_key:
        # Kiểm tra subscription đang hoạt động
        active_subscription = await UserChatbotSubscriptionRepository.get_active_subscription_by_user(db, user.id)
        if not active_subscription or not active_subscription.plan or not active_subscription.plan.services:
            logger.warning(f"User {user.email} has no active subscription for component chatbot via API key.")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="API Key hợp lệ, nhưng bạn cần đăng ký gói dịch vụ để sử dụng chức năng này."
            )

        # Kiểm tra scopes - sử dụng tên service để xác định quyền truy cập
        service_names = [service.name.lower() for service in active_subscription.plan.services]
        allowed_scopes = ["bán linh kiện", "linh kiện", "phụ kiện", "accessory", "component", "*"]
        has_access = any(
            allowed_scope in service_name
            for service_name in service_names
            for allowed_scope in allowed_scopes
        )
        if not has_access:
            logger.warning(
                f"User {user.email} tried to access component chatbot without required scope via API key. Service names: {service_names}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Gói dịch vụ của bạn không bao gồm chức năng chatbot linh kiện."
            )

    return user

from app.configs.settings import settings
from app.services.chatbot_service import ChatbotService
from app.services.chatbot_subscription_service import ChatbotSubscriptionService
from app.utils.crypto import token_encryption
from app.tasks.chatbot_tasks import chatbot_custom_chat
from app.logging.decorators import with_log_context

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL của ChatbotCustom service
CHATBOT_CUSTOM_BASE_URL = settings.CHATBOT_CUSTOM_API_BASE_URL  # Cập nhật theo config thực tế

# Log thông tin khởi tạo
logger.info(f"ChatbotLinhKien controller initialized with base URL: {CHATBOT_CUSTOM_BASE_URL}")

router = APIRouter()

@router.post("/chat")
@with_log_context(
    event_action="chatbot_linhkien.chat",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def chat_with_linhkien_bot(
    message: str = Form(...),
    model_choice: str = Form(default="gemini"),
    session_id: str = Form(default="default"),
    image_url: Optional[str] = Form(None),
    image_urls: Optional[str] = Form(None),
    image: Optional[UploadFile] = File(None),
    platform: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_authorized_user_for_components),
    request: Request = None,
):
    """
    Proxy endpoint để chat với chatbot linh kiện.
    Sử dụng customer_id từ user hiện tại và forward request đến ChatbotCustom.
    """
    
    # Gating by platform when using X-API-Key
    try:
        x_key = await api_key_header(request) if request else None
    except Exception:
        x_key = None
    if x_key and platform:
        enabled = await UserBotControlRepository.is_enabled(db, current_user.id, platform)
        if not enabled:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Nền tảng này đang tắt.")
    subscription = None
    if x_key:
        subscription = await ChatbotSubscriptionService.get_my_active_subscription(db, current_user)
        await ChatbotSubscriptionService.check_and_increment_usage(db, current_user)

    # Validate và normalize model_choice
    valid_models = ["gemini", "lmstudio", "openai"]
    original_model = model_choice
    
    if model_choice not in valid_models:
        logger.warning(f"Invalid model_choice: {model_choice}. Valid options: {valid_models}. Using default: gemini")
        model_choice = "gemini"  # Sử dụng gemini làm mặc định
    
    # Luôn sử dụng Gemini API key - decrypt trước khi sử dụng
    use_env_gemini = False
    if x_key and subscription and hasattr(subscription, "max_api_calls") and subscription.max_api_calls is not None and subscription.max_api_calls > 0:
        use_env_gemini = True
    if use_env_gemini:
        api_key = settings.GEMINI_API_KEY
        if not api_key:
            logger.error("GEMINI_API_KEY không được cấu hình trong ENV")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Hệ thống chưa được cấu hình GEMINI_API_KEY."
            )
    else:
        encrypted_api_key = current_user.gemini_api_key
        if not encrypted_api_key:
            logger.warning(f"User {current_user.email} không có Gemini API key")
            return {
                "message": "Vui lòng nhập Gemini API key ở trang cấu hình để sử dụng chatbot",
                "type": "api_key_required",
                "status": "warning"
            }
        
        try:
            api_key = token_encryption.decrypt(encrypted_api_key)
            if not api_key:
                logger.error(f"Không thể decrypt API key cho user {current_user.email}")
                return {
                    "message": "API key không hợp lệ, vui lòng kiểm tra lại cấu hình",
                    "type": "api_key_invalid",
                    "status": "error"
                }
        except Exception as e:
            logger.error(f"Lỗi khi decrypt API key cho user {current_user.email}: {str(e)}")
            return {
                "message": "Lỗi xử lý API key, vui lòng thử lại sau",
                "type": "api_key_error",
                "status": "error"
            }
    
    try:
        # Sử dụng user_id làm customer_id để kết nối với database chung
        customer_id = str(current_user.id)
        # session_id đã được truyền vào từ Form parameter, nếu không có sẽ dùng "default"
        

        
        # Xử lý image nếu có
        if image:
            logger.info(f"Image uploaded: {image.filename}, size: {image.size} bytes")
        
        # Log thông tin request
        logger.info(f"Chat request - User: {current_user.email} (ID: {customer_id}), Original Model: {original_model}, Normalized Model: {model_choice}, Session: {session_id}")
        if image:
            logger.info(f"Image file: {image.filename}, size: {image.size} bytes, type: {image.content_type}")
        # Optional async enqueue mode via query param ?async=1
        try:
            async_flag = request.query_params.get("async") if request else None
        except Exception:
            async_flag = None
        is_async = str(async_flag).lower() in ("1", "true", "yes") if async_flag is not None else False
        # Parse image_urls JSON string (if provided) into list[str]
        image_urls_list: Optional[List[str]] = None
        try:
            if image_urls:
                parsed = json.loads(image_urls)
                if isinstance(parsed, list):
                    image_urls_list = [str(u) for u in parsed if isinstance(u, (str, bytes))]
        except Exception:
            image_urls_list = None

        if is_async:
            res = chatbot_custom_chat.delay(
                customer_id=customer_id,
                message=message,
                model_choice=model_choice,
                api_key=api_key,
                session_id=session_id,
                image_url=image_url,
                image_urls=image_urls_list,
            )
            return {"task_id": res.id, "status": "queued"}
        
        # Gọi API ChatbotCustom
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Endpoint của ChatbotCustom
            url = f"{CHATBOT_CUSTOM_BASE_URL}/chat/{customer_id}"
            
            # Chuẩn bị form data theo API mới của ChatbotCustom
            form_data = {
                "message": message,
                "model_choice": model_choice,
                "api_key": api_key,
                "session_id": session_id
            }
            if image_url:
                form_data["image_url"] = image_url
            if image_urls_list:
                try:
                    form_data["image_urls"] = json.dumps(image_urls_list)
                except Exception:
                    pass
            
            logger.info(f"Calling ChatbotCustom API - Method: POST, URL: {url}")
            logger.info(f"Form data: {form_data}")
            
            # Gửi request với file ảnh nếu có
            if image:
                files = {"image": (image.filename, image.file, image.content_type)}
                response = await client.post(
                    url,
                    data=form_data,
                    files=files,
                    timeout=30.0
                )
            else:
                response = await client.post(
                    url,
                    data=form_data,
                    timeout=30.0
                )
            
            logger.info(f"ChatbotCustom API response - Status: {response.status_code}, Headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                error_detail = f"Lỗi từ chatbot service: {response.text}"
                logger.error(f"ChatbotCustom API error - Status: {response.status_code}, Detail: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=error_detail
                )
            
            # Log response thành công
            response_data = response.json()
            logger.info(f"ChatbotCustom API success - Response: {response_data}")
            
            # Trả về response từ ChatbotCustom
            return response_data
            
    except httpx.RequestError as e:
        error_msg = f"Không thể kết nối đến chatbot service: {str(e)}"
        logger.error(f"Request error to ChatbotCustom: {error_msg}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg
        )
    except Exception as e:
        error_msg = f"Lỗi server: {str(e)}"
        logger.error(f"Unexpected error in chat endpoint: {error_msg}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )

@router.post("/control/{session_id}")
@with_log_context(
    event_action="chatbot_linhkien.control",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def control_linhkien_bot(
    session_id: str,
    command: str = Form(...),  # "start" hoặc "stop"
    current_user: User = Depends(get_authorized_user_for_components)
):
    """
    Điều khiển trạng thái bot (start/stop) cho session cụ thể.
    """
    
    # Validate command
    valid_commands = ["start", "stop"]
    if command not in valid_commands:
        logger.error(f"Invalid command: {command}. Valid options: {valid_commands}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"command phải là một trong: {', '.join(valid_commands)}"
        )
    
    try:
        customer_id = str(current_user.id)
        
        # Sử dụng đúng format JSON theo ControlBotRequest schema
        json_data = {"command": command}
        
        # Log thông tin request
        logger.info(f"Control request - User: {current_user.email} (ID: {customer_id}), Command: {command}, Session: {session_id}")
        logger.info(f"Request data: {json_data}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Sửa URL endpoint theo đúng API của ChatbotCustom
            url = f"{CHATBOT_CUSTOM_BASE_URL}/control-bot/{customer_id}"
            params = {"session_id": session_id}
            
            logger.info(f"Calling ChatbotCustom control API - Method: POST, URL: {url}, Params: {params}")
            logger.info(f"Request payload: {json_data}")
            
            response = await client.post(url, json=json_data, params=params)
            
            logger.info(f"ChatbotCustom control API response - Status: {response.status_code}, Headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                error_detail = f"Lỗi từ chatbot service: {response.text}"
                logger.error(f"ChatbotCustom control API error - Status: {response.status_code}, Detail: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=error_detail
                )
            
            # Log response thành công
            response_data = response.json()
            logger.info(f"ChatbotCustom control API success - Response: {response_data}")
            
            return response_data
            
    except httpx.RequestError as e:
        error_msg = f"Không thể kết nối đến chatbot service: {str(e)}"
        logger.error(f"Request error to ChatbotCustom control API: {error_msg}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg
        )
    except Exception as e:
        error_msg = f"Lỗi server: {str(e)}"
        logger.error(f"Unexpected error in control endpoint: {error_msg}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )

@router.post("/human-handover/{session_id}")
@with_log_context(
    event_action="chatbot_linhkien.human_handover",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def human_handover_linhkien(
    session_id: str,
    current_user: User = Depends(get_authorized_user_for_components)
):
    """
    Chuyển chat sang chế độ nhân viên hỗ trợ.
    """
    
    try:
        customer_id = str(current_user.id)
        
        # Log thông tin request
        logger.info(f"Human handover request - User: {current_user.email} (ID: {customer_id}), Session: {session_id}")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Sửa URL endpoint theo đúng API của ChatbotCustom
            url = f"{CHATBOT_CUSTOM_BASE_URL}/human-chatting/{customer_id}"
            params = {"session_id": session_id}
            
            logger.info(f"Calling ChatbotCustom human handover API - Method: POST, URL: {url}, Params: {params}")
            
            response = await client.post(url, params=params)
            
            logger.info(f"ChatbotCustom human handover API response - Status: {response.status_code}, Headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                error_detail = f"Lỗi từ chatbot service: {response.text}"
                logger.error(f"ChatbotCustom human handover API error - Status: {response.status_code}, Detail: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=error_detail
                )
            
            # Log response thành công
            response_data = response.json()
            logger.info(f"ChatbotCustom human handover API success - Response: {response_data}")
            
            return response_data
            
    except httpx.RequestError as e:
        error_msg = f"Không thể kết nối đến chatbot service: {str(e)}"
        logger.error(f"Request error to ChatbotCustom human handover API: {error_msg}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg
        )
    except Exception as e:
        error_msg = f"Lỗi server: {str(e)}"
        logger.error(f"Unexpected error in human handover endpoint: {error_msg}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )

@router.get("/health")
@with_log_context(
    event_action="chatbot_linhkien.health",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def check_linhkien_bot_health():
    """
    Kiểm tra tình trạng kết nối với ChatbotCustom service.
    """
    
    logger.info("Health check request received")
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # Kiểm tra kết nối bằng cách gọi một endpoint đơn giản
            # Sử dụng endpoint chat với một message test nhỏ
            test_url = f"{CHATBOT_CUSTOM_BASE_URL}/chat/test-customer"
            test_data = {
                "message": "test",
                "model_choice": "gemini",
                "image_url": None
            }
            
            logger.info(f"Calling ChatbotCustom test API - Method: POST, URL: {test_url}")
            logger.info(f"Test payload: {test_data}")
            
            response = await client.post(test_url, json=test_data, params={"session_id": "test"})
            
            logger.info(f"ChatbotCustom test API response - Status: {response.status_code}, Headers: {dict(response.headers)}")
            
            if response.status_code in [200, 422]:  # 422 là OK vì customer_id không tồn tại
                health_status = {
                    "status": "healthy",
                    "chatbot_service": "connected",
                    "message": "Chatbot Linh Kiện service đang hoạt động bình thường"
                }
                logger.info(f"Health check success: {health_status}")
                return health_status
            else:
                health_status = {
                    "status": "unhealthy",
                    "chatbot_service": "error",
                    "message": f"Chatbot service trả về status {response.status_code}"
                }
                logger.warning(f"Health check warning: {health_status}")
                return health_status
                
    except httpx.RequestError as e:
        health_status = {
            "status": "unhealthy",
            "chatbot_service": "disconnected",
            "message": "Không thể kết nối đến Chatbot Linh Kiện service"
        }
        logger.error(f"Health check failed - Connection error: {str(e)}")
        return health_status

@router.post("/sync-data")
@with_log_context(
    event_action="chatbot_linhkien.sync_data",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def sync_user_data_to_linhkien_bot(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_authorized_user_for_components)
):
    """
    Đồng bộ toàn bộ dữ liệu product components của user với ChatbotCustom.
    """
    
    logger.info(f"Sync data request - User: {current_user.email} (ID: {current_user.id})")
    
    try:
        logger.info(f"Starting sync for user: {current_user.full_name or current_user.email}")
        
        success = await ChatbotService.sync_all_user_components_to_custom(current_user)
        
        if success:
            result = {
                "status": "success",
                "message": "Đồng bộ dữ liệu thành công",
                "synced_for_user": current_user.full_name or current_user.email
            }
            logger.info(f"Sync data success: {result}")
            return result
        else:
            result = {
                "status": "error",
                "message": "Đồng bộ dữ liệu thất bại"
            }
            logger.error(f"Sync data failed: {result}")
            return result
            
    except Exception as e:
        error_msg = f"Lỗi khi đồng bộ dữ liệu: {str(e)}"
        logger.error(f"Sync data error: {error_msg}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg
        )

@router.get("/system-prompt")
@with_log_context(
    event_action="chatbot_linhkien.system_prompt.get",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def get_system_prompt(
    current_user: User = Depends(get_authorized_user_for_components)
):
    """
    Lấy system prompt tùy chỉnh của một người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{CHATBOT_CUSTOM_BASE_URL}/prompts/{user_id}"
            response = await client.get(url)
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class SystemPromptUpdate(BaseModel):
    prompt_content: str

@router.put("/system-prompt")
@with_log_context(
    event_action="chatbot_linhkien.system_prompt.update",
    event_category="chatbot_linhkien",
    source_layer="controller",
    source_controller="chatbot_linhkien_controller",
)
async def update_system_prompt(
    prompt_update: SystemPromptUpdate = Body(...),
    current_user: User = Depends(get_authorized_user_for_components)
):
    """
    Cập nhật system prompt tùy chỉnh của một người dùng.
    Gọi API từ ChatbotCustom.
    """
    try:
        user_id = str(current_user.id)
        if not user_id:
            raise HTTPException(status_code=400, detail="Không thể xác định người dùng")
        async with httpx.AsyncClient(timeout=10.0) as client:
            url = f"{CHATBOT_CUSTOM_BASE_URL}/prompts/{user_id}"
            response = await client.put(url, json={"prompt_content": prompt_update.prompt_content})
            return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))