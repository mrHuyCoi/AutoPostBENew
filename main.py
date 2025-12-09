from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import traceback
import os
from asgi_correlation_id import CorrelationIdMiddleware

from app.configs.settings import settings
from app.database.database import create_tables
# Ensure all models are imported so SQLAlchemy metadata is populated
import app.models  # noqa: F401
from app.middlewares.error_handler import ErrorHandlerMiddleware
from app.middlewares.subscription_middleware import check_active_subscription

# Import tất cả các controller
from app.api.v1 import auth_router, registration_router
from app.controllers import (
    user_controller, 
    subscription_controller, 
    youtube_controller, 
    websocket_controller, 
    task_controller,
    user_config_controller,
    document_controller,
    chatbot_controller,
    chatbot_subscription_controller,
    auth_controller,
    file_upload_controller,
    # Thêm các controller còn lại
    brand_controller,
    service_controller,
    facebook_controller,
    instagram_controller,
    scheduled_video_controller,
    admin_controller,
    device_info_controller,
    color_controller,
    user_device_controller,
    device_color_controller,
    device_storage_controller,
    device_brand_controller,
    product_component_controller,
    category_controller,
    property_controller,
    warranty_service_controller,
    chatbot_linhkien_controller,
    faq_mobile_controller,
    zalo_controller,
    staffzalo_controller,
    zalo_ignored_controller,
    zalo_bot_config_controller,
    zalo_oa_controller,
    zalo_oa_webhook_controller,
    messenger_webhook_controller,
    order_controller,
    order_chatcustom_controller,
    user_sync_url_controller,
    chatbot_js_settings_controller,
    user_device_from_url_router,
    material_controller
)
from app.controllers.device_storage_controller import router as storage_router
from fastapi import APIRouter


app = FastAPI(
    title=settings.APP_NAME,
    description="API cho ứng dụng đăng bài tự động lên mạng xã hội",
    version="1.0.0",
)

# Middleware log lỗi chi tiết cho tất cả các request
@app.middleware("http")
async def log_exceptions(request: Request, call_next):
    response = await call_next(request)
    return response

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    CorrelationIdMiddleware,
    header_name="X-Request-ID",
)

# Middleware xử lý lỗi (nếu bạn có custom handler logic riêng)
app.add_middleware(ErrorHandlerMiddleware)

# --- Đăng ký router ---
# Các router không yêu cầu gói subscription
app.include_router(auth_router.router, prefix="/api/v1/auth", tags=["Auth"])
app.include_router(registration_router.router, prefix="/api/v1/registration", tags=["Registration"])
app.include_router(subscription_controller.router, prefix="/api/v1/subscriptions", tags=["Subscriptions"])
app.include_router(chatbot_controller.router, prefix="/api/v1/chatbot", tags=["Chatbot"])
app.include_router(chatbot_subscription_controller.router, prefix="/api/v1", tags=["Chatbot Subscriptions"])
app.include_router(user_config_controller.router, prefix="/api/v1", tags=["User Config"])
app.include_router(document_controller.router, prefix="/api/v1", tags=["Documents"])
app.include_router(file_upload_controller.router, prefix="/api/v1/files", tags=["File Upload"])
# WebSocket endpoints (real-time chat bridge)
app.include_router(websocket_controller.router, prefix="/api/v1", tags=["WebSocket"])
# Các router quản lý chung, có thể không cần check sub, chỉ cần login
app.include_router(admin_controller.router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(task_controller.task_router, prefix="/api/v1/tasks", tags=["Tasks"])
app.include_router(auth_controller.router, prefix="/api/v1/auth", tags=["Auth"])

# Các router YÊU CẦU có gói subscription video đang hoạt động
protected_video_router = APIRouter(dependencies=[Depends(check_active_subscription)])
protected_video_router.include_router(user_controller.router, prefix="/users", tags=["Users"])
protected_video_router.include_router(youtube_controller.router, prefix="/youtube", tags=["YouTube"])
protected_video_router.include_router(facebook_controller.router, prefix="/facebook", tags=["Facebook"])
protected_video_router.include_router(instagram_controller.router, prefix="/instagram", tags=["Instagram"])
protected_video_router.include_router(scheduled_video_controller.router, prefix="/scheduled-videos", tags=["Scheduled Videos"])
# ... thêm các router khác cần check sub video vào đây

app.include_router(protected_video_router, prefix="/api/v1")


# Các router quản lý sản phẩm, thiết bị (chỉ cần login)
app.include_router(device_info_controller.router, prefix="/api/v1", tags=["Device Info"])
app.include_router(color_controller.router, prefix="/api/v1", tags=["Colors"])
app.include_router(user_device_controller.router, prefix="/api/v1", tags=["User Devices"])
app.include_router(device_color_controller.router, prefix="/api/v1", tags=["Device Colors"])
app.include_router(device_storage_controller.router, prefix="/api/v1", tags=["Device Storages"])
app.include_router(brand_controller.router, prefix="/api/v1", tags=["Brands"])
app.include_router(service_controller.router, prefix="/api/v1", tags=["Services"])
app.include_router(storage_router, prefix="/api/v1", tags=["Storages"])
app.include_router(device_brand_controller.router, prefix="/api/v1", tags=["Device Brands"])
app.include_router(product_component_controller.router, prefix="/api/v1/product-components", tags=["Product Components"])
app.include_router(category_controller.router, prefix="/api/v1/categories", tags=["Categories"])
app.include_router(property_controller.router, prefix="/api/v1/properties", tags=["Properties"])
app.include_router(warranty_service_controller.router, prefix="/api/v1/warranty-services", tags=["Warranty Services"])
app.include_router(chatbot_linhkien_controller.router, prefix="/api/v1/chatbot-linhkien", tags=["Chatbot Linh Kiện"])
app.include_router(faq_mobile_controller.router, prefix="/api/v1", tags=["FAQ Mobile"])
app.include_router(zalo_controller.router, prefix="/api/v1/zalo", tags=["Zalo"])
app.include_router(zalo_ignored_controller.router, prefix="/api/v1/zalo", tags=["Zalo Ignored"])
app.include_router(zalo_bot_config_controller.router, prefix="/api/v1/zalo", tags=["Zalo Bot Config"])
app.include_router(zalo_oa_controller.router, prefix="/api/v1/zalo-oa", tags=["Zalo OA"])
app.include_router(zalo_oa_webhook_controller.router, prefix="/api/v1/zalo-oa", tags=["Zalo OA Webhook"])
app.include_router(messenger_webhook_controller.router, prefix="/api/v1/messenger", tags=["Messenger Webhook"])
app.include_router(staffzalo_controller.router, prefix="/api/v1", tags=["Staff Zalo"])
app.include_router(order_controller.router, prefix="/api/v1", tags=["Orders"])
app.include_router(order_chatcustom_controller.router, prefix="/api/v1", tags=["Orders Custom"])
app.include_router(user_sync_url_controller, prefix="/api/v1", tags=["User Sync URL"])
app.include_router(user_device_from_url_router, prefix="/api/v1", tags=["User Devices From URL"])
app.include_router(chatbot_js_settings_controller.router, prefix="/api/v1", tags=["Chatbot JS Settings"])
app.include_router(material_controller.router, prefix="/api/v1", tags=["Materials"])


@app.get("/")
def root():
    return {
        "app_name": settings.APP_NAME,
        "version": "1.0.0",
        "description": "API cho ứng dụng đăng bài tự động lên mạng xã hội",
        "documentation": "/docs",
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8011, reload=False)
