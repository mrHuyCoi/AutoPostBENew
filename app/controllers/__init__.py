# Import tất cả các controller
from app.controllers.user_controller import router as user_router
from app.controllers.subscription_controller import router as subscription_router
from app.controllers.user_device_controller import router as user_device_router
from app.controllers.youtube_controller import router as youtube_router
from app.controllers.facebook_controller import router as facebook_router
from app.controllers.instagram_controller import router as instagram_router
from app.controllers.device_info_controller import router as device_info_router
from app.controllers.color_controller import router as color_router
from app.controllers.device_color_controller import router as device_color_router
from app.controllers.device_storage_controller import router as device_storage_router
from app.controllers.service_controller import router as service_router
from app.controllers.brand_controller import router as brand_router
from app.controllers.device_brand_controller import router as device_brand_router
from app.controllers.category_controller import router as category_router
from app.controllers.property_controller import router as property_router
from app.controllers.product_component_controller import router as product_component_router
from app.controllers.warranty_service_controller import router as warranty_service_router
from app.controllers.user_config_controller import router as user_config_router
from app.controllers.document_controller import router as document_router
from app.controllers.chatbot_controller import router as chatbot_router
from app.controllers.scheduled_video_controller import router as scheduled_video_router
from app.controllers.admin_controller import router as admin_router
from app.controllers.auth_controller import router as auth_router
from app.controllers.websocket_controller import router as websocket_router
from app.controllers.zalo_controller import router as zalo_router
from app.controllers.zalo_ignored_controller import router as zalo_ignored_router
from app.controllers.zalo_bot_config_controller import router as zalo_bot_config_router
from app.controllers.zalo_oa_controller import router as zalo_oa_router
from app.controllers.zalo_oa_webhook_controller import router as zalo_oa_webhook_router
from app.controllers.order_controller import router as order_router
from app.controllers.order_chatcustom_controller import router as order_chatcustom_router
from app.controllers.user_sync_url_controller import router as user_sync_url_controller
from app.controllers.messenger_webhook_controller import router as messenger_router
from app.controllers.chatbot_js_settings_controller import router as chatbot_js_settings_router
from app.controllers.user_device_from_url_controller import router as user_device_from_url_router

__all__ = [
    "user_router",
    "subscription_router",
    "user_device_router",
    "youtube_router",
    "facebook_router",
    "instagram_router",
    "device_info_router",
    "color_router",
    "user_device_router",
    "device_color_router",
    "device_storage_router",
    "service_router",
    "brand_router",
    "device_brand_router",
    "category_router",
    "property_router",
    "product_component_router",
    "warranty_service_router",
    "user_config_router",
    "document_router",
    "chatbot_router",
    "scheduled_video_router",
    "admin_router",
    "auth_router",
    "websocket_router",
    "zalo_router",
    "zalo_ignored_router",
    "zalo_bot_config_router",
    "zalo_oa_router",
    "zalo_oa_webhook_router",
    "order_router",
    "order_chatcustom_router",
    "user_sync_url_controller",
    "user_device_from_url_router",
    "messenger_router",
    "chatbot_js_settings_router",
]
