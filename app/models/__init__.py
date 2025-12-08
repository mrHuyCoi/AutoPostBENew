# Import tất cả các model
from .user_bot_control import UserBotControl
from app.models.user import User
from app.models.subscription import Subscription
from app.models.user_subscription import UserSubscription
from app.models.media_asset import MediaAsset
from app.models.social_account import SocialAccount
from app.models.platform_post import PlatformPost
from app.models.youtube_metadata import YouTubeMetadata
from app.models.platform_post_media_asset import platform_post_media_asset
from app.models.device_info import DeviceInfo
from app.models.color import Color
from app.models.user_device import UserDevice
from app.models.user_device_from_url import UserDeviceFromUrl
from app.models.device_color import DeviceColor
from app.models.device_storage import DeviceStorage
from app.models.service import Service
from app.models.brand import Brand
from app.models.device_brand import DeviceBrand
from app.models.category import Category
from app.models.property import Property
from app.models.product_component import ProductComponent
from app.models.warranty_service import WarrantyService
from app.models.chatbot_service import ChatbotService
from app.models.chatbot_plan import ChatbotPlan, chatbot_plan_service_association
from app.models.user_chatbot_subscription import UserChatbotSubscription
from app.models.user_api_key import UserApiKey
from app.models.material import Material
from app.models.user_sync_url import UserSyncUrl
from app.models.messenger_message import MessengerMessage
from app.models.messenger_bot_config import MessengerBotConfig
from app.models.oa_account import OaAccount
from app.models.oa_token import OaToken
from app.models.oauth_state import OauthState
from app.models.oa_conversation import OaConversation
from app.models.oa_message import OaMessage
from app.models.oa_webhook_event import OaWebhookEvent
from app.models.verification_code import VerificationCode
# ProductComponentProperty model removed due to schema change

__all__ = [
    "User",
    "Subscription",
    "UserSubscription",
    "MediaAsset",
    "SocialAccount",
    "PlatformPost",
    "YouTubeMetadata",
    "platform_post_media_asset",
    "DeviceInfo",
    "Color",
    "UserDevice",
    "UserDeviceFromUrl",
    "DeviceColor",
    "DeviceStorage",
    "Service",
    "Brand",
    "DeviceBrand",
    "Category",
    "Property",
    "ProductComponent",
    # "ProductComponentProperty",
    "WarrantyService",
    "ChatbotService",
    "ChatbotPlan",
    "chatbot_plan_service_association",
    "UserChatbotSubscription",
    "UserApiKey",
    "Material",
    "UserSyncUrl",
    "MessengerMessage",
    "MessengerBotConfig",
    "OaAccount",
    "OaToken",
    "OauthState",
    "OaConversation",
    "OaMessage",
    "OaWebhookEvent",
    "VerificationCode",
]