# create_tables.py
import asyncio
from app.database.database import Base
from app.database.session import engine  # nếu bạn lưu engine trong session.py
# hoặc nếu engine ở file khác, sửa lại import cho đúng
# from app.database.database import engine

# ⚠️ import tất cả models để SQLAlchemy biết có bảng nào
from app.models.user import User
from app.models.user_subscription import UserSubscription
from app.models.media_asset import MediaAsset
from app.models.social_account import SocialAccount
from app.models.user_device import UserDevice
from app.models.device_brand import DeviceBrand
from app.models.product_component import ProductComponent
from app.models.warranty_service import WarrantyService
from app.models.user_sync_url import UserSyncUrl
from app.models.user_device_from_url import UserDeviceFromUrl
from app.models.user_chatbot_subscription import UserChatbotSubscription
from app.models.user_api_key import UserApiKey
from app.models.user_bot_control import UserBotControl

async def create_all():
    async with engine.begin() as conn:
        print("🧱 Creating all tables in PostgreSQL...")
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Done! All tables created successfully.")

if __name__ == "__main__":
    asyncio.run(create_all())
