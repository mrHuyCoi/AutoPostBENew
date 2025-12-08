#!/usr/bin/env python
import asyncio
import sys
import os

# Thêm thư mục gốc của dự án vào sys.path để import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.ext.asyncio import create_async_engine
from app.database.database import Base
from app.configs.settings import settings

# Import tất cả các model để đảm bảo metadata được đăng ký
from app.models import (
    User, 
    MediaAsset, 
    SocialAccount, 
    platform_post_media_asset,
    PlatformPost, 
    YouTubeMetadata,
    UserSubscription,
    Subscription,
    Color,
    DeviceInfo,
    UserDevice,
    DeviceColor,
    DeviceStorage,
    device_brand,
    Brand,
    Service,
    Category,
    Property,
    ProductComponent,
    WarrantyService
)

# ensure JSONB type works with SQLite fallback
try:
    from sqlalchemy.dialects.postgresql import JSONB  # noqa: F401
except ImportError:
    pass

async def create_tables():
    print(f"Kết nối đến database: {settings.DATABASE_URL}")
    
    # Tạo engine
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    
    # Tạo tất cả các bảng
    async with engine.begin() as conn:
        # Xóa tất cả các bảng hiện có (thận trọng với tùy chọn này trong production)
        await conn.run_sync(Base.metadata.drop_all)
        
        # Tạo lại tất cả các bảng
        print("Đang tạo tất cả các bảng...")
        await conn.run_sync(Base.metadata.create_all)
    
    print("Đã tạo tất cả các bảng thành công!")
    
    # Đóng engine
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(create_tables())
